package com.github.f442y.dispersion.examples;

import com.github.f442y.dispersion.control.ExecutionStatus;
import com.github.f442y.dispersion.control.ExecutionSummary;
import com.github.f442y.dispersion.control.MachineDescriptor;
import com.github.f442y.dispersion.control.MachineType;
import com.github.f442y.dispersion.control.SignalDeliveryResult;
import com.github.f442y.dispersion.control.core.DefaultControlPlane;
import com.github.f442y.dispersion.control.test.FakeInspectableMachine;
import com.github.f442y.dispersion.event.test.RecordingEventBus;
import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.fsm.core.atomic.AtomicStateMachineBuilder;
import com.github.f442y.dispersion.fsm.core.atomic.AtomicStateMachineExecutor;
import com.github.f442y.dispersion.fsm.state.StateKey;
import com.github.f442y.dispersion.orchestration.OrchestrationCheckpoint;
import com.github.f442y.dispersion.orchestration.OrchestrationTurnResult;
import com.github.f442y.dispersion.orchestration.batch.BarrierPolicy;
import com.github.f442y.dispersion.orchestration.batch.BatchOrchestrationCheckpoint;
import com.github.f442y.dispersion.orchestration.batch.BatchTurnResult;
import com.github.f442y.dispersion.orchestration.command.SignalCommand;
import com.github.f442y.dispersion.orchestration.core.InMemoryCheckpointStore;
import com.github.f442y.dispersion.orchestration.core.OrchestrationStateMachineBuilder;
import com.github.f442y.dispersion.orchestration.core.OrchestrationStateMachineExecutor;
import com.github.f442y.dispersion.orchestration.core.batch.BatchOrchestrationExecutor;
import com.github.f442y.dispersion.orchestration.core.batch.BatchOrchestrationStateMachineBuilder;
import com.github.f442y.dispersion.testing.DispersionTestKit;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration tests demonstrating how real execution engines (Atomic, Orchestration, Batch)
 * adapt to {@link com.github.f442y.dispersion.control.InspectableMachine} via {@code asInspectableMachine()}
 * and register into {@link DefaultControlPlane} with full telemetry and signal routing.
 */
@DisplayName("Control Plane End-to-End Integration Tests")
public class ControlPlaneIntegrationExampleTests {

    private DefaultControlPlane controlPlane;

    @BeforeEach
    void setUp() {
        controlPlane = new DefaultControlPlane();
    }

    @AfterEach
    void tearDown() {
        if (controlPlane != null) {
            controlPlane.close();
        }
    }

    // =========================================================================
    // 1. Atomic State Machine Integration
    // =========================================================================

    public enum AtomicStep implements StateKey {
        INIT, PROCESS, FINISHED
    }

    public static class SimpleContext implements StateMachineContext {
        public int value = 0;
    }

    @Test
    @DisplayName("Should register real AtomicStateMachineExecutor via asInspectableMachine() and track telemetry")
    void testAtomicStateMachineIntegration() throws Exception {
        AtomicStateMachineExecutor<SimpleContext, AtomicStep, Integer, Integer> executor =
                AtomicStateMachineBuilder.<SimpleContext, AtomicStep, Integer, Integer>create("SimpleAtomicEngine", AtomicStep.class)
                        .context(SimpleContext::new)
                        .initialState(AtomicStep.INIT)
                        .endStates(AtomicStep.FINISHED)
                        .eventListener(controlPlane.getEventListener())
                        .input((ctx, in) -> {
                            ctx.value = in != null ? in : 0;
                            return ctx;
                        })
                        .state(AtomicStep.INIT)
                            .action(ctx -> {
                                ctx.value += 10;
                                return ctx;
                            })
                            .transition(AtomicStep.PROCESS)
                        .state(AtomicStep.PROCESS)
                            .action(ctx -> {
                                ctx.value *= 2;
                                return ctx;
                            })
                            .transition(AtomicStep.FINISHED)
                        .output(ctx -> ctx.value)
                        .buildExecutor();

        // Register using decoupled adapter
        controlPlane.register(executor.asInspectableMachine());

        // Verify descriptor registered in Control Plane
        Optional<MachineDescriptor> descOpt = controlPlane.getMachine("SimpleAtomicEngine");
        assertThat(descOpt).isPresent();
        MachineDescriptor desc = descOpt.get();
        assertThat(desc.name()).isEqualTo("SimpleAtomicEngine");
        assertThat(desc.type()).isEqualTo(MachineType.ATOMIC);
        assertThat(desc.initialState()).isEqualTo("INIT");
        assertThat(desc.endStates()).containsExactly("FINISHED");

        // Execute workflow
        int result = executor.dispatchSync(5);
        assertThat(result).isEqualTo(30);

        // Verify telemetry recorded in Control Plane
        List<ExecutionSummary> executions = controlPlane.listExecutions("SimpleAtomicEngine", ExecutionStatus.COMPLETED, 10);
        assertThat(executions).hasSize(1);
        ExecutionSummary summary = executions.getFirst();
        assertThat(summary.currentState()).isEqualTo("FINISHED");
        assertThat(summary.status()).isEqualTo(ExecutionStatus.COMPLETED);
        assertThat(summary.transitionsCount()).isGreaterThanOrEqualTo(2);
    }

    // =========================================================================
    // 2. Orchestration State Machine Integration (Signal Delivery & Checkpoints)
    // =========================================================================

    public enum ApprovalState implements StateKey {
        START, PENDING_REVIEW, APPROVED
    }

    public record ReviewSignal(@NonNull String correlationKey, @NonNull String reviewer) implements SignalCommand {
        @Override
        @NonNull
        public String signalName() {
            return "ReviewSignal";
        }
    }

    public static class ApprovalContext implements StateMachineContext {
        public String docId = "DOC-100";
        public String reviewer;
    }

    @Test
    @DisplayName("Should register real OrchestrationStateMachineExecutor, inspect checkpoint, and deliver signal")
    void testOrchestrationStateMachineIntegration() throws Exception {
        InMemoryCheckpointStore<ApprovalContext, ApprovalState> store = new InMemoryCheckpointStore<>();

        OrchestrationStateMachineExecutor<ApprovalContext, ApprovalState, ApprovalContext, String> executor =
                OrchestrationStateMachineBuilder.<ApprovalContext, ApprovalState, ApprovalContext, String>create("DocumentApproval", ApprovalState.class)
                        .context(ApprovalContext::new)
                        .initialState(ApprovalState.START)
                        .endStates(ApprovalState.APPROVED)
                        .checkpointStore(store)
                        .correlationKey(ctx -> ctx.docId)
                        .eventListener(controlPlane.getEventListener())
                        .input((ctx, in) -> {
                            if (in != null) ctx.docId = in.docId;
                            return ctx;
                        })
                        .state(ApprovalState.START)
                            .action(ctx -> ctx)
                            .transition(ApprovalState.PENDING_REVIEW)
                        .state(ApprovalState.PENDING_REVIEW)
                            .waitForCommand(ReviewSignal.class, (ctx, sig) -> {
                                ctx.reviewer = sig.reviewer();
                                return ctx;
                            })
                            .transition(ApprovalState.APPROVED)
                        .output(ctx -> "APPROVED_BY_" + ctx.reviewer)
                        .buildExecutor();

        // Register through adapter
        controlPlane.register(executor.asInspectableMachine());

        // 1. Initial turn suspends
        ApprovalContext input = new ApprovalContext();
        input.docId = "DOC-100";
        OrchestrationTurnResult<ApprovalContext, ApprovalState, String> turn = executor.dispatchTurnSync(null, input);
        assertThat(turn.isSuspended()).isTrue();
        assertThat(turn.currentStateKey()).isEqualTo(ApprovalState.PENDING_REVIEW);

        // 2. Control plane reflects suspended state
        List<ExecutionSummary> suspendedList = controlPlane.listExecutions("DocumentApproval", ExecutionStatus.SUSPENDED, 10);
        assertThat(suspendedList).hasSize(1);
        ExecutionSummary summary = suspendedList.getFirst();
        assertThat(summary.correlationKey()).isEqualTo("DOC-100");
        assertThat(summary.status()).isEqualTo(ExecutionStatus.SUSPENDED);

        // 3. Inspect checkpoint through generic Control Plane method
        Optional<OrchestrationCheckpoint> checkpoint =
                controlPlane.inspectCheckpoint("DocumentApproval", "DOC-100", OrchestrationCheckpoint.class);
        assertThat(checkpoint).isPresent();
        assertThat(checkpoint.get().correlationKey()).isEqualTo("DOC-100");
        assertThat(checkpoint.get().currentStateKey()).isEqualTo(ApprovalState.PENDING_REVIEW);

        // 4. Send signal through Control Plane
        CompletableFuture<SignalDeliveryResult> deliveryFuture = controlPlane.sendSignal(
                "DocumentApproval",
                "DOC-100",
                "ReviewSignal",
                new ReviewSignal("DOC-100", "Bob")
        );

        SignalDeliveryResult deliveryResult = deliveryFuture.get();
        assertThat(deliveryResult.delivered()).isTrue();
        assertThat(deliveryResult.completed()).isTrue();
        assertThat(deliveryResult.resultingState()).isEqualTo("APPROVED");

        // 5. Control plane reflects completion
        Optional<ExecutionSummary> completed = controlPlane.getExecution(summary.executionId());
        assertThat(completed).isPresent();
        assertThat(completed.get().status()).isEqualTo(ExecutionStatus.COMPLETED);

        executor.close();
    }

    // =========================================================================
    // 3. Batch Orchestration State Machine Integration
    // =========================================================================

    public enum BatchStep implements StateKey {
        IMPORT, RUN_CHECKS, AWAIT_TRIGGER, COMPLETE
    }

    public static class BatchCtx implements StateMachineContext {
        public String batchId = "BATCH-ALPHA";
    }

    public static class ItemCtx implements StateMachineContext {
        public String itemId;
        public String status = "INIT";
        public ItemCtx() {}
        public ItemCtx(String itemId) { this.itemId = itemId; }
    }

    @Test
    @DisplayName("Should register real BatchOrchestrationExecutor, inspect batch checkpoint, and signal barrier")
    void testBatchOrchestrationIntegration() throws Exception {
        BatchOrchestrationExecutor<BatchCtx, ItemCtx, BatchStep, String> batchExecutor =
                BatchOrchestrationStateMachineBuilder.<BatchCtx, ItemCtx, BatchStep, String>create("AlphaBatchPipeline", BatchStep.class)
                        .batchContext(BatchCtx::new)
                        .batchKey(ctx -> ctx.batchId)
                        .itemKey(ctx -> ctx.itemId)
                        .initialState(BatchStep.IMPORT)
                        .endStates(BatchStep.COMPLETE)
                        .output(ctx -> "BATCH_DONE:" + ctx.batchId)
                        .itemState(BatchStep.IMPORT)
                            .action(ctx -> { ctx.status = "IMPORTED"; return ctx; })
                            .transition(BatchStep.RUN_CHECKS)
                        .itemState(BatchStep.RUN_CHECKS)
                            .barrier(BarrierPolicy.ALL_ITEMS_ARRIVED)
                            .transition(BatchStep.AWAIT_TRIGGER)
                        .itemState(BatchStep.AWAIT_TRIGGER)
                            .barrier(BarrierPolicy.SIGNAL_TRIGGERED)
                            .transition(BatchStep.COMPLETE)
                        .buildExecutor();

        // Register through decoupled adapter
        controlPlane.register(batchExecutor.asInspectableMachine());

        // Verify descriptor
        Optional<MachineDescriptor> descOpt = controlPlane.getMachine("AlphaBatchPipeline");
        assertThat(descOpt).isPresent();
        assertThat(descOpt.get().type()).isEqualTo(MachineType.BATCH);

        // 1. Dispatch batch -> Reaches AWAIT_TRIGGER and suspends
        List<ItemCtx> items = List.of(new ItemCtx("ITEM-A"), new ItemCtx("ITEM-B"));
        BatchTurnResult<BatchCtx, ItemCtx, BatchStep, String> result = batchExecutor.dispatchBatchSync(items);
        assertThat(result.isSuspended()).isTrue();
        assertThat(result.currentBatchStateKey()).isEqualTo(BatchStep.AWAIT_TRIGGER);

        // 2. Inspect checkpoint through Control Plane
        Optional<BatchOrchestrationCheckpoint> cpOpt =
                controlPlane.inspectCheckpoint("AlphaBatchPipeline", "BATCH-ALPHA", BatchOrchestrationCheckpoint.class);
        assertThat(cpOpt).isPresent();
        assertThat(cpOpt.get().batchName()).isEqualTo("AlphaBatchPipeline");
        assertThat(cpOpt.get().currentBatchStateKey()).isEqualTo(BatchStep.AWAIT_TRIGGER);

        // 3. Deliver barrier trigger signal through Control Plane
        CompletableFuture<SignalDeliveryResult> signalFuture =
                controlPlane.sendSignal("AlphaBatchPipeline", "BATCH-ALPHA", "PROCEED", "payload");
        SignalDeliveryResult delivery = signalFuture.get();
        assertThat(delivery.delivered()).isTrue();
        assertThat(delivery.completed()).isTrue();
        assertThat(delivery.resultingState()).isEqualTo("COMPLETE");

        batchExecutor.close();
    }

    // =========================================================================
    // 4. Test Kit Verification
    // =========================================================================

    @Test
    @DisplayName("Should demonstrate DispersionTestKit fluent facade for quick testing")
    void testTestKitUsage() {
        RecordingEventBus recordingBus = DispersionTestKit.recordingEventBus();
        FakeInspectableMachine fakeMachine = DispersionTestKit.fakeInspectableMachine("KitMachine", MachineType.ORCHESTRATION);

        controlPlane.register(fakeMachine);
        controlPlane.attachTo(recordingBus);

        assertThat(controlPlane.getMachine("KitMachine")).isPresent();
        assertThat(recordingBus.events()).isEmpty();
    }
}
