package com.github.f442y.dispersion.control.core;

import com.github.f442y.dispersion.control.*;
import com.github.f442y.dispersion.event.*;
import com.github.f442y.dispersion.event.dispatcher.*;
import com.github.f442y.dispersion.fsm.context.*;
import com.github.f442y.dispersion.fsm.core.atomic.*;
import com.github.f442y.dispersion.fsm.state.*;
import com.github.f442y.dispersion.orchestration.*;
import com.github.f442y.dispersion.orchestration.command.*;
import com.github.f442y.dispersion.orchestration.core.*;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("Control Plane Registry & Inspection API Tests")
class DefaultControlPlaneTests {

    private DefaultControlPlane controlPlane;

    public enum AtomicFlowState implements StateKey {
        IDLE,
        PROCESSING,
        FINISHED
    }

    public static final class FlowContext implements StateMachineContext {
        public String orderId = "ORD-999";
        public int count = 0;
        public String status = "NEW";
    }

    public enum OrchestrationFlowState implements StateKey {
        INIT,
        WAIT_APPROVAL,
        DISPATCH,
        COMPLETED
    }

    public record ApprovalSignal(
            String correlationKey,
            String approver,
            boolean approved
    ) implements SignalCommand {
        @Override
        @NonNull
        public String signalName() {
            return "ApprovalSignal";
        }
    }

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

    @Test
    @DisplayName("Should register machine topologies and generate valid Mermaid diagrams")
    void testMachineRegistrationAndTopologyInspection() {
        AtomicStateMachineExecutor<FlowContext, AtomicFlowState, Integer, String> atomicExecutor =
                AtomicStateMachineBuilder.<FlowContext, AtomicFlowState, Integer, String>create("PaymentValidationMachine", AtomicFlowState.class)
                        .context(FlowContext::new)
                        .initialState(AtomicFlowState.IDLE)
                        .endStates(AtomicFlowState.FINISHED)
                        .state(AtomicFlowState.IDLE)
                            .action(ctx -> ctx)
                            .transition(AtomicFlowState.PROCESSING)
                        .state(AtomicFlowState.PROCESSING)
                            .action(ctx -> ctx)
                            .transition(AtomicFlowState.FINISHED)
                        .output(ctx -> "OK")
                        .buildExecutor();

        controlPlane.register(atomicExecutor);

        List<MachineDescriptor> machines = controlPlane.listMachines();
        assertEquals(1, machines.size());

        MachineDescriptor desc = machines.getFirst();
        assertEquals("PaymentValidationMachine", desc.name());
        assertEquals(MachineType.ATOMIC, desc.type());
        assertEquals("IDLE", desc.initialState());
        assertEquals(Set.of("FINISHED"), desc.endStates());
        assertTrue(desc.allStates().containsAll(List.of("IDLE", "PROCESSING", "FINISHED")));
        assertTrue(desc.mermaidGraph().startsWith("stateDiagram-v2"));
        assertTrue(desc.mermaidGraph().contains("[*] --> IDLE"));
        assertTrue(desc.mermaidGraph().contains("IDLE --> PROCESSING"));

        // Direct lookup
        Optional<MachineDescriptor> lookup = controlPlane.getMachine("PaymentValidationMachine");
        assertTrue(lookup.isPresent());
        assertEquals(desc, lookup.get());

        // Unregister
        assertTrue(controlPlane.unregister("PaymentValidationMachine"));
        assertTrue(controlPlane.listMachines().isEmpty());
    }

    @Test
    @DisplayName("Should track atomic machine execution lifecycle in real-time")
    void testAtomicExecutionTracking() throws Exception {
        AtomicStateMachineExecutor<FlowContext, AtomicFlowState, Integer, String> atomicExecutor =
                AtomicStateMachineBuilder.<FlowContext, AtomicFlowState, Integer, String>create("OrderFulfillmentFSM", AtomicFlowState.class)
                        .context(FlowContext::new)
                        .initialState(AtomicFlowState.IDLE)
                        .endStates(AtomicFlowState.FINISHED)
                        .eventListener(controlPlane.getEventListener())
                        .input((ctx, in) -> {
                            ctx.count = in != null ? in : 0;
                            return ctx;
                        })
                        .state(AtomicFlowState.IDLE)
                            .action(ctx -> {
                                ctx.count += 5;
                                return ctx;
                            })
                            .transition(AtomicFlowState.PROCESSING)
                        .state(AtomicFlowState.PROCESSING)
                            .action(ctx -> {
                                ctx.count *= 3;
                                return ctx;
                            })
                            .transition(AtomicFlowState.FINISHED)
                        .output(ctx -> "TOTAL=" + ctx.count)
                        .buildExecutor();

        controlPlane.register(atomicExecutor);

        String result = atomicExecutor.dispatchSync(10);
        assertEquals("TOTAL=45", result);

        List<ExecutionSummary> executions = controlPlane.listExecutions("OrderFulfillmentFSM", null, 10);
        assertEquals(1, executions.size());

        ExecutionSummary summary = executions.getFirst();
        assertEquals("OrderFulfillmentFSM", summary.machineName());
        assertEquals("FINISHED", summary.currentState());
        assertEquals(ExecutionStatus.COMPLETED, summary.status());
        assertNotNull(summary.startTime());
        assertNotNull(summary.endTime());
        assertTrue(summary.transitionsCount() >= 2);

        // Verify execution timeline
        var timeline = controlPlane.getExecutionTimeline(summary.executionId());
        assertFalse(timeline.isEmpty());

        // Verify recent events
        var recent = controlPlane.getRecentEvents("OrderFulfillmentFSM", 20);
        assertFalse(recent.isEmpty());
    }

    @Test
    @DisplayName("Should track suspended orchestration, inspect checkpoints, and deliver external signals")
    void testOrchestrationSuspensionAndSignalDelivery() throws Exception {
        InMemoryCheckpointStore<FlowContext, OrchestrationFlowState> checkpointStore = new InMemoryCheckpointStore<>();

        OrchestrationStateMachineExecutor<FlowContext, OrchestrationFlowState, FlowContext, String> orchExecutor =
                OrchestrationStateMachineBuilder.<FlowContext, OrchestrationFlowState, FlowContext, String>create("OrderApprovalWorkflow", OrchestrationFlowState.class)
                        .context(FlowContext::new)
                        .initialState(OrchestrationFlowState.INIT)
                        .endStates(OrchestrationFlowState.COMPLETED)
                        .eventListener(controlPlane.getEventListener())
                        .checkpointStore(checkpointStore)
                        .correlationKey(ctx -> ctx.orderId)
                        .input((ctx, in) -> {
                            if (in != null) {
                                ctx.orderId = in.orderId;
                                ctx.status = in.status;
                            }
                            return ctx;
                        })
                        .state(OrchestrationFlowState.INIT)
                            .action(ctx -> {
                                ctx.status = "SUBMITTED";
                                return ctx;
                            })
                            .transition(OrchestrationFlowState.WAIT_APPROVAL)
                        .state(OrchestrationFlowState.WAIT_APPROVAL)
                            .waitForCommand(ApprovalSignal.class, (ctx, sig) -> {
                                ctx.status = sig.approved() ? "APPROVED_BY_" + sig.approver() : "REJECTED";
                                return ctx;
                            })
                            .transition(OrchestrationFlowState.DISPATCH)
                        .state(OrchestrationFlowState.DISPATCH)
                            .action(ctx -> {
                                ctx.status += "->DISPATCHED";
                                return ctx;
                            })
                            .transition(OrchestrationFlowState.COMPLETED)
                        .output(ctx -> ctx.status)
                        .buildExecutor();

        controlPlane.register(orchExecutor);

        // 1. Initial execution -> Suspends at WAIT_APPROVAL
        FlowContext input = new FlowContext();
        input.orderId = "ORD-4200";

        OrchestrationTurnResult<FlowContext, OrchestrationFlowState, String> turn1 = orchExecutor.dispatchTurnSync(null, input);
        assertTrue(turn1.isSuspended());
        assertEquals(OrchestrationFlowState.WAIT_APPROVAL, turn1.currentStateKey());

        // Verify Control Plane sees suspended execution
        List<ExecutionSummary> suspendedList = controlPlane.listExecutions("OrderApprovalWorkflow", ExecutionStatus.SUSPENDED, 10);
        assertEquals(1, suspendedList.size());

        ExecutionSummary suspendedSummary = suspendedList.getFirst();
        assertEquals("WAIT_APPROVAL", suspendedSummary.currentState());
        assertEquals(ExecutionStatus.SUSPENDED, suspendedSummary.status());
        assertEquals("ORD-4200", suspendedSummary.correlationKey());
        assertEquals("ApprovalSignal", suspendedSummary.suspendedSignal());

        // 2. Checkpoint inspection via Control Plane
        Optional<OrchestrationCheckpoint<?, ?>> checkpointOpt = controlPlane.getCheckpoint("OrderApprovalWorkflow", "ORD-4200");
        assertTrue(checkpointOpt.isPresent());
        assertEquals("ORD-4200", checkpointOpt.get().correlationKey());
        assertEquals(OrchestrationFlowState.WAIT_APPROVAL, checkpointOpt.get().currentStateKey());

        // 3. Deliver signal via Control Plane
        ApprovalSignal signal = new ApprovalSignal("ORD-4200", "AliceMgr", true);
        CompletableFuture<SignalDeliveryResult> signalFuture = controlPlane.sendSignal(
                "OrderApprovalWorkflow",
                "ORD-4200",
                "ApprovalSignal",
                signal
        );

        SignalDeliveryResult deliveryResult = signalFuture.get();
        assertTrue(deliveryResult.delivered(), "Signal must be delivered");
        assertTrue(deliveryResult.completed(), "Turn must complete workflow");
        assertFalse(deliveryResult.suspended());
        assertEquals("COMPLETED", deliveryResult.resultingState());

        // 4. Verify updated status in Control Plane
        Optional<ExecutionSummary> completedSummaryOpt = controlPlane.getExecution(suspendedSummary.executionId());
        assertTrue(completedSummaryOpt.isPresent());
        assertEquals(ExecutionStatus.COMPLETED, completedSummaryOpt.get().status());
        assertEquals("COMPLETED", completedSummaryOpt.get().currentState());
    }

    @Test
    @DisplayName("Should handle signal delivery to non-existent machine gracefully")
    void testSignalDeliveryToNonExistentMachine() throws Exception {
        CompletableFuture<SignalDeliveryResult> future = controlPlane.sendSignal(
                "NonExistentMachine",
                "CORR-123",
                "SomeSignal",
                null
        );
        SignalDeliveryResult result = future.get();
        assertFalse(result.delivered());
        assertTrue(result.message().contains("not registered"));
    }

    @Test
    @DisplayName("Should reject signal delivery to atomic state machines")
    void testSignalDeliveryToAtomicMachine() throws Exception {
        AtomicStateMachineExecutor<FlowContext, AtomicFlowState, Integer, String> atomicExecutor =
                AtomicStateMachineBuilder.<FlowContext, AtomicFlowState, Integer, String>create("ReadOnlyAtomic", AtomicFlowState.class)
                        .context(FlowContext::new)
                        .initialState(AtomicFlowState.IDLE)
                        .endStates(AtomicFlowState.FINISHED)
                        .state(AtomicFlowState.IDLE)
                            .action(ctx -> ctx)
                            .transition(AtomicFlowState.FINISHED)
                        .output(ctx -> "DONE")
                        .buildExecutor();

        controlPlane.register(atomicExecutor);

        CompletableFuture<SignalDeliveryResult> future = controlPlane.sendSignal(
                "ReadOnlyAtomic",
                "CORR-999",
                "SignalX",
                null
        );
        SignalDeliveryResult result = future.get();
        assertFalse(result.delivered());
        assertTrue(result.message().contains("Atomic state machines do not support"));
    }
}
