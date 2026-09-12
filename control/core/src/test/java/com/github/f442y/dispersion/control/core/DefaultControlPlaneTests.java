package com.github.f442y.dispersion.control.core;

import com.github.f442y.dispersion.control.ExecutionStatus;
import com.github.f442y.dispersion.control.ExecutionSummary;
import com.github.f442y.dispersion.control.InspectableMachine;
import com.github.f442y.dispersion.control.MachineDescriptor;
import com.github.f442y.dispersion.control.MachineType;
import com.github.f442y.dispersion.control.SignalDeliveryResult;
import com.github.f442y.dispersion.event.EventStream;
import com.github.f442y.dispersion.event.ExecutionEvent;
import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.fsm.core.atomic.AtomicStateMachineBuilder;
import com.github.f442y.dispersion.fsm.core.atomic.AtomicStateMachineExecutor;
import com.github.f442y.dispersion.fsm.state.StateKey;
import com.github.f442y.dispersion.orchestration.OrchestrationCheckpoint;
import com.github.f442y.dispersion.orchestration.OrchestrationTurnResult;
import com.github.f442y.dispersion.orchestration.command.SignalCommand;
import com.github.f442y.dispersion.orchestration.core.InMemoryCheckpointStore;
import com.github.f442y.dispersion.orchestration.core.OrchestrationStateMachineBuilder;
import com.github.f442y.dispersion.orchestration.core.OrchestrationStateMachineExecutor;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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

    @Test
    @DisplayName("Should register custom InspectableMachine via SPI and route signals and checkpoints")
    void testInspectableMachineSpiRegistration() throws Exception {
        MachineDescriptor desc = new MachineDescriptor(
                "CustomSpiMachine",
                MachineType.ORCHESTRATION,
                "START",
                Set.of("SUCCESS", "FAILED"),
                List.of("START", "STEP_A", "SUCCESS", "FAILED"),
                "stateDiagram-v2\n[*] --> START"
        );

        InspectableMachine customMachine = new InspectableMachine() {
            @Override
            @NonNull
            public MachineDescriptor descriptor() {
                return desc;
            }

            @Override
            @NonNull
            public CompletableFuture<SignalDeliveryResult> sendSignal(
                    @NonNull String correlationKey,
                    @NonNull String signalName,
                    @Nullable Object payload
            ) {
                return CompletableFuture.completedFuture(
                        new SignalDeliveryResult(true, "Custom handled", "CustomSpiMachine", correlationKey, signalName, true, false, "SUCCESS", null)
                );
            }

            @Override
            @NonNull
            public Optional<Object> inspectCheckpoint(@NonNull String correlationKey) {
                return Optional.of("MOCK_CHECKPOINT_FOR_" + correlationKey);
            }
        };

        controlPlane.register(customMachine);

        // Verify descriptor lookup
        Optional<MachineDescriptor> found = controlPlane.getMachine("CustomSpiMachine");
        assertTrue(found.isPresent());
        assertEquals(desc, found.get());

        // Verify signal routing
        SignalDeliveryResult sigResult = controlPlane.sendSignal("CustomSpiMachine", "CORR-77", "TestSig", "payload").get();
        assertTrue(sigResult.delivered());
        assertEquals("SUCCESS", sigResult.resultingState());

        // Verify checkpoint inspection
        Optional<Object> cp = controlPlane.inspectCheckpoint("CustomSpiMachine", "CORR-77");
        assertTrue(cp.isPresent());
        assertEquals("MOCK_CHECKPOINT_FOR_CORR-77", cp.get());
    }

    @Test
    @DisplayName("Segmented O(1) eviction should strictly protect active/suspended workflows while capping terminal executions")
    void testSegmentedO1EvictionProtectsActiveWorkflows() {
        // Capacity: 2 terminal executions
        try (DefaultControlPlane boundedCp = new DefaultControlPlane(2, 50, 100)) {
            UUID activeId = UUID.randomUUID();
            UUID term1 = UUID.randomUUID();
            UUID term2 = UUID.randomUUID();
            UUID term3 = UUID.randomUUID();

            Instant now = Instant.now();

            // 1. Emit active suspended workflow
            boundedCp.onEvent(new ExecutionEvent.TurnStartedEvent(activeId, "WorkflowA", "CORR-ACTIVE", now));
            boundedCp.onEvent(new ExecutionEvent.TurnSuspendedEvent(activeId, "WorkflowA", "WAIT_INPUT", "UserApprovalSig", "CORR-ACTIVE", Duration.ofMillis(10), now));

            // Verify active workflow is tracked as SUSPENDED
            Optional<ExecutionSummary> activeSummary = boundedCp.getExecution(activeId.toString());
            assertTrue(activeSummary.isPresent());
            assertEquals(ExecutionStatus.SUSPENDED, activeSummary.get().status());

            // 2. Emit terminal execution 1
            boundedCp.onEvent(new ExecutionEvent.TurnStartedEvent(term1, "WorkflowA", "CORR-1", now));
            boundedCp.onEvent(new ExecutionEvent.TurnCompletedEvent(term1, "WorkflowA", "DONE", "CORR-1", Duration.ofMillis(10), now));

            // 3. Emit terminal execution 2 (reaches capacity 2)
            boundedCp.onEvent(new ExecutionEvent.TurnStartedEvent(term2, "WorkflowA", "CORR-2", now));
            boundedCp.onEvent(new ExecutionEvent.TurnCompletedEvent(term2, "WorkflowA", "DONE", "CORR-2", Duration.ofMillis(10), now));

            // Both term1 and term2 present
            assertTrue(boundedCp.getExecution(term1.toString()).isPresent());
            assertTrue(boundedCp.getExecution(term2.toString()).isPresent());

            // 4. Emit terminal execution 3 (exceeds capacity -> term1 evicted via O(1) FIFO)
            boundedCp.onEvent(new ExecutionEvent.TurnStartedEvent(term3, "WorkflowA", "CORR-3", now));
            boundedCp.onEvent(new ExecutionEvent.TurnCompletedEvent(term3, "WorkflowA", "DONE", "CORR-3", Duration.ofMillis(10), now));

            // term1 must be evicted
            assertFalse(boundedCp.getExecution(term1.toString()).isPresent(), "Oldest terminal execution must be evicted");
            assertTrue(boundedCp.getExecution(term2.toString()).isPresent());
            assertTrue(boundedCp.getExecution(term3.toString()).isPresent());

            // CRITICAL: activeId must NEVER be evicted!
            Optional<ExecutionSummary> activeStillPresent = boundedCp.getExecution(activeId.toString());
            assertTrue(activeStillPresent.isPresent(), "Active/Suspended executions must never be evicted by terminal limits");
            assertEquals(ExecutionStatus.SUSPENDED, activeStillPresent.get().status());
        }
    }

    @Test
    @DisplayName("Should live-stream execution and machine events via Virtual Thread event streams")
    void testLiveStreamingWatchExecutionAndWatchMachine() throws Exception {
        UUID targetExecId = UUID.randomUUID();
        UUID otherExecId = UUID.randomUUID();
        Instant now = Instant.now();

        try (EventStream execStream = controlPlane.watchExecution(targetExecId.toString());
             EventStream machineStream = controlPlane.watchMachine("TargetMachine")) {

            // Emit event for target machine and target execution
            controlPlane.onEvent(new ExecutionEvent.TurnStartedEvent(targetExecId, "TargetMachine", "CORR-TARGET", now));

            // Emit event for target machine but DIFFERENT execution
            controlPlane.onEvent(new ExecutionEvent.TurnStartedEvent(otherExecId, "TargetMachine", "CORR-OTHER", now));

            // Emit event for DIFFERENT machine
            controlPlane.onEvent(new ExecutionEvent.TurnStartedEvent(UUID.randomUUID(), "OtherMachine", "CORR-X", now));

            // execStream should only receive targetExecId event
            ExecutionEvent execEvent = execStream.poll(Duration.ofSeconds(2));
            assertNotNull(execEvent);
            assertEquals(targetExecId, execEvent.machineId());
            // Should have no more events
            ExecutionEvent nextExecEvent = execStream.poll(Duration.ofMillis(100));
            assertNull(nextExecEvent);

            // machineStream should receive both TargetMachine events
            ExecutionEvent mEvent1 = machineStream.poll(Duration.ofSeconds(2));
            assertNotNull(mEvent1);
            assertEquals("TargetMachine", mEvent1.machineName());

            ExecutionEvent mEvent2 = machineStream.poll(Duration.ofSeconds(2));
            assertNotNull(mEvent2);
            assertEquals("TargetMachine", mEvent2.machineName());

            // No third event (OtherMachine was filtered out)
            ExecutionEvent mEvent3 = machineStream.poll(Duration.ofMillis(100));
            assertNull(mEvent3);
        }
    }
}
