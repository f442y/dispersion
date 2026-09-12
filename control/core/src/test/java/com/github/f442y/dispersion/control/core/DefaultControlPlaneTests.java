package com.github.f442y.dispersion.control.core;

import com.github.f442y.dispersion.control.ExecutionStatus;
import com.github.f442y.dispersion.control.ExecutionSummary;
import com.github.f442y.dispersion.control.InspectableMachine;
import com.github.f442y.dispersion.control.MachineDescriptor;
import com.github.f442y.dispersion.control.MachineType;
import com.github.f442y.dispersion.control.SignalDeliveryResult;
import com.github.f442y.dispersion.control.test.FakeInspectableMachine;
import com.github.f442y.dispersion.event.EventStream;
import com.github.f442y.dispersion.event.ExecutionEvent;
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
        MachineDescriptor desc = new MachineDescriptor(
                "PaymentValidationMachine",
                MachineType.ATOMIC,
                "IDLE",
                Set.of("FINISHED"),
                List.of("IDLE", "PROCESSING", "FINISHED"),
                "stateDiagram-v2\n    [*] --> IDLE\n    IDLE --> PROCESSING\n    PROCESSING --> FINISHED\n    FINISHED --> [*]\n"
        );
        FakeInspectableMachine atomicMachine = new FakeInspectableMachine(desc);

        controlPlane.register(atomicMachine);

        List<MachineDescriptor> machines = controlPlane.listMachines();
        assertEquals(1, machines.size());

        MachineDescriptor found = machines.getFirst();
        assertEquals("PaymentValidationMachine", found.name());
        assertEquals(MachineType.ATOMIC, found.type());
        assertEquals("IDLE", found.initialState());
        assertEquals(Set.of("FINISHED"), found.endStates());
        assertTrue(found.allStates().containsAll(List.of("IDLE", "PROCESSING", "FINISHED")));
        assertTrue(found.mermaidGraph().startsWith("stateDiagram-v2"));
        assertTrue(found.mermaidGraph().contains("[*] --> IDLE"));
        assertTrue(found.mermaidGraph().contains("IDLE --> PROCESSING"));

        // Direct lookup
        Optional<MachineDescriptor> lookup = controlPlane.getMachine("PaymentValidationMachine");
        assertTrue(lookup.isPresent());
        assertEquals(found, lookup.get());

        // Unregister
        assertTrue(controlPlane.unregister("PaymentValidationMachine"));
        assertTrue(controlPlane.listMachines().isEmpty());
    }

    @Test
    @DisplayName("Should track atomic machine execution lifecycle in real-time")
    void testAtomicExecutionTracking() {
        UUID execId = UUID.randomUUID();
        Instant now = Instant.now();

        // Simulate execution telemetry emitted into Control Plane
        controlPlane.onEvent(new ExecutionEvent.TurnStartedEvent(execId, "OrderFulfillmentFSM", "ORD-10", now));
        controlPlane.onEvent(new ExecutionEvent.TransitionEvaluatedEvent(execId, "OrderFulfillmentFSM", "IDLE", "PROCESSING", now));
        controlPlane.onEvent(new ExecutionEvent.TransitionEvaluatedEvent(execId, "OrderFulfillmentFSM", "PROCESSING", "FINISHED", now));
        controlPlane.onEvent(new ExecutionEvent.TurnCompletedEvent(execId, "OrderFulfillmentFSM", "FINISHED", "ORD-10", Duration.ofMillis(10), now));

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
        record OrderCheckpoint(String correlationKey, String currentStateKey) {}
        OrderCheckpoint checkpoint = new OrderCheckpoint("ORD-4200", "WAIT_APPROVAL");

        FakeInspectableMachine orchMachine = new FakeInspectableMachine("OrderApprovalWorkflow", MachineType.ORCHESTRATION)
                .withCheckpoint("ORD-4200", checkpoint)
                .withSignalHandler(sig -> new SignalDeliveryResult(
                        true,
                        "Signal delivered successfully",
                        "OrderApprovalWorkflow",
                        sig.correlationKey(),
                        sig.signalName(),
                        true,
                        false,
                        "COMPLETED",
                        null
                ));

        controlPlane.register(orchMachine);

        // 1. Telemetry indicates workflow suspended at WAIT_APPROVAL
        UUID execId = UUID.randomUUID();
        Instant now = Instant.now();
        controlPlane.onEvent(new ExecutionEvent.TurnStartedEvent(execId, "OrderApprovalWorkflow", "ORD-4200", now));
        controlPlane.onEvent(new ExecutionEvent.TurnSuspendedEvent(
                execId, "OrderApprovalWorkflow", "WAIT_APPROVAL", "ApprovalSignal", "ORD-4200", Duration.ofMillis(10), now
        ));

        // Verify Control Plane sees suspended execution
        List<ExecutionSummary> suspendedList = controlPlane.listExecutions("OrderApprovalWorkflow", ExecutionStatus.SUSPENDED, 10);
        assertEquals(1, suspendedList.size());

        ExecutionSummary suspendedSummary = suspendedList.getFirst();
        assertEquals("WAIT_APPROVAL", suspendedSummary.currentState());
        assertEquals(ExecutionStatus.SUSPENDED, suspendedSummary.status());
        assertEquals("ORD-4200", suspendedSummary.correlationKey());
        assertEquals("ApprovalSignal", suspendedSummary.suspendedSignal());

        // 2. Checkpoint inspection via Control Plane
        Optional<OrderCheckpoint> checkpointOpt = controlPlane.inspectCheckpoint("OrderApprovalWorkflow", "ORD-4200", OrderCheckpoint.class);
        assertTrue(checkpointOpt.isPresent());
        assertEquals("ORD-4200", checkpointOpt.get().correlationKey());
        assertEquals("WAIT_APPROVAL", checkpointOpt.get().currentStateKey());

        // 3. Deliver signal via Control Plane
        CompletableFuture<SignalDeliveryResult> signalFuture = controlPlane.sendSignal(
                "OrderApprovalWorkflow",
                "ORD-4200",
                "ApprovalSignal",
                "ApprovedByAlice"
        );

        SignalDeliveryResult deliveryResult = signalFuture.get();
        assertTrue(deliveryResult.delivered(), "Signal must be delivered");
        assertTrue(deliveryResult.completed(), "Turn must complete workflow");
        assertFalse(deliveryResult.suspended());
        assertEquals("COMPLETED", deliveryResult.resultingState());
        orchMachine.assertSignalReceived("ApprovalSignal", "ORD-4200");

        // 4. Telemetry indicates turn completion
        controlPlane.onEvent(new ExecutionEvent.TurnCompletedEvent(execId, "OrderApprovalWorkflow", "COMPLETED", "ORD-4200", Duration.ofMillis(10), now));

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
        FakeInspectableMachine atomicMachine = new FakeInspectableMachine("ReadOnlyAtomic", MachineType.ATOMIC)
                .withSignalHandler(sig -> SignalDeliveryResult.failure("ReadOnlyAtomic", sig.correlationKey(), sig.signalName(),
                        "Atomic state machines do not support external signal suspension or delivery"));

        controlPlane.register(atomicMachine);

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

    @Test
    @DisplayName("Should register batch machine, inspect descriptor, send signal, and retrieve checkpoint")
    void testRegisterBatchOrchestrationExecutor() throws Exception {
        MachineDescriptor batchDesc = new MachineDescriptor(
                "DocumentBatch",
                MachineType.BATCH,
                "LOAD",
                Set.of("DONE"),
                List.of("LOAD", "PROCESS", "AWAIT_APPROVAL", "DONE"),
                "stateDiagram-v2\n    [*] --> LOAD\n    note right of PROCESS : Barrier (ALL_ITEMS_ARRIVED)\n    note right of AWAIT_APPROVAL : Barrier (SIGNAL_TRIGGERED)\n    DONE --> [*]\n"
        );
        record BatchCheckpoint(String batchName, String batchKey, String state) {}
        BatchCheckpoint batchCp = new BatchCheckpoint("DocumentBatch", "BATCH-001", "AWAIT_APPROVAL");

        FakeInspectableMachine batchMachine = new FakeInspectableMachine(batchDesc)
                .withCheckpoint("BATCH-001", batchCp)
                .withSignalHandler(sig -> new SignalDeliveryResult(
                        true, "Signal delivered to batch successfully", "DocumentBatch", sig.correlationKey(), sig.signalName(),
                        true, false, "DONE", null
                ));

        controlPlane.register(batchMachine);

        // 1. Inspect descriptor
        Optional<MachineDescriptor> descOpt = controlPlane.getMachine("DocumentBatch");
        assertTrue(descOpt.isPresent());
        MachineDescriptor desc = descOpt.get();
        assertEquals("DocumentBatch", desc.name());
        assertEquals(MachineType.BATCH, desc.type());
        assertEquals("LOAD", desc.initialState());
        assertTrue(desc.endStates().contains("DONE"));
        assertTrue(desc.mermaidGraph().contains("Barrier (ALL_ITEMS_ARRIVED)"));
        assertTrue(desc.mermaidGraph().contains("Barrier (SIGNAL_TRIGGERED)"));

        // 2. Inspect checkpoint through Control Plane
        Optional<BatchCheckpoint> cpOpt =
                controlPlane.inspectCheckpoint("DocumentBatch", "BATCH-001", BatchCheckpoint.class);
        assertTrue(cpOpt.isPresent());
        assertEquals("DocumentBatch", cpOpt.get().batchName());
        assertEquals("AWAIT_APPROVAL", cpOpt.get().state());

        // 3. Send signal through Control Plane
        CompletableFuture<SignalDeliveryResult> signalFuture =
                controlPlane.sendSignal("DocumentBatch", "BATCH-001", "APPROVE_BATCH", "payload");
        SignalDeliveryResult deliveryResult = signalFuture.get();
        assertTrue(deliveryResult.delivered());
        assertTrue(deliveryResult.completed());
        assertEquals("DONE", deliveryResult.resultingState());
        batchMachine.assertSignalReceived("APPROVE_BATCH", "BATCH-001");
    }

    @Test
    @DisplayName("Should maintain segmented O(1) eviction bounds under 10,000 terminal executions without starving active pool")
    void testEvictionStressUnderHighThroughput() {
        int maxTerminal = 500;
        int terminalExecutionsToEmit = 5_000;
        int activeExecutionsToEmit = 100;

        try (DefaultControlPlane stressCp = new DefaultControlPlane(maxTerminal, 50, 1_000)) {
            Instant now = Instant.now();

            // 1. Create active/suspended workflows
            List<UUID> activeIds = new java.util.ArrayList<>();
            for (int i = 0; i < activeExecutionsToEmit; i++) {
                UUID activeId = UUID.randomUUID();
                activeIds.add(activeId);
                stressCp.onEvent(new ExecutionEvent.TurnStartedEvent(activeId, "StressFlow", "CORR-ACT-" + i, now));
                stressCp.onEvent(new ExecutionEvent.TurnSuspendedEvent(activeId, "StressFlow", "WAIT_GATE", "SIG", "CORR-ACT-" + i, Duration.ZERO, now));
            }

            // 2. Concurrently emit 5,000 terminal executions using Virtual Threads
            try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
                for (int i = 0; i < terminalExecutionsToEmit; i++) {
                    final int idx = i;
                    executor.submit(() -> {
                        UUID termId = UUID.randomUUID();
                        stressCp.onEvent(new ExecutionEvent.TurnStartedEvent(termId, "StressFlow", "CORR-TERM-" + idx, now));
                        stressCp.onEvent(new ExecutionEvent.TurnCompletedEvent(termId, "StressFlow", "DONE", "CORR-TERM-" + idx, Duration.ofMillis(1), now));
                    });
                }
            }

            // 3. Verify active executions are 100% retained and untouched
            for (UUID activeId : activeIds) {
                Optional<ExecutionSummary> summaryOpt = stressCp.getExecution(activeId.toString());
                assertTrue(summaryOpt.isPresent(), "Active execution must never be evicted!");
                assertEquals(ExecutionStatus.SUSPENDED, summaryOpt.get().status());
            }

            // 4. Verify listExecutions respects limits
            List<ExecutionSummary> activeList = stressCp.listExecutions(null, ExecutionStatus.SUSPENDED, 1000);
            assertEquals(activeExecutionsToEmit, activeList.size());

            List<ExecutionSummary> terminalList = stressCp.listExecutions(null, ExecutionStatus.COMPLETED, 1000);
            assertTrue(terminalList.size() <= maxTerminal, "Terminal executions must never exceed maxTrackedExecutions limit");
        }
    }
}
