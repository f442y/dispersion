package com.github.f442y.dispersion.serialization.avaje;

import com.github.f442y.dispersion.control.ExecutionStatus;
import com.github.f442y.dispersion.control.ExecutionSummary;
import com.github.f442y.dispersion.control.MachineDescriptor;
import com.github.f442y.dispersion.control.MachineType;
import com.github.f442y.dispersion.control.SignalDeliveryResult;
import com.github.f442y.dispersion.event.ExecutionEvent;
import com.github.f442y.dispersion.event.child.ChildMachineCompletedEvent;
import com.github.f442y.dispersion.event.child.ChildMachineSpawnedEvent;
import com.github.f442y.dispersion.event.compensation.CompensationStepCompletedEvent;
import com.github.f442y.dispersion.event.compensation.CompensationStepFailedEvent;
import com.github.f442y.dispersion.event.compensation.CompensationStepStartedEvent;
import com.github.f442y.dispersion.event.control.ExecutionCancelledEvent;
import com.github.f442y.dispersion.event.control.ExecutionPausedEvent;
import com.github.f442y.dispersion.event.control.ExecutionResumedEvent;
import com.github.f442y.dispersion.event.guard.CircuitBreakerTrippedEvent;
import com.github.f442y.dispersion.event.guard.StateVisitLimitExceededEvent;
import com.github.f442y.dispersion.event.parallel.ParallelBranchCompletedEvent;
import com.github.f442y.dispersion.event.parallel.ParallelForkStartedEvent;
import com.github.f442y.dispersion.event.parallel.ParallelJoinCompletedEvent;
import com.github.f442y.dispersion.event.retry.RetryAttemptedEvent;
import com.github.f442y.dispersion.event.retry.RetryExhaustedEvent;
import com.github.f442y.dispersion.event.signal.SignalAwaitedEvent;
import com.github.f442y.dispersion.event.signal.SignalDeliveredEvent;
import com.github.f442y.dispersion.event.signal.SignalDiscardedEvent;
import com.github.f442y.dispersion.event.signal.SignalTimedOutEvent;
import com.github.f442y.dispersion.event.state.ActionExecutedEvent;
import com.github.f442y.dispersion.event.state.StateEnteredEvent;
import com.github.f442y.dispersion.event.state.StateExitedEvent;
import com.github.f442y.dispersion.event.state.TransitionEvaluatedEvent;
import com.github.f442y.dispersion.event.turn.TurnCompensatedEvent;
import com.github.f442y.dispersion.event.turn.TurnCompletedEvent;
import com.github.f442y.dispersion.event.turn.TurnFailedEvent;
import com.github.f442y.dispersion.event.turn.TurnStartedEvent;
import com.github.f442y.dispersion.event.turn.TurnSuspendedEvent;
import com.github.f442y.dispersion.serialization.json.JsonSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

final class AvajeJsonSerializerTest {

    private JsonSerializer serializer;
    private final UUID machineId = UUID.randomUUID();
    private final Instant now = Instant.parse("2026-09-19T02:00:00Z");

    @BeforeEach
    void setUp() {
        serializer = new AvajeJsonSerializer();
    }

    @Test
    @DisplayName("ServiceLoader successfully discovers AvajeJsonSerializer")
    void shouldDiscoverViaServiceLoader() {
        JsonSerializer discovered = JsonSerializer.load();
        assertThat(discovered).isNotNull();
        assertThat(discovered).isInstanceOf(AvajeJsonSerializer.class);
    }

    @Test
    @DisplayName("Round-trip serialization of MachineDescriptor")
    void shouldRoundTripMachineDescriptor() {
        MachineDescriptor descriptor = new MachineDescriptor(
                "OrderWorkflow",
                MachineType.ORCHESTRATION,
                "ORDER_CREATED",
                Set.of("ORDER_COMPLETED", "ORDER_CANCELLED"),
                List.of("ORDER_CREATED", "PAYMENT_PENDING", "ORDER_COMPLETED", "ORDER_CANCELLED"),
                "graph TD;\nORDER_CREATED-->PAYMENT_PENDING;"
        );

        String json = serializer.serializeDescriptor(descriptor);
        assertThat(json).contains("\"name\":\"OrderWorkflow\"");
        assertThat(json).contains("\"type\":\"ORCHESTRATION\"");

        MachineDescriptor restored = serializer.deserializeDescriptor(json);
        assertThat(restored.name()).isEqualTo("OrderWorkflow");
        assertThat(restored.type()).isEqualTo(MachineType.ORCHESTRATION);
        assertThat(restored.initialState()).isEqualTo("ORDER_CREATED");
        assertThat(restored.endStates()).containsExactlyInAnyOrder("ORDER_COMPLETED", "ORDER_CANCELLED");
        assertThat(restored.allStates()).containsExactly("ORDER_CREATED", "PAYMENT_PENDING", "ORDER_COMPLETED", "ORDER_CANCELLED");
        assertThat(restored.mermaidGraph()).isEqualTo("graph TD;\nORDER_CREATED-->PAYMENT_PENDING;");
    }

    @Test
    @DisplayName("Round-trip serialization of ExecutionSummary")
    void shouldRoundTripExecutionSummary() {
        ExecutionSummary summary = new ExecutionSummary(
                "exec-12345",
                "OrderWorkflow",
                "PAYMENT_PENDING",
                ExecutionStatus.SUSPENDED,
                now,
                null,
                "corr-999",
                "PaymentReceived",
                4L,
                now.plusSeconds(30),
                null
        );

        String json = serializer.serializeSummary(summary);
        assertThat(json).contains("\"executionId\":\"exec-12345\"");
        assertThat(json).contains("\"status\":\"SUSPENDED\"");

        ExecutionSummary restored = serializer.deserializeSummary(json);
        assertThat(restored.executionId()).isEqualTo("exec-12345");
        assertThat(restored.machineName()).isEqualTo("OrderWorkflow");
        assertThat(restored.currentState()).isEqualTo("PAYMENT_PENDING");
        assertThat(restored.status()).isEqualTo(ExecutionStatus.SUSPENDED);
        assertThat(restored.startTime()).isEqualTo(now);
        assertThat(restored.endTime()).isNull();
        assertThat(restored.correlationKey()).isEqualTo("corr-999");
        assertThat(restored.suspendedSignal()).isEqualTo("PaymentReceived");
        assertThat(restored.transitionsCount()).isEqualTo(4L);
        assertThat(restored.lastUpdated()).isEqualTo(now.plusSeconds(30));
        assertThat(restored.errorMessage()).isNull();
    }

    @Test
    @DisplayName("Round-trip serialization of SignalDeliveryResult")
    void shouldRoundTripSignalDeliveryResult() {
        SignalDeliveryResult result = new SignalDeliveryResult(
                true,
                "Signal delivered successfully",
                "OrderWorkflow",
                "corr-999",
                "PaymentReceived",
                false,
                false,
                "FULFILLMENT",
                null
        );

        String json = serializer.serializeSignalResult(result);
        assertThat(json).contains("\"delivered\":true");
        assertThat(json).contains("\"resultingState\":\"FULFILLMENT\"");

        SignalDeliveryResult restored = serializer.deserializeSignalResult(json);
        assertThat(restored.delivered()).isTrue();
        assertThat(restored.message()).isEqualTo("Signal delivered successfully");
        assertThat(restored.machineName()).isEqualTo("OrderWorkflow");
        assertThat(restored.correlationKey()).isEqualTo("corr-999");
        assertThat(restored.signalName()).isEqualTo("PaymentReceived");
        assertThat(restored.completed()).isFalse();
        assertThat(restored.suspended()).isFalse();
        assertThat(restored.resultingState()).isEqualTo("FULFILLMENT");
        assertThat(restored.errorMessage()).isNull();
    }

    @Test
    @DisplayName("Round-trip serialization of Turn events")
    void shouldRoundTripTurnEvents() {
        TurnStartedEvent started = new TurnStartedEvent(machineId, "OrderWorkflow", "corr-1", now);
        ExecutionEvent roundStarted = serializer.deserializeEvent(serializer.serializeEvent(started));
        assertThat(roundStarted).isInstanceOf(TurnStartedEvent.class);
        TurnStartedEvent rStarted = (TurnStartedEvent) roundStarted;
        assertThat(rStarted.machineId()).isEqualTo(machineId);
        assertThat(rStarted.machineName()).isEqualTo("OrderWorkflow");
        assertThat(rStarted.correlationKey()).isEqualTo("corr-1");

        TurnCompletedEvent completed = new TurnCompletedEvent(machineId, "OrderWorkflow", "SUCCESS", "corr-1", Duration.ofMillis(120), now);
        ExecutionEvent roundCompleted = serializer.deserializeEvent(serializer.serializeEvent(completed));
        assertThat(roundCompleted).isInstanceOf(TurnCompletedEvent.class);
        TurnCompletedEvent rCompleted = (TurnCompletedEvent) roundCompleted;
        assertThat(rCompleted.finalStateName()).isEqualTo("SUCCESS");
        assertThat(rCompleted.duration()).isEqualTo(Duration.ofMillis(120));

        TurnFailedEvent failed = new TurnFailedEvent(machineId, "OrderWorkflow", "PAYMENT", new IllegalStateException("Card declined"), "corr-1", Duration.ofMillis(50), now);
        ExecutionEvent roundFailed = serializer.deserializeEvent(serializer.serializeEvent(failed));
        assertThat(roundFailed).isInstanceOf(TurnFailedEvent.class);
        TurnFailedEvent rFailed = (TurnFailedEvent) roundFailed;
        assertThat(rFailed.failedStateName()).isEqualTo("PAYMENT");
        assertThat(rFailed.cause().getMessage()).isEqualTo("Card declined");

        TurnSuspendedEvent suspended = new TurnSuspendedEvent(machineId, "OrderWorkflow", "AWAITING_PAYMENT", "PaymentReceived", "corr-1", Duration.ofMillis(10), now);
        ExecutionEvent roundSuspended = serializer.deserializeEvent(serializer.serializeEvent(suspended));
        assertThat(roundSuspended).isInstanceOf(TurnSuspendedEvent.class);
        TurnSuspendedEvent rSuspended = (TurnSuspendedEvent) roundSuspended;
        assertThat(rSuspended.stateName()).isEqualTo("AWAITING_PAYMENT");
        assertThat(rSuspended.expectedSignal()).isEqualTo("PaymentReceived");

        TurnCompensatedEvent compensated = new TurnCompensatedEvent(machineId, "OrderWorkflow", "INVENTORY", List.of("ORDER_STEP"), new RuntimeException("Rollback"), "corr-1", Duration.ofMillis(90), now);
        ExecutionEvent roundCompensated = serializer.deserializeEvent(serializer.serializeEvent(compensated));
        assertThat(roundCompensated).isInstanceOf(TurnCompensatedEvent.class);
        TurnCompensatedEvent rCompensated = (TurnCompensatedEvent) roundCompensated;
        assertThat(rCompensated.compensatedStates()).containsExactly("ORDER_STEP");
    }

    @Test
    @DisplayName("Round-trip serialization of State lifecycle events")
    void shouldRoundTripStateEvents() {
        StateEnteredEvent entered = new StateEnteredEvent(machineId, "M", "S1", now);
        StateEnteredEvent rEntered = (StateEnteredEvent) serializer.deserializeEvent(serializer.serializeEvent(entered));
        assertThat(rEntered.stateName()).isEqualTo("S1");

        StateExitedEvent exited = new StateExitedEvent(machineId, "M", "S1", Duration.ofMillis(15), now);
        StateExitedEvent rExited = (StateExitedEvent) serializer.deserializeEvent(serializer.serializeEvent(exited));
        assertThat(rExited.stateName()).isEqualTo("S1");
        assertThat(rExited.duration()).isEqualTo(Duration.ofMillis(15));

        TransitionEvaluatedEvent evaluated = new TransitionEvaluatedEvent(machineId, "M", "S1", "S2", now);
        TransitionEvaluatedEvent rEvaluated = (TransitionEvaluatedEvent) serializer.deserializeEvent(serializer.serializeEvent(evaluated));
        assertThat(rEvaluated.sourceState()).isEqualTo("S1");
        assertThat(rEvaluated.targetState()).isEqualTo("S2");

        ActionExecutedEvent action = new ActionExecutedEvent(machineId, "M", "S1", Duration.ofMillis(5), now);
        ActionExecutedEvent rAction = (ActionExecutedEvent) serializer.deserializeEvent(serializer.serializeEvent(action));
        assertThat(rAction.stateName()).isEqualTo("S1");
        assertThat(rAction.duration()).isEqualTo(Duration.ofMillis(5));
    }

    @Test
    @DisplayName("Round-trip serialization of Signal events")
    void shouldRoundTripSignalEvents() {
        SignalAwaitedEvent awaited = new SignalAwaitedEvent(machineId, "M", "S1", "SigA", "k1", now);
        SignalAwaitedEvent rAwaited = (SignalAwaitedEvent) serializer.deserializeEvent(serializer.serializeEvent(awaited));
        assertThat(rAwaited.expectedSignal()).isEqualTo("SigA");
        assertThat(rAwaited.correlationKey()).isEqualTo("k1");

        SignalDeliveredEvent delivered = new SignalDeliveredEvent(machineId, "M", "S1", "SigA", "k1", now);
        SignalDeliveredEvent rDelivered = (SignalDeliveredEvent) serializer.deserializeEvent(serializer.serializeEvent(delivered));
        assertThat(rDelivered.signalName()).isEqualTo("SigA");

        SignalDiscardedEvent discarded = new SignalDiscardedEvent(machineId, "M", "SigB", "k1", "Unmatched key", now);
        SignalDiscardedEvent rDiscarded = (SignalDiscardedEvent) serializer.deserializeEvent(serializer.serializeEvent(discarded));
        assertThat(rDiscarded.reason()).isEqualTo("Unmatched key");

        SignalTimedOutEvent timedOut = new SignalTimedOutEvent(machineId, "M", "S1", "SigA", "k1", Duration.ofSeconds(30), now);
        SignalTimedOutEvent rTimedOut = (SignalTimedOutEvent) serializer.deserializeEvent(serializer.serializeEvent(timedOut));
        assertThat(rTimedOut.timeout()).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    @DisplayName("Round-trip serialization of Compensation events")
    void shouldRoundTripCompensationEvents() {
        CompensationStepStartedEvent started = new CompensationStepStartedEvent(machineId, "M", "S1", true, now);
        CompensationStepStartedEvent rStarted = (CompensationStepStartedEvent) serializer.deserializeEvent(serializer.serializeEvent(started));
        assertThat(rStarted.isRouted()).isTrue();

        CompensationStepCompletedEvent completed = new CompensationStepCompletedEvent(machineId, "M", "S1", Duration.ofMillis(20), now);
        CompensationStepCompletedEvent rCompleted = (CompensationStepCompletedEvent) serializer.deserializeEvent(serializer.serializeEvent(completed));
        assertThat(rCompleted.duration()).isEqualTo(Duration.ofMillis(20));

        CompensationStepFailedEvent failed = new CompensationStepFailedEvent(machineId, "M", "S1", new RuntimeException("Comp failed"), now);
        CompensationStepFailedEvent rFailed = (CompensationStepFailedEvent) serializer.deserializeEvent(serializer.serializeEvent(failed));
        assertThat(rFailed.cause().getMessage()).isEqualTo("Comp failed");
    }

    @Test
    @DisplayName("Round-trip serialization of Retry events")
    void shouldRoundTripRetryEvents() {
        RetryAttemptedEvent attempted = new RetryAttemptedEvent(machineId, "M", "S1", 1, 3, Duration.ofMillis(100), new RuntimeException("Attempt 1"), now);
        RetryAttemptedEvent rAttempted = (RetryAttemptedEvent) serializer.deserializeEvent(serializer.serializeEvent(attempted));
        assertThat(rAttempted.attempt()).isEqualTo(1);
        assertThat(rAttempted.maxAttempts()).isEqualTo(3);
        assertThat(rAttempted.delay()).isEqualTo(Duration.ofMillis(100));

        RetryExhaustedEvent exhausted = new RetryExhaustedEvent(machineId, "M", "S1", 3, new RuntimeException("Retries failed"), now);
        RetryExhaustedEvent rExhausted = (RetryExhaustedEvent) serializer.deserializeEvent(serializer.serializeEvent(exhausted));
        assertThat(rExhausted.attempts()).isEqualTo(3);
    }

    @Test
    @DisplayName("Round-trip serialization of Child machine events")
    void shouldRoundTripChildEvents() {
        UUID childId = UUID.randomUUID();
        ChildMachineSpawnedEvent spawned = new ChildMachineSpawnedEvent(machineId, "ParentM", childId, "ChildM", "ParentState", now);
        ChildMachineSpawnedEvent rSpawned = (ChildMachineSpawnedEvent) serializer.deserializeEvent(serializer.serializeEvent(spawned));
        assertThat(rSpawned.childMachineId()).isEqualTo(childId);
        assertThat(rSpawned.childMachineName()).isEqualTo("ChildM");
        assertThat(rSpawned.parentStateName()).isEqualTo("ParentState");

        ChildMachineCompletedEvent completed = new ChildMachineCompletedEvent(machineId, "ParentM", childId, "ChildM", now);
        ChildMachineCompletedEvent rCompleted = (ChildMachineCompletedEvent) serializer.deserializeEvent(serializer.serializeEvent(completed));
        assertThat(rCompleted.childMachineId()).isEqualTo(childId);
    }

    @Test
    @DisplayName("Round-trip serialization of Parallel events")
    void shouldRoundTripParallelEvents() {
        ParallelForkStartedEvent fork = new ParallelForkStartedEvent(machineId, "M", "FORK_STATE", List.of("BranchA", "BranchB"), now);
        ParallelForkStartedEvent rFork = (ParallelForkStartedEvent) serializer.deserializeEvent(serializer.serializeEvent(fork));
        assertThat(rFork.branchNames()).containsExactly("BranchA", "BranchB");

        ParallelBranchCompletedEvent branch = new ParallelBranchCompletedEvent(machineId, "M", "FORK_STATE", "BranchA", Duration.ofMillis(45), now);
        ParallelBranchCompletedEvent rBranch = (ParallelBranchCompletedEvent) serializer.deserializeEvent(serializer.serializeEvent(branch));
        assertThat(rBranch.branchName()).isEqualTo("BranchA");

        ParallelJoinCompletedEvent join = new ParallelJoinCompletedEvent(machineId, "M", "JOIN_STATE", 2, Duration.ofMillis(60), now);
        ParallelJoinCompletedEvent rJoin = (ParallelJoinCompletedEvent) serializer.deserializeEvent(serializer.serializeEvent(join));
        assertThat(rJoin.totalBranches()).isEqualTo(2);
    }

    @Test
    @DisplayName("Round-trip serialization of Guard events")
    void shouldRoundTripGuardEvents() {
        StateVisitLimitExceededEvent limit = new StateVisitLimitExceededEvent(machineId, "M", "LOOP_STATE", 10, "FALLBACK", now);
        StateVisitLimitExceededEvent rLimit = (StateVisitLimitExceededEvent) serializer.deserializeEvent(serializer.serializeEvent(limit));
        assertThat(rLimit.visitLimit()).isEqualTo(10);
        assertThat(rLimit.fallbackState()).isEqualTo("FALLBACK");

        CircuitBreakerTrippedEvent circuit = new CircuitBreakerTrippedEvent(machineId, "M", 100, now);
        CircuitBreakerTrippedEvent rCircuit = (CircuitBreakerTrippedEvent) serializer.deserializeEvent(serializer.serializeEvent(circuit));
        assertThat(rCircuit.maxTransitions()).isEqualTo(100);
    }

    @Test
    @DisplayName("Round-trip serialization of Control plane events")
    void shouldRoundTripControlPlaneEvents() {
        ExecutionPausedEvent paused = new ExecutionPausedEvent(machineId, "M", "S1", "Maintenance window", now);
        ExecutionPausedEvent rPaused = (ExecutionPausedEvent) serializer.deserializeEvent(serializer.serializeEvent(paused));
        assertThat(rPaused.reason()).isEqualTo("Maintenance window");

        ExecutionResumedEvent resumed = new ExecutionResumedEvent(machineId, "M", "S1", now);
        ExecutionResumedEvent rResumed = (ExecutionResumedEvent) serializer.deserializeEvent(serializer.serializeEvent(resumed));
        assertThat(rResumed.stateName()).isEqualTo("S1");

        ExecutionCancelledEvent cancelled = new ExecutionCancelledEvent(machineId, "M", "S1", "admin-1", "User requested cancel", now);
        ExecutionCancelledEvent rCancelled = (ExecutionCancelledEvent) serializer.deserializeEvent(serializer.serializeEvent(cancelled));
        assertThat(rCancelled.operatorId()).isEqualTo("admin-1");
        assertThat(rCancelled.reason()).isEqualTo("User requested cancel");
    }
}
