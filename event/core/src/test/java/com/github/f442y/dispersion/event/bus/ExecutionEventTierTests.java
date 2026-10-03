package com.github.f442y.dispersion.event.bus;

import com.github.f442y.dispersion.event.EventTier;
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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Execution Event Tier Classification Tests")
class ExecutionEventTierTests {

    private final UUID machineId = UUID.randomUUID();
    private final String machineName = "TestMachine";
    private final Instant now = Instant.now();
    private final Duration duration = Duration.ofMillis(50);

    @Test
    @DisplayName("Turn lifecycle events must be classified as LIFECYCLE tier")
    void testTurnLifecycleEventsAreLifecycleTier() {
        TurnStartedEvent started = new TurnStartedEvent(machineId, machineName, "corr-1", now);
        assertThat(started.tier()).isEqualTo(EventTier.LIFECYCLE);
        assertThat(started.isLifecycle()).isTrue();
        assertThat(started.isGranular()).isFalse();

        TurnCompletedEvent completed = new TurnCompletedEvent(machineId, machineName, "EndState", "corr-1", duration, now);
        assertThat(completed.tier()).isEqualTo(EventTier.LIFECYCLE);
        assertThat(completed.isLifecycle()).isTrue();
        assertThat(completed.isGranular()).isFalse();

        TurnSuspendedEvent suspended = new TurnSuspendedEvent(machineId, machineName, "WaitState", "SigX", "corr-1", duration, now);
        assertThat(suspended.tier()).isEqualTo(EventTier.LIFECYCLE);
        assertThat(suspended.isLifecycle()).isTrue();
        assertThat(suspended.isGranular()).isFalse();

        TurnFailedEvent failed = new TurnFailedEvent(machineId, machineName, "FailState", new RuntimeException("err"), "corr-1", duration, now);
        assertThat(failed.tier()).isEqualTo(EventTier.LIFECYCLE);
        assertThat(failed.isLifecycle()).isTrue();
        assertThat(failed.isGranular()).isFalse();

        TurnCompensatedEvent compensated = new TurnCompensatedEvent(machineId, machineName, "FailState", List.of("S1"), new RuntimeException("err"), "corr-1", duration, now);
        assertThat(compensated.tier()).isEqualTo(EventTier.LIFECYCLE);
        assertThat(compensated.isLifecycle()).isTrue();
        assertThat(compensated.isGranular()).isFalse();
    }

    @Test
    @DisplayName("Control plane operator events must be classified as LIFECYCLE tier")
    void testControlPlaneEventsAreLifecycleTier() {
        ExecutionCancelledEvent cancelled = new ExecutionCancelledEvent(machineId, machineName, "StateA", "admin", "Operator cancel", now);
        assertThat(cancelled.tier()).isEqualTo(EventTier.LIFECYCLE);
        assertThat(cancelled.isLifecycle()).isTrue();
        assertThat(cancelled.isGranular()).isFalse();

        ExecutionPausedEvent paused = new ExecutionPausedEvent(machineId, machineName, "StateA", "Operator pause", now);
        assertThat(paused.tier()).isEqualTo(EventTier.LIFECYCLE);
        assertThat(paused.isLifecycle()).isTrue();
        assertThat(paused.isGranular()).isFalse();

        ExecutionResumedEvent resumed = new ExecutionResumedEvent(machineId, machineName, "StateA", now);
        assertThat(resumed.tier()).isEqualTo(EventTier.LIFECYCLE);
        assertThat(resumed.isLifecycle()).isTrue();
        assertThat(resumed.isGranular()).isFalse();
    }

    @Test
    @DisplayName("State lifecycle events must be classified as GRANULAR tier")
    void testStateLifecycleEventsAreGranularTier() {
        StateEnteredEvent entered = new StateEnteredEvent(machineId, machineName, "StateA", now);
        assertThat(entered.tier()).isEqualTo(EventTier.GRANULAR);
        assertThat(entered.isLifecycle()).isFalse();
        assertThat(entered.isGranular()).isTrue();

        StateExitedEvent exited = new StateExitedEvent(machineId, machineName, "StateA", duration, now);
        assertThat(exited.tier()).isEqualTo(EventTier.GRANULAR);
        assertThat(exited.isLifecycle()).isFalse();
        assertThat(exited.isGranular()).isTrue();

        TransitionEvaluatedEvent transition = new TransitionEvaluatedEvent(machineId, machineName, "StateA", "StateB", now);
        assertThat(transition.tier()).isEqualTo(EventTier.GRANULAR);
        assertThat(transition.isLifecycle()).isFalse();
        assertThat(transition.isGranular()).isTrue();

        ActionExecutedEvent action = new ActionExecutedEvent(machineId, machineName, "StateA", duration, now);
        assertThat(action.tier()).isEqualTo(EventTier.GRANULAR);
        assertThat(action.isLifecycle()).isFalse();
        assertThat(action.isGranular()).isTrue();
    }

    @Test
    @DisplayName("Signal events must be classified as GRANULAR tier")
    void testSignalEventsAreGranularTier() {
        SignalAwaitedEvent awaited = new SignalAwaitedEvent(machineId, machineName, "StateA", "SigX", "corr-1", now);
        assertThat(awaited.tier()).isEqualTo(EventTier.GRANULAR);
        assertThat(awaited.isGranular()).isTrue();

        SignalDeliveredEvent delivered = new SignalDeliveredEvent(machineId, machineName, "StateA", "SigX", "corr-1", now);
        assertThat(delivered.tier()).isEqualTo(EventTier.GRANULAR);
        assertThat(delivered.isGranular()).isTrue();

        SignalTimedOutEvent timedOut = new SignalTimedOutEvent(machineId, machineName, "StateA", "SigX", "corr-1", Duration.ofSeconds(5), now);
        assertThat(timedOut.tier()).isEqualTo(EventTier.GRANULAR);
        assertThat(timedOut.isGranular()).isTrue();

        SignalDiscardedEvent discarded = new SignalDiscardedEvent(machineId, machineName, "SigX", "corr-1", "No listener", now);
        assertThat(discarded.tier()).isEqualTo(EventTier.GRANULAR);
        assertThat(discarded.isGranular()).isTrue();
    }

    @Test
    @DisplayName("Retry, Guard, Compensation steps, Parallel, and Child events must be classified as GRANULAR tier")
    void testRemainingCategoriesAreGranularTier() {
        RetryAttemptedEvent retryAttempted = new RetryAttemptedEvent(machineId, machineName, "StateA", 1, 3, duration, new RuntimeException("retry"), now);
        assertThat(retryAttempted.tier()).isEqualTo(EventTier.GRANULAR);

        RetryExhaustedEvent retryExhausted = new RetryExhaustedEvent(machineId, machineName, "StateA", 3, new RuntimeException("exhausted"), now);
        assertThat(retryExhausted.tier()).isEqualTo(EventTier.GRANULAR);

        StateVisitLimitExceededEvent visitLimit = new StateVisitLimitExceededEvent(machineId, machineName, "StateA", 100, "FallbackState", now);
        assertThat(visitLimit.tier()).isEqualTo(EventTier.GRANULAR);

        CircuitBreakerTrippedEvent cb = new CircuitBreakerTrippedEvent(machineId, machineName, 50, now);
        assertThat(cb.tier()).isEqualTo(EventTier.GRANULAR);

        CompensationStepStartedEvent compStarted = new CompensationStepStartedEvent(machineId, machineName, "StateA", false, now);
        assertThat(compStarted.tier()).isEqualTo(EventTier.GRANULAR);

        CompensationStepCompletedEvent compCompleted = new CompensationStepCompletedEvent(machineId, machineName, "StateA", duration, now);
        assertThat(compCompleted.tier()).isEqualTo(EventTier.GRANULAR);

        CompensationStepFailedEvent compFailed = new CompensationStepFailedEvent(machineId, machineName, "StateA", new RuntimeException("comp-err"), now);
        assertThat(compFailed.tier()).isEqualTo(EventTier.GRANULAR);

        ParallelForkStartedEvent fork = new ParallelForkStartedEvent(machineId, machineName, "ForkNode", List.of("B1", "B2"), now);
        assertThat(fork.tier()).isEqualTo(EventTier.GRANULAR);

        ParallelBranchCompletedEvent branch = new ParallelBranchCompletedEvent(machineId, machineName, "ForkNode", "B1", duration, now);
        assertThat(branch.tier()).isEqualTo(EventTier.GRANULAR);

        ParallelJoinCompletedEvent join = new ParallelJoinCompletedEvent(machineId, machineName, "JoinNode", 2, duration, now);
        assertThat(join.tier()).isEqualTo(EventTier.GRANULAR);

        ChildMachineSpawnedEvent childSpawned = new ChildMachineSpawnedEvent(machineId, machineName, UUID.randomUUID(), "ChildMachine", "ParentState", now);
        assertThat(childSpawned.tier()).isEqualTo(EventTier.GRANULAR);

        ChildMachineCompletedEvent childCompleted = new ChildMachineCompletedEvent(machineId, machineName, UUID.randomUUID(), "ChildMachine", now);
        assertThat(childCompleted.tier()).isEqualTo(EventTier.GRANULAR);
    }

    @Test
    @DisplayName("Direct implementation of ExecutionEvent defaults to GRANULAR tier")
    void testDirectExecutionEventDefaultsToGranular() {
        record CustomDomainEvent(UUID machineId, String machineName, Instant timestamp) implements ExecutionEvent {
        }

        CustomDomainEvent custom = new CustomDomainEvent(machineId, machineName, now);
        assertThat(custom.tier()).isEqualTo(EventTier.GRANULAR);
        assertThat(custom.isLifecycle()).isFalse();
        assertThat(custom.isGranular()).isTrue();
    }
}
