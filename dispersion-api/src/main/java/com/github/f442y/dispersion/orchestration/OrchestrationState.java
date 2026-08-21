package com.github.f442y.dispersion.orchestration;

import com.github.f442y.dispersion.config.StateMachineConfiguration;
import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.state.Action;
import com.github.f442y.dispersion.state.State;
import com.github.f442y.dispersion.state.StateKey;
import com.github.f442y.dispersion.state.Transition;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * Specialized {@link State} node contract for Orchestration State Machines.
 * Can execute a direct action, a child state machine (atomic micro-machine or nested orchestration machine),
 * or multiple parallel independent branches concurrently, while supporting {@link ContextRecoverer} retries
 * on virtual threads and Saga {@link CompensationAction} rollbacks.
 *
 * @param <ORCHESTRATION_CONTEXT>   The orchestration context type
 * @param <ORCHESTRATION_STATE_KEY> The orchestration state key enum type
 */
public interface OrchestrationState<
        ORCHESTRATION_CONTEXT extends StateMachineContext,
        ORCHESTRATION_STATE_KEY extends Enum<ORCHESTRATION_STATE_KEY> & StateKey>
        extends State<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY> {

    /**
     * Checks whether this orchestration state executes a child state machine.
     *
     * @return {@code true} if configured with a child state machine; otherwise {@code false}
     */
    default boolean hasChildStateMachine() {
        return childStateMachine() != null;
    }

    /**
     * Legacy alias for {@link #hasChildStateMachine()}.
     *
     * @return {@code true} if configured with a child state machine; otherwise {@code false}
     */
    default boolean hasAtomicStateMachine() {
        return hasChildStateMachine();
    }

    /**
     * Returns the child state machine configuration (Atomic or Nested Orchestration), if present.
     *
     * @return The child state machine configuration, or {@code null}
     */
    @Nullable
    StateMachineConfiguration<?, ?, ?, ?> childStateMachine();

    /**
     * Legacy alias for {@link #childStateMachine()}.
     *
     * @return The child state machine configuration, or {@code null}
     */
    @Nullable
    default StateMachineConfiguration<?, ?, ?, ?> atomicStateMachine() {
        return childStateMachine();
    }

    /**
     * Checks whether this orchestration state executes multiple parallel independent branches.
     *
     * @return {@code true} if parallel branches are present; otherwise {@code false}
     */
    default boolean hasParallelBranches() {
        return !parallelBranches().isEmpty();
    }

    /**
     * Returns the list of parallel branches configured to execute concurrently in this state.
     *
     * @return Unmodifiable list of {@link ParallelBranch} instances
     */
    @NonNull
    List<ParallelBranch<ORCHESTRATION_CONTEXT>> parallelBranches();

    /**
     * Returns the context recoverer responsible for rebuilding clean input upon retries.
     *
     * @return The {@link ContextRecoverer}, or {@code null}
     */
    @Nullable
    ContextRecoverer<ORCHESTRATION_CONTEXT, ?> contextRecoverer();

    /**
     * Returns the output merger function combining the child machine result into the parent orchestration context.
     *
     * @return The output merger function, or {@code null}
     */
    @Nullable
    BiFunction<ORCHESTRATION_CONTEXT, ?, ORCHESTRATION_CONTEXT> outputMerger();

    /**
     * Returns the retry policy configured for this state.
     *
     * @return The immutable {@link RetryPolicy} instance
     */
    @NonNull
    RetryPolicy retryPolicy();

    /**
     * Returns the Saga compensation action executed upon flow failure.
     *
     * @return The {@link CompensationAction} instance
     */
    @NonNull
    CompensationAction<ORCHESTRATION_CONTEXT> compensationAction();

    /**
     * Factory method for creating an immutable {@link OrchestrationState}.
     *
     * @param <C> The orchestration context type
     * @param <S> The orchestration state key type
     * @param action The action business logic
     * @param permittedTargets The explicit set of valid target states
     * @param transition The transition routing logic
     * @param isTerminal Whether this state terminates the machine
     * @param maxVisits Maximum visits allowed
     * @param maxVisitsFallback Fallback state key when visit limit is exceeded
     * @param childStateMachine The child state machine configuration (Atomic or Orchestration)
     * @param contextRecoverer The recoverer function
     * @param outputMerger The output merger function
     * @param retryPolicy The retry policy
     * @param compensationAction The compensation action
     * @return A new {@link OrchestrationState} instance
     */
    @NonNull
    static <C extends StateMachineContext, S extends Enum<S> & StateKey> OrchestrationState<C, S> of(
            @Nullable Action<C> action,
            @NonNull Set<S> permittedTargets,
            @NonNull Transition<C, S> transition,
            boolean isTerminal,
            int maxVisits,
            @Nullable S maxVisitsFallback,
            @Nullable StateMachineConfiguration<?, ?, ?, ?> childStateMachine,
            @Nullable ContextRecoverer<C, ?> contextRecoverer,
            @Nullable BiFunction<C, ?, C> outputMerger,
            @Nullable RetryPolicy retryPolicy,
            @Nullable CompensationAction<C> compensationAction
    ) {
        return of(
                action,
                permittedTargets,
                transition,
                isTerminal,
                maxVisits,
                maxVisitsFallback,
                childStateMachine,
                Collections.emptyList(),
                contextRecoverer,
                outputMerger,
                retryPolicy,
                compensationAction
        );
    }

    /**
     * Factory method for creating an immutable {@link OrchestrationState} with parallel branches.
     *
     * @param <C> The orchestration context type
     * @param <S> The orchestration state key type
     * @param action The action business logic
     * @param permittedTargets The explicit set of valid target states
     * @param transition The transition routing logic
     * @param isTerminal Whether this state terminates the machine
     * @param maxVisits Maximum visits allowed
     * @param maxVisitsFallback Fallback state key when visit limit is exceeded
     * @param childStateMachine The child state machine configuration
     * @param parallelBranches List of parallel branches to run concurrently
     * @param contextRecoverer The recoverer function
     * @param outputMerger The output merger function
     * @param retryPolicy The retry policy
     * @param compensationAction The compensation action
     * @return A new {@link OrchestrationState} instance
     */
    @NonNull
    static <C extends StateMachineContext, S extends Enum<S> & StateKey> OrchestrationState<C, S> of(
            @Nullable Action<C> action,
            @NonNull Set<S> permittedTargets,
            @NonNull Transition<C, S> transition,
            boolean isTerminal,
            int maxVisits,
            @Nullable S maxVisitsFallback,
            @Nullable StateMachineConfiguration<?, ?, ?, ?> childStateMachine,
            @Nullable List<ParallelBranch<C>> parallelBranches,
            @Nullable ContextRecoverer<C, ?> contextRecoverer,
            @Nullable BiFunction<C, ?, C> outputMerger,
            @Nullable RetryPolicy retryPolicy,
            @Nullable CompensationAction<C> compensationAction
    ) {
        return new SimpleOrchestrationState<>(
                action,
                permittedTargets,
                transition,
                isTerminal,
                maxVisits,
                maxVisitsFallback,
                childStateMachine,
                parallelBranches != null ? List.copyOf(parallelBranches) : Collections.emptyList(),
                contextRecoverer,
                outputMerger,
                retryPolicy != null ? retryPolicy : RetryPolicy.noRetries(),
                compensationAction != null ? compensationAction : CompensationAction.noop()
        );
    }

    /**
     * Default immutable implementation of {@link OrchestrationState}.
     */
    record SimpleOrchestrationState<C extends StateMachineContext, S extends Enum<S> & StateKey>(
            @NonNull Action<C> action,
            @NonNull Set<S> permittedTargets,
            @NonNull Transition<C, S> transition,
            boolean isTerminal,
            int maxVisits,
            @Nullable S maxVisitsFallback,
            @Nullable StateMachineConfiguration<?, ?, ?, ?> childStateMachine,
            @NonNull List<ParallelBranch<C>> parallelBranches,
            @Nullable ContextRecoverer<C, ?> contextRecoverer,
            @Nullable BiFunction<C, ?, C> outputMerger,
            @NonNull RetryPolicy retryPolicy,
            @NonNull CompensationAction<C> compensationAction
    ) implements OrchestrationState<C, S> {
        public SimpleOrchestrationState {
            action = (action != null) ? action : Action.identity();
            permittedTargets = (permittedTargets != null) ? Collections.unmodifiableSet(permittedTargets) : Collections.emptySet();
            transition = (transition != null) ? transition : Transition.terminal();
            parallelBranches = (parallelBranches != null) ? List.copyOf(parallelBranches) : Collections.emptyList();
            retryPolicy = (retryPolicy != null) ? retryPolicy : RetryPolicy.noRetries();
            compensationAction = (compensationAction != null) ? compensationAction : CompensationAction.noop();
        }
    }
}
