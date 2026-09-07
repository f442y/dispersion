package com.github.f442y.dispersion.orchestration;

import com.github.f442y.dispersion.config.StateMachineConfiguration;
import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.orchestration.messaging.SignalPublisher;
import com.github.f442y.dispersion.state.Action;
import com.github.f442y.dispersion.state.State;
import com.github.f442y.dispersion.state.StateKey;
import com.github.f442y.dispersion.state.Transition;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * State contract for Orchestration State Machines extending standard {@link State} with
 * child state machine execution, automated Saga compensation, parallel fork-join branching,
 * signal/command suspension, retries with context recovery, and outbound broker publishing.
 *
 * @param <CONTEXT>   The context type
 * @param <STATE_KEY> The state key enum type
 */
public class OrchestrationState<
        CONTEXT extends StateMachineContext,
        STATE_KEY extends Enum<STATE_KEY> & StateKey> implements State<CONTEXT, STATE_KEY> {

    private final Action<CONTEXT> action;
    private final Transition<CONTEXT, STATE_KEY> transition;
    private final Set<STATE_KEY> permittedTargets;
    private final boolean isTerminal;
    private final int maxVisits;
    private final STATE_KEY maxVisitsFallback;

    // Orchestration specific capabilities
    private final CompensationAction<CONTEXT> compensationAction;
    private final StateMachineConfiguration<?, ?, ?, ?> childStateMachine;
    private final Function<CONTEXT, ?> childInputExtractor;
    private final BiFunction<CONTEXT, Object, CONTEXT> childOutputMerger;
    private final ContextRecoverer<CONTEXT, ?> contextRecoverer;
    private final RetryPolicy retryPolicy;
    private final List<ParallelBranch<CONTEXT>> parallelBranches;
    private final String expectedSignal;
    private final SignalHandler<CONTEXT, ?> signalHandler;
    private final SignalPublisher signalPublisher;
    private final String publishDestination;
    private final Function<CONTEXT, ?> publishPayloadExtractor;

    public OrchestrationState(
            @NonNull Action<CONTEXT> action,
            @NonNull Transition<CONTEXT, STATE_KEY> transition,
            @Nullable Set<STATE_KEY> permittedTargets,
            boolean isTerminal,
            int maxVisits,
            @Nullable STATE_KEY maxVisitsFallback,
            @Nullable CompensationAction<CONTEXT> compensationAction,
            @Nullable StateMachineConfiguration<?, ?, ?, ?> childStateMachine,
            @Nullable Function<CONTEXT, ?> childInputExtractor,
            @Nullable BiFunction<CONTEXT, Object, CONTEXT> childOutputMerger,
            @Nullable ContextRecoverer<CONTEXT, ?> contextRecoverer,
            @Nullable RetryPolicy retryPolicy,
            @Nullable List<ParallelBranch<CONTEXT>> parallelBranches,
            @Nullable String expectedSignal,
            @Nullable SignalHandler<CONTEXT, ?> signalHandler,
            @Nullable SignalPublisher signalPublisher,
            @Nullable String publishDestination,
            @Nullable Function<CONTEXT, ?> publishPayloadExtractor
    ) {
        this.action = Objects.requireNonNull(action, "action must not be null");
        this.transition = Objects.requireNonNull(transition, "transition must not be null");
        this.permittedTargets = (permittedTargets != null) ? Set.copyOf(permittedTargets) : Collections.emptySet();
        this.isTerminal = isTerminal;
        this.maxVisits = maxVisits;
        this.maxVisitsFallback = maxVisitsFallback;
        this.compensationAction = compensationAction;
        this.childStateMachine = childStateMachine;
        this.childInputExtractor = childInputExtractor;
        this.childOutputMerger = childOutputMerger;
        this.contextRecoverer = contextRecoverer;
        this.retryPolicy = (retryPolicy != null) ? retryPolicy : RetryPolicy.noRetries();
        this.parallelBranches = (parallelBranches != null) ? List.copyOf(parallelBranches) : Collections.emptyList();
        this.expectedSignal = expectedSignal;
        this.signalHandler = signalHandler;
        this.signalPublisher = signalPublisher;
        this.publishDestination = publishDestination;
        this.publishPayloadExtractor = publishPayloadExtractor;
    }

    @NonNull
    @Override
    public Action<CONTEXT> action() {
        return action;
    }

    @NonNull
    @Override
    public Transition<CONTEXT, STATE_KEY> transition() {
        return transition;
    }

    @NonNull
    @Override
    public Set<STATE_KEY> permittedTargets() {
        return permittedTargets;
    }

    @Override
    public boolean isTerminal() {
        return isTerminal;
    }

    @Override
    public int maxVisits() {
        return maxVisits;
    }

    @Nullable
    @Override
    public STATE_KEY maxVisitsFallback() {
        return maxVisitsFallback;
    }

    @Nullable
    public CompensationAction<CONTEXT> compensationAction() {
        return compensationAction;
    }

    @Nullable
    public StateMachineConfiguration<?, ?, ?, ?> childStateMachine() {
        return childStateMachine;
    }

    @Nullable
    public Function<CONTEXT, ?> childInputExtractor() {
        return childInputExtractor;
    }

    @Nullable
    public BiFunction<CONTEXT, Object, CONTEXT> childOutputMerger() {
        return childOutputMerger;
    }

    @Nullable
    public ContextRecoverer<CONTEXT, ?> contextRecoverer() {
        return contextRecoverer;
    }

    @NonNull
    public RetryPolicy retryPolicy() {
        return retryPolicy;
    }

    @NonNull
    public List<ParallelBranch<CONTEXT>> parallelBranches() {
        return parallelBranches;
    }

    @Nullable
    public String expectedSignal() {
        return expectedSignal;
    }

    @Nullable
    public SignalHandler<CONTEXT, ?> signalHandler() {
        return signalHandler;
    }

    @Nullable
    public SignalPublisher signalPublisher() {
        return signalPublisher;
    }

    @Nullable
    public String publishDestination() {
        return publishDestination;
    }

    @Nullable
    public Function<CONTEXT, ?> publishPayloadExtractor() {
        return publishPayloadExtractor;
    }

    public boolean isSignalWaitState() {
        return expectedSignal != null;
    }

    public boolean isParallelState() {
        return !parallelBranches.isEmpty();
    }

    public boolean hasChildStateMachine() {
        return childStateMachine != null;
    }

    public boolean hasOutboundPublish() {
        return signalPublisher != null && publishDestination != null;
    }
}
