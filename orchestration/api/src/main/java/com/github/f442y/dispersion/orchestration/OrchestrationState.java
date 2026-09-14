package com.github.f442y.dispersion.orchestration;

import com.github.f442y.dispersion.fsm.config.StateMachineConfiguration;
import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.fsm.state.Action;
import com.github.f442y.dispersion.fsm.state.State;
import com.github.f442y.dispersion.fsm.state.StateKey;
import com.github.f442y.dispersion.fsm.state.Transition;
import com.github.f442y.dispersion.orchestration.messaging.SignalPublisher;
import com.github.f442y.dispersion.routing.policy.RoutingSelector;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;

/**
 * State contract for Orchestration State Machines extending standard {@link State} with
 * unified workload invocation (in-process child state machines and distributed microservice calls),
 * automated Saga compensations, parallel fork-join branching, signal/command suspension,
 * retries with context recovery, and outbound broker publishing.
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
    private final WorkloadInvocation<CONTEXT, ?, ?> workloadInvocation;
    private final List<ParallelBranch<CONTEXT>> parallelBranches;
    private final Function<CONTEXT, CONTEXT> parallelContextCloner;
    private final BinaryOperator<CONTEXT> parallelContextReducer;
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
            @Nullable WorkloadInvocation<CONTEXT, ?, ?> workloadInvocation,
            @Nullable List<ParallelBranch<CONTEXT>> parallelBranches,
            @Nullable Function<CONTEXT, CONTEXT> parallelContextCloner,
            @Nullable BinaryOperator<CONTEXT> parallelContextReducer,
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
        this.workloadInvocation = workloadInvocation;
        this.parallelBranches = (parallelBranches != null) ? List.copyOf(parallelBranches) : Collections.emptyList();
        this.parallelContextCloner = parallelContextCloner;
        this.parallelContextReducer = parallelContextReducer;
        this.expectedSignal = expectedSignal;
        this.signalHandler = signalHandler;
        this.signalPublisher = signalPublisher;
        this.publishDestination = publishDestination;
        this.publishPayloadExtractor = publishPayloadExtractor;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
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
        this(action, transition, permittedTargets, isTerminal, maxVisits, maxVisitsFallback,
                compensationAction, childStateMachine, childInputExtractor, childOutputMerger,
                contextRecoverer, retryPolicy, parallelBranches, null, null,
                expectedSignal, signalHandler, signalPublisher, publishDestination, publishPayloadExtractor,
                null, null, null);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
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
            @Nullable Function<CONTEXT, CONTEXT> parallelContextCloner,
            @Nullable BinaryOperator<CONTEXT> parallelContextReducer,
            @Nullable String expectedSignal,
            @Nullable SignalHandler<CONTEXT, ?> signalHandler,
            @Nullable SignalPublisher signalPublisher,
            @Nullable String publishDestination,
            @Nullable Function<CONTEXT, ?> publishPayloadExtractor
    ) {
        this(action, transition, permittedTargets, isTerminal, maxVisits, maxVisitsFallback,
                compensationAction, childStateMachine, childInputExtractor, childOutputMerger,
                contextRecoverer, retryPolicy, parallelBranches, parallelContextCloner, parallelContextReducer,
                expectedSignal, signalHandler, signalPublisher, publishDestination, publishPayloadExtractor,
                null, null, null);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
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
            @Nullable Function<CONTEXT, CONTEXT> parallelContextCloner,
            @Nullable BinaryOperator<CONTEXT> parallelContextReducer,
            @Nullable String expectedSignal,
            @Nullable SignalHandler<CONTEXT, ?> signalHandler,
            @Nullable SignalPublisher signalPublisher,
            @Nullable String publishDestination,
            @Nullable Function<CONTEXT, ?> publishPayloadExtractor,
            @Nullable String serviceName,
            @Nullable RoutingSelector routingSelector,
            @Nullable Function<CONTEXT, ?> routedCompensationExtractor
    ) {
        this(
                action,
                transition,
                permittedTargets,
                isTerminal,
                maxVisits,
                maxVisitsFallback,
                compensationAction,
                buildWorkloadInvocation(
                        childStateMachine,
                        childInputExtractor,
                        childOutputMerger,
                        contextRecoverer,
                        retryPolicy,
                        serviceName,
                        routingSelector,
                        routedCompensationExtractor
                ),
                parallelBranches,
                parallelContextCloner,
                parallelContextReducer,
                expectedSignal,
                signalHandler,
                signalPublisher,
                publishDestination,
                publishPayloadExtractor
        );
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <CONTEXT extends StateMachineContext> WorkloadInvocation<CONTEXT, ?, ?> buildWorkloadInvocation(
            @Nullable StateMachineConfiguration<?, ?, ?, ?> childStateMachine,
            @Nullable Function<CONTEXT, ?> childInputExtractor,
            @Nullable BiFunction<CONTEXT, Object, CONTEXT> childOutputMerger,
            @Nullable ContextRecoverer<CONTEXT, ?> contextRecoverer,
            @Nullable RetryPolicy retryPolicy,
            @Nullable String serviceName,
            @Nullable RoutingSelector routingSelector,
            @Nullable Function<CONTEXT, ?> routedCompensationExtractor
    ) {
        if (childStateMachine != null) {
            String name = (serviceName != null) ? serviceName : childStateMachine.getMachineName();
            Function inFn = (childInputExtractor != null) ? childInputExtractor : (Function<Object, Object>) ctx -> null;
            BiFunction outFn = (childOutputMerger != null) ? childOutputMerger : (BiFunction<Object, Object, Object>) (ctx, out) -> ctx;
            RetryPolicy rp = (retryPolicy != null) ? retryPolicy : RetryPolicy.noRetries();
            RoutingSelector sel = (routingSelector != null) ? routingSelector : RoutingSelector.any();
            return new WorkloadInvocation<>(
                    name,
                    (StateMachineConfiguration) childStateMachine,
                    Object.class,
                    Object.class,
                    inFn,
                    outFn,
                    sel,
                    rp,
                    (ContextRecoverer) contextRecoverer,
                    WorkloadExecutionMode.SYNCHRONOUS_VIRTUAL_THREAD,
                    Duration.ofSeconds(30),
                    routedCompensationExtractor
            );
        }
        if (serviceName != null) {
            Function inFn = (childInputExtractor != null) ? childInputExtractor : (Function<Object, Object>) ctx -> null;
            BiFunction outFn = (childOutputMerger != null) ? childOutputMerger : (BiFunction<Object, Object, Object>) (ctx, out) -> ctx;
            RetryPolicy rp = (retryPolicy != null) ? retryPolicy : RetryPolicy.noRetries();
            RoutingSelector sel = (routingSelector != null) ? routingSelector : RoutingSelector.any();
            return new WorkloadInvocation<>(
                    serviceName,
                    null,
                    Object.class,
                    Object.class,
                    inFn,
                    outFn,
                    sel,
                    rp,
                    (ContextRecoverer) contextRecoverer,
                    WorkloadExecutionMode.SYNCHRONOUS_VIRTUAL_THREAD,
                    Duration.ofSeconds(30),
                    routedCompensationExtractor
            );
        }
        return null;
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

    public boolean hasWorkloadInvocation() {
        return workloadInvocation != null;
    }

    @Nullable
    public WorkloadInvocation<CONTEXT, ?, ?> workloadInvocation() {
        return workloadInvocation;
    }

    @Nullable
    public StateMachineConfiguration<?, ?, ?, ?> childStateMachine() {
        return (workloadInvocation != null) ? workloadInvocation.localStateMachine() : null;
    }

    @Nullable
    public Function<CONTEXT, ?> childInputExtractor() {
        return (workloadInvocation != null) ? workloadInvocation.inputExtractor() : null;
    }

    @Nullable
    @SuppressWarnings("unchecked")
    public BiFunction<CONTEXT, Object, CONTEXT> childOutputMerger() {
        return (workloadInvocation != null) ? (BiFunction<CONTEXT, Object, CONTEXT>) workloadInvocation.outputMerger() : null;
    }

    @Nullable
    public ContextRecoverer<CONTEXT, ?> contextRecoverer() {
        return (workloadInvocation != null) ? workloadInvocation.recoverer() : null;
    }

    @NonNull
    public RetryPolicy retryPolicy() {
        return (workloadInvocation != null) ? workloadInvocation.retryPolicy() : RetryPolicy.noRetries();
    }

    public boolean hasChildStateMachine() {
        return workloadInvocation != null && workloadInvocation.hasLocalStateMachine();
    }

    public boolean hasRoutedService() {
        return workloadInvocation != null && !workloadInvocation.hasLocalStateMachine();
    }

    @Nullable
    public String serviceName() {
        return (workloadInvocation != null) ? workloadInvocation.serviceName() : null;
    }

    @NonNull
    public RoutingSelector routingSelector() {
        return (workloadInvocation != null) ? workloadInvocation.routingSelector() : RoutingSelector.any();
    }

    @Nullable
    public Function<CONTEXT, ?> routedCompensationExtractor() {
        return (workloadInvocation != null) ? workloadInvocation.routedCompensationExtractor() : null;
    }

    @NonNull
    public List<ParallelBranch<CONTEXT>> parallelBranches() {
        return parallelBranches;
    }

    public boolean isParallelState() {
        return !parallelBranches.isEmpty();
    }

    @Nullable
    public Function<CONTEXT, CONTEXT> parallelContextCloner() {
        return parallelContextCloner;
    }

    @Nullable
    public BinaryOperator<CONTEXT> parallelContextReducer() {
        return parallelContextReducer;
    }

    @Nullable
    public String expectedSignal() {
        return expectedSignal;
    }

    @Nullable
    public SignalHandler<CONTEXT, ?> signalHandler() {
        return signalHandler;
    }

    public boolean isSignalWaitState() {
        return expectedSignal != null;
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

    public boolean hasOutboundPublish() {
        return signalPublisher != null && publishDestination != null && publishPayloadExtractor != null;
    }

    public boolean hasPublishTrigger() {
        return hasOutboundPublish();
    }
}
