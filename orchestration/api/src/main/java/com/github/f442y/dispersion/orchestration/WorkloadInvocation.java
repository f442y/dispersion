package com.github.f442y.dispersion.orchestration;

import com.github.f442y.dispersion.fsm.config.StateMachineConfiguration;
import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.routing.policy.RoutingSelector;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Immutable definition of a workload execution within an orchestration step,
 * unifying in-process child state machines and distributed microservice worker calls.
 *
 * @param <CONTEXT>  The orchestration context type
 * @param <REQUEST>  The request payload type
 * @param <RESPONSE> The response payload type
 */
public record WorkloadInvocation<
        CONTEXT extends StateMachineContext,
        REQUEST,
        RESPONSE>(
        @NonNull String serviceName,
        @Nullable StateMachineConfiguration<?, ?, REQUEST, RESPONSE> localStateMachine,
        @NonNull Class<REQUEST> requestType,
        @NonNull Class<RESPONSE> responseType,
        @NonNull Function<CONTEXT, REQUEST> inputExtractor,
        @NonNull BiFunction<CONTEXT, RESPONSE, CONTEXT> outputMerger,
        @NonNull RoutingSelector routingSelector,
        @NonNull RetryPolicy retryPolicy,
        @Nullable ContextRecoverer<CONTEXT, REQUEST> recoverer,
        @NonNull WorkloadExecutionMode executionMode,
        @NonNull Duration timeout,
        @Nullable Function<CONTEXT, ?> routedCompensationExtractor
) {

    public WorkloadInvocation {
        Objects.requireNonNull(serviceName, "serviceName must not be null");
        Objects.requireNonNull(requestType, "requestType must not be null");
        Objects.requireNonNull(responseType, "responseType must not be null");
        Objects.requireNonNull(inputExtractor, "inputExtractor must not be null");
        Objects.requireNonNull(outputMerger, "outputMerger must not be null");
        Objects.requireNonNull(routingSelector, "routingSelector must not be null");
        Objects.requireNonNull(retryPolicy, "retryPolicy must not be null");
        Objects.requireNonNull(executionMode, "executionMode must not be null");
        Objects.requireNonNull(timeout, "timeout must not be null");
    }

    public boolean hasLocalStateMachine() {
        return localStateMachine != null;
    }

    public boolean hasRoutedCompensation() {
        return routedCompensationExtractor != null;
    }
}
