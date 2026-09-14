package com.github.f442y.dispersion.orchestration;

import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.routing.policy.RoutingSelector;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * Represents a registered compensation step in the Saga execution history,
 * supporting both in-process actions and routed compensations dispatched to remote workers.
 *
 * @param <CONTEXT> The state machine context type
 */
public sealed interface CompensationRecord<CONTEXT extends StateMachineContext> {

    @NonNull
    String stateKey();

    /**
     * In-process compensation action modifying the local context.
     *
     * @param <CONTEXT> The state machine context type
     */
    record LocalCompensation<CONTEXT extends StateMachineContext>(
            @NonNull String stateKey,
            @NonNull CompensationAction<CONTEXT> action
    ) implements CompensationRecord<CONTEXT> {
        public LocalCompensation {
            Objects.requireNonNull(stateKey, "stateKey must not be null");
            Objects.requireNonNull(action, "action must not be null");
        }
    }

    /**
     * Cross-boundary compensation routed to an external worker service.
     *
     * @param <CONTEXT> The state machine context type
     * @param <PAYLOAD> The typed compensation request payload
     */
    record RoutedCompensation<CONTEXT extends StateMachineContext, PAYLOAD>(
            @NonNull String stateKey,
            @NonNull String serviceName,
            @NonNull RoutingSelector routingSelector,
            @Nullable PAYLOAD payload
    ) implements CompensationRecord<CONTEXT> {
        public RoutedCompensation {
            Objects.requireNonNull(stateKey, "stateKey must not be null");
            Objects.requireNonNull(serviceName, "serviceName must not be null");
            Objects.requireNonNull(routingSelector, "routingSelector must not be null");
        }
    }
}
