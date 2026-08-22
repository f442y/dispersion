package com.github.f442y.dispersion.orchestration;

import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.state.Action;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * Definition of a single concurrent branch within a parallel fork-join orchestration state.
 *
 * @param <CONTEXT> The parent context type
 */
public record ParallelBranch<CONTEXT extends StateMachineContext>(
        @NonNull String name,
        @NonNull Action<CONTEXT> action,
        @Nullable CompensationAction<CONTEXT> compensationAction
) {

    public ParallelBranch {
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(action, "action must not be null");
    }

    public static <C extends StateMachineContext> ParallelBranch<C> of(
            @NonNull String name,
            @NonNull Action<C> action
    ) {
        return new ParallelBranch<>(name, action, null);
    }

    public static <C extends StateMachineContext> ParallelBranch<C> of(
            @NonNull String name,
            @NonNull Action<C> action,
            @Nullable CompensationAction<C> compensationAction
    ) {
        return new ParallelBranch<>(name, action, compensationAction);
    }
}
