package com.github.f442y.dispersion.orchestration;

import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.state.Action;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * Represents an independent parallel branch of execution within an orchestration state.
 * Multiple branches execute concurrently on Java Virtual Threads.
 *
 * @param <ORCHESTRATION_CONTEXT> The orchestration context type
 */
public interface ParallelBranch<ORCHESTRATION_CONTEXT extends StateMachineContext> {

    /**
     * Returns the name identifier of this parallel branch.
     *
     * @return The branch name
     */
    @NonNull
    String name();

    /**
     * Executes this parallel branch against the given orchestration context.
     *
     * @param context The current orchestration context
     * @return The updated orchestration context
     * @throws Exception If an error occurs
     */
    @NonNull
    ORCHESTRATION_CONTEXT execute(@NonNull ORCHESTRATION_CONTEXT context) throws Exception;

    /**
     * Returns the branch-level compensation action executed if a sibling branch
     * fails during concurrent execution.
     *
     * @return The {@link CompensationAction} instance
     */
    @NonNull
    CompensationAction<ORCHESTRATION_CONTEXT> compensationAction();

    /**
     * Creates a parallel branch wrapping a direct business action.
     *
     * @param <C>                The orchestration context type
     * @param name               The branch name
     * @param action             The business action
     * @param compensationAction Optional branch compensation
     * @return A new {@link ParallelBranch} instance
     */
    @NonNull
    static <C extends StateMachineContext> ParallelBranch<C> of(
            @NonNull String name,
            @NonNull Action<C> action,
            @Nullable CompensationAction<C> compensationAction
    ) {
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(action, "action must not be null");
        CompensationAction<C> effComp = (compensationAction != null) ? compensationAction : CompensationAction.noop();
        return new DirectActionBranch<>(name, action, effComp);
    }

    /**
     * Direct action implementation of {@link ParallelBranch}.
     */
    record DirectActionBranch<C extends StateMachineContext>(
            @NonNull String name,
            @NonNull Action<C> action,
            @NonNull CompensationAction<C> compensationAction
    ) implements ParallelBranch<C> {
        @NonNull
        @Override
        public C execute(@NonNull C context) throws Exception {
            return action.execute(context);
        }
    }
}
