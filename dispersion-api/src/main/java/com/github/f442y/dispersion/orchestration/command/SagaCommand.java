package com.github.f442y.dispersion.orchestration.command;

import com.github.f442y.dispersion.context.StateMachineContext;
import org.jspecify.annotations.NonNull;

/**
 * Reversible Command encapsulating a forward business operation ({@link #execute(StateMachineContext)})
 * paired with an undo compensation ({@link #undo(StateMachineContext)}) for automated Saga rollbacks.
 *
 * @param <CONTEXT> The concrete type of {@link StateMachineContext} managed by the state machine
 */
@FunctionalInterface
public interface SagaCommand<CONTEXT extends StateMachineContext> {

    /**
     * Executes the forward business logic, mutating and returning the updated context.
     *
     * @param context The current state machine context
     * @return The updated context
     * @throws Exception If business or integration execution fails
     */
    @NonNull
    CONTEXT execute(@NonNull CONTEXT context) throws Exception;

    /**
     * Reverses or compensates the effects of {@link #execute(StateMachineContext)} upon downstream failure.
     * Default implementation performs no compensation action (no-op).
     *
     * @param context The context at the point of compensation
     * @return The compensated context
     */
    @NonNull
    default CONTEXT undo(@NonNull CONTEXT context) {
        return context;
    }

    /**
     * Factory method creating a {@link SagaCommand} from explicit action and undo lambdas.
     *
     * @param <C>        The context type
     * @param forwardFn  Forward execution function
     * @param rollbackFn Undo compensation function
     * @return A new {@link SagaCommand} instance
     */
    @NonNull
    static <C extends StateMachineContext> SagaCommand<C> of(
            @NonNull ActionFunction<C> forwardFn,
            @NonNull RollbackFunction<C> rollbackFn
    ) {
        return new SagaCommand<>() {
            @Override
            public @NonNull C execute(@NonNull C context) throws Exception {
                return forwardFn.execute(context);
            }

            @Override
            public @NonNull C undo(@NonNull C context) {
                return rollbackFn.undo(context);
            }
        };
    }

    @FunctionalInterface
    interface ActionFunction<C extends StateMachineContext> {
        @NonNull C execute(@NonNull C context) throws Exception;
    }

    @FunctionalInterface
    interface RollbackFunction<C extends StateMachineContext> {
        @NonNull C undo(@NonNull C context);
    }
}
