package com.github.f442y.dispersion.orchestration.command;

import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import org.jspecify.annotations.NonNull;

/**
 * Reversible command combining both forward business execution ({@link #execute(StateMachineContext)})
 * and compensating rollback logic ({@link #compensate(StateMachineContext)}) within a single self-contained unit.
 *
 * @param <CONTEXT> The context type
 */
public interface SagaCommand<CONTEXT extends StateMachineContext> {

    /**
     * Executes the forward business logic of this command.
     *
     * @param context The current state machine context
     * @return The updated context
     * @throws Exception If execution fails
     */
    @NonNull
    CONTEXT execute(@NonNull CONTEXT context) throws Exception;

    /**
     * Executes compensating rollback logic to undo the forward execution of this command.
     *
     * @param context The current state machine context
     * @return The compensated context
     * @throws Exception If compensation fails
     */
    @NonNull
    CONTEXT compensate(@NonNull CONTEXT context) throws Exception;
}
