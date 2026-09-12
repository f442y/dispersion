package com.github.f442y.dispersion.fsm;

import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.fsm.state.StateKey;
import org.jspecify.annotations.NonNull;

import java.util.UUID;

/**
 * Universal contract representing an active, uniquely identifiable Finite State Machine instance.
 *
 * @param <CONTEXT>   The concrete type of {@link StateMachineContext} managed by the state machine
 * @param <STATE_KEY> The enum type representing state identifiers in the state machine
 */
public interface StateMachine<CONTEXT extends StateMachineContext, STATE_KEY extends Enum<STATE_KEY> & StateKey> {

    /**
     * Unique identifier representing this running state machine instance.
     *
     * @return The unique {@link UUID} of this state machine execution
     */
    @NonNull
    UUID uuid();
}
