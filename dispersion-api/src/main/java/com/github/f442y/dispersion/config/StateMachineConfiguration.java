package com.github.f442y.dispersion.config;

import com.github.f442y.dispersion.StateMachine;
import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.context.StateMachineContextFactory;
import com.github.f442y.dispersion.exception.StateMachineException;
import com.github.f442y.dispersion.state.StateKey;
import com.github.f442y.dispersion.state.StateMap;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.Optional;

/**
 * Base configuration model encapsulating state maps, context factories, transformation functions,
 * lifecycle event callbacks, and global loop safeguards for a state machine definition.
 *
 * @param <CONTEXT>   The concrete type of {@link StateMachineContext} managed by the state machine
 * @param <STATE_KEY> The enum type representing state identifiers in the state machine
 * @param <INPUT>     The type of input payload accepted by the state machine
 * @param <OUTPUT>    The type of output result produced upon state machine completion
 */
public abstract class StateMachineConfiguration<
        CONTEXT extends StateMachineContext,
        STATE_KEY extends Enum<STATE_KEY> & StateKey,
        INPUT,
        OUTPUT> {

    public static final int DEFAULT_MAX_TRANSITIONS = 10_000;

    @NonNull
    private final StateMap<CONTEXT, STATE_KEY> stateMap;
    private final int maxTransitions;

    /**
     * Constructs a configuration with the specified state map and default maximum transition limit.
     *
     * @param stateMap The state map containing states and transition routing
     */
    public StateMachineConfiguration(@NonNull StateMap<CONTEXT, STATE_KEY> stateMap) {
        this(stateMap, DEFAULT_MAX_TRANSITIONS);
    }

    /**
     * Constructs a configuration with the specified state map and custom maximum transition limit.
     *
     * @param stateMap       The state map containing states and transition routing
     * @param maxTransitions Maximum transitions allowed in a single execution run
     */
    public StateMachineConfiguration(@NonNull StateMap<CONTEXT, STATE_KEY> stateMap, int maxTransitions) {
        this.stateMap = Objects.requireNonNull(stateMap, "stateMap must not be null");
        this.maxTransitions = (maxTransitions > 0) ? maxTransitions : DEFAULT_MAX_TRANSITIONS;
    }

    /**
     * Returns the configured {@link StateMap} instance.
     *
     * @return The state map
     */
    @NonNull
    public StateMap<CONTEXT, STATE_KEY> getStateMap() {
        return stateMap;
    }

    /**
     * Returns the global maximum transition limit before execution is terminated with an exception.
     *
     * @return The maximum transition count
     */
    public int getMaxTransitions() {
        return maxTransitions;
    }

    /**
     * Factory for generating a fresh {@link StateMachineContext} instance for each execution.
     *
     * @return The context factory
     */
    @NonNull
    public abstract StateMachineContextFactory<CONTEXT> stateMachineContextFactory();

    /**
     * Optional input transformation function applied before state execution begins.
     *
     * @param stateMachine The active state machine instance
     * @return An {@link Optional} containing the {@link InputFunction}, or empty if unconfigured
     * @throws StateMachineException If resolution fails
     */
    public Optional<InputFunction<CONTEXT, INPUT>> inputFunctionTrigger(
            @NonNull StateMachine<CONTEXT, STATE_KEY> stateMachine
    ) throws StateMachineException {
        return Optional.ofNullable(inputFunction());
    }

    @Nullable
    public abstract InputFunction<CONTEXT, INPUT> inputFunction() throws StateMachineException;

    /**
     * Optional output transformation function extracting the final result from context upon completion.
     *
     * @param stateMachine The active state machine instance
     * @return An {@link Optional} containing the {@link OutputFunction}, or empty if unconfigured
     * @throws StateMachineException If resolution fails
     */
    public Optional<OutputFunction<CONTEXT, OUTPUT>> outputFunctionTrigger(
            @NonNull StateMachine<CONTEXT, STATE_KEY> stateMachine
    ) throws StateMachineException {
        return Optional.ofNullable(outputFunction());
    }

    @Nullable
    public abstract OutputFunction<CONTEXT, OUTPUT> outputFunction() throws StateMachineException;

    /**
     * Context factory trigger for obtaining a context instance for an active state machine execution.
     *
     * @param stateMachine The executing state machine instance
     * @return The context factory
     */
    @NonNull
    public StateMachineContextFactory<CONTEXT> stateMachineContextFactoryTrigger(
            @NonNull StateMachine<CONTEXT, STATE_KEY> stateMachine
    ) {
        return stateMachineContextFactory();
    }

    /**
     * Lifecycle callback invoked when an unhandled {@link StateMachineException} occurs.
     *
     * @param stateMachine          The active state machine instance
     * @param stateMachineException The exception that was thrown
     */
    public void stateMachineExceptionTrigger(
            @NonNull StateMachine<CONTEXT, STATE_KEY> stateMachine,
            @NonNull StateMachineException stateMachineException
    ) {
        // Default hook is a no-op; subclasses or listeners can override
    }

    /**
     * Lifecycle callback invoked upon state machine completion or cancellation.
     *
     * @param stateMachine The completed state machine instance
     */
    public void stateMachineFinishTrigger(
            @NonNull StateMachine<CONTEXT, STATE_KEY> stateMachine
    ) {
        // Default hook is a no-op; subclasses or listeners can override
    }
}
