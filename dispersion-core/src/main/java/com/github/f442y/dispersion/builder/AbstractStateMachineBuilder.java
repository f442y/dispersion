package com.github.f442y.dispersion.builder;

import com.github.f442y.dispersion.config.InputFunction;
import com.github.f442y.dispersion.config.OutputFunction;
import com.github.f442y.dispersion.config.StateMachineConfiguration;
import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.context.StateMachineContextFactory;
import com.github.f442y.dispersion.state.StateKey;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Abstract base builder defining common configuration properties and fluent methods
 * shared across both Atomic (micro) and Orchestration (macro) State Machine builders.
 *
 * @param <CONTEXT>   The concrete type of {@link StateMachineContext} managed by the state machine
 * @param <STATE_KEY> The enum type representing state identifiers in the state machine
 * @param <INPUT>     The type of input payload accepted by the state machine
 * @param <OUTPUT>    The type of output result produced upon state machine completion
 * @param <SELF>      The recursive self-type for fluent builder chaining
 */
public abstract class AbstractStateMachineBuilder<
        CONTEXT extends StateMachineContext,
        STATE_KEY extends Enum<STATE_KEY> & StateKey,
        INPUT,
        OUTPUT,
        SELF extends AbstractStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT, SELF>> {

    @NonNull
    protected final Class<STATE_KEY> stateKeyClass;

    protected StateMachineContextFactory<CONTEXT> contextFactory;
    protected STATE_KEY initialStateKey;
    protected final Set<STATE_KEY> endStateKeys = new HashSet<>();
    protected InputFunction<CONTEXT, INPUT> inputFunction;
    protected OutputFunction<CONTEXT, OUTPUT> outputFunction;
    protected int maxTransitions = StateMachineConfiguration.DEFAULT_MAX_TRANSITIONS;

    protected AbstractStateMachineBuilder(@NonNull Class<STATE_KEY> stateKeyClass) {
        this.stateKeyClass = Objects.requireNonNull(stateKeyClass, "stateKeyClass must not be null");
    }

    @SuppressWarnings("unchecked")
    @NonNull
    protected SELF self() {
        return (SELF) this;
    }

    /**
     * Configures the supplier used to instantiate a fresh {@link StateMachineContext} for each execution.
     *
     * @param contextSupplier A lambda supplier returning a new context instance
     * @return This builder instance for chaining
     */
    @NonNull
    public SELF context(@NonNull Supplier<CONTEXT> contextSupplier) {
        Objects.requireNonNull(contextSupplier, "context supplier must not be null");
        this.contextFactory = contextSupplier::get;
        return self();
    }

    /**
     * Configures the factory used to instantiate a fresh {@link StateMachineContext}.
     *
     * @param contextFactory The factory instance
     * @return This builder instance for chaining
     */
    @NonNull
    public SELF contextFactory(@NonNull StateMachineContextFactory<CONTEXT> contextFactory) {
        this.contextFactory = Objects.requireNonNull(contextFactory, "contextFactory must not be null");
        return self();
    }

    /**
     * Sets the entry-point state key for the state machine.
     *
     * @param initialState The initial state key
     * @return This builder instance for chaining
     */
    @NonNull
    public SELF initialState(@NonNull STATE_KEY initialState) {
        this.initialStateKey = Objects.requireNonNull(initialState, "initialState must not be null");
        return self();
    }

    /**
     * Configures a global maximum transition circuit breaker to prevent infinite loops.
     *
     * @param maxTransitions Maximum transitions allowed in a single execution run
     * @return This builder instance for chaining
     */
    @NonNull
    public SELF maxTransitions(int maxTransitions) {
        this.maxTransitions = maxTransitions;
        return self();
    }

    /**
     * Declares one or more terminal end states that conclude the state machine lifecycle.
     *
     * @param endStates The state keys representing terminal endpoints
     * @return This builder instance for chaining
     */
    @NonNull
    @SafeVarargs
    public final SELF endStates(@NonNull STATE_KEY... endStates) {
        if (endStates != null) {
            Collections.addAll(this.endStateKeys, endStates);
        }
        return self();
    }

    /**
     * Configures the input transformation function applied before state execution begins.
     *
     * @param inputFunction Function merging input payload into the initial context
     * @return This builder instance for chaining
     */
    @NonNull
    public SELF input(@Nullable BiFunction<CONTEXT, INPUT, CONTEXT> inputFunction) {
        if (inputFunction != null) {
            this.inputFunction = inputFunction::apply;
        }
        return self();
    }

    /**
     * Configures the output transformation function extracting the final result from context.
     *
     * @param outputFunction Function extracting the final output from context upon completion
     * @return This builder instance for chaining
     */
    @NonNull
    public SELF output(@Nullable Function<CONTEXT, OUTPUT> outputFunction) {
        if (outputFunction != null) {
            this.outputFunction = outputFunction::apply;
        }
        return self();
    }
}
