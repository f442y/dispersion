package com.github.f442y.dispersion.builder;

import com.github.f442y.dispersion.config.InputFunction;
import com.github.f442y.dispersion.config.OutputFunction;
import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.context.StateMachineContextFactory;
import com.github.f442y.dispersion.state.State;
import com.github.f442y.dispersion.state.StateKey;
import org.jspecify.annotations.NonNull;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Abstract fluent builder providing common configuration options across Atomic and Orchestration State Machines.
 *
 * @param <CONTEXT>   The concrete type of {@link StateMachineContext}
 * @param <STATE_KEY> The state identifier enum type
 * @param <INPUT>     The input payload type
 * @param <OUTPUT>    The output return type
 * @param <SELF>      The recursive builder subtype for method chaining
 */
public abstract class AbstractStateMachineBuilder<
        CONTEXT extends StateMachineContext,
        STATE_KEY extends Enum<STATE_KEY> & StateKey,
        INPUT,
        OUTPUT,
        SELF extends AbstractStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT, SELF>> {

    protected final Class<STATE_KEY> stateKeyClass;
    protected final Map<STATE_KEY, State<CONTEXT, STATE_KEY>> states;
    protected STATE_KEY initialState;
    protected final Set<STATE_KEY> endStates;
    protected int maxTransitions = -1;
    protected StateMachineContextFactory<CONTEXT> contextFactory;
    protected InputFunction<CONTEXT, INPUT> inputFunction;
    protected OutputFunction<CONTEXT, OUTPUT> outputFunction;
    protected BiConsumer<CONTEXT, Throwable> exceptionTrigger;
    protected Consumer<CONTEXT> finishTrigger;

    protected AbstractStateMachineBuilder(@NonNull Class<STATE_KEY> stateKeyClass) {
        this.stateKeyClass = Objects.requireNonNull(stateKeyClass, "stateKeyClass must not be null");
        this.states = new EnumMap<>(stateKeyClass);
        this.endStates = EnumSet.noneOf(stateKeyClass);
    }

    @SuppressWarnings("unchecked")
    protected SELF self() {
        return (SELF) this;
    }

    @NonNull
    public SELF context(@NonNull StateMachineContextFactory<CONTEXT> factory) {
        this.contextFactory = Objects.requireNonNull(factory, "factory must not be null");
        return self();
    }

    @NonNull
    public SELF initialState(@NonNull STATE_KEY initialState) {
        this.initialState = Objects.requireNonNull(initialState, "initialState must not be null");
        return self();
    }

    @NonNull
    public SELF endState(@NonNull STATE_KEY endState) {
        Objects.requireNonNull(endState, "endState must not be null");
        this.endStates.add(endState);
        return self();
    }

    @NonNull
    @SafeVarargs
    public final SELF endStates(@NonNull STATE_KEY... endStates) {
        for (STATE_KEY s : endStates) {
            endState(s);
        }
        return self();
    }

    @NonNull
    public SELF endStates(@NonNull Set<STATE_KEY> endStates) {
        Objects.requireNonNull(endStates, "endStates must not be null");
        this.endStates.addAll(endStates);
        return self();
    }

    @NonNull
    public SELF maxTransitions(int maxTransitions) {
        this.maxTransitions = maxTransitions;
        return self();
    }

    @NonNull
    public SELF input(@NonNull InputFunction<CONTEXT, INPUT> inputFunction) {
        this.inputFunction = Objects.requireNonNull(inputFunction, "inputFunction must not be null");
        return self();
    }

    @NonNull
    public SELF output(@NonNull OutputFunction<CONTEXT, OUTPUT> outputFunction) {
        this.outputFunction = Objects.requireNonNull(outputFunction, "outputFunction must not be null");
        return self();
    }

    @NonNull
    public SELF onException(@NonNull BiConsumer<CONTEXT, Throwable> exceptionTrigger) {
        this.exceptionTrigger = Objects.requireNonNull(exceptionTrigger, "exceptionTrigger must not be null");
        return self();
    }

    @NonNull
    public SELF onFinish(@NonNull Consumer<CONTEXT> finishTrigger) {
        this.finishTrigger = Objects.requireNonNull(finishTrigger, "finishTrigger must not be null");
        return self();
    }

    @NonNull
    public SELF addState(@NonNull STATE_KEY stateKey, @NonNull State<CONTEXT, STATE_KEY> state) {
        Objects.requireNonNull(stateKey, "stateKey must not be null");
        Objects.requireNonNull(state, "state must not be null");
        this.states.put(stateKey, state);
        return self();
    }
}
