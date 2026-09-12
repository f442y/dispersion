package com.github.f442y.dispersion.fsm.config;

import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.fsm.context.StateMachineContextFactory;
import com.github.f442y.dispersion.event.ExecutionEventListener;
import com.github.f442y.dispersion.fsm.exception.StateMachineException;
import com.github.f442y.dispersion.fsm.state.StateKey;
import com.github.f442y.dispersion.fsm.state.StateMap;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Immutable configuration definition for a Finite State Machine.
 *
 * @param <CONTEXT>   The concrete type of {@link StateMachineContext} managed by the state machine
 * @param <STATE_KEY> The enum type representing state identifiers in the state machine
 * @param <INPUT>     The type of input payload accepted by this state machine
 * @param <OUTPUT>    The type of output result returned by this state machine
 */
public class StateMachineConfiguration<
        CONTEXT extends StateMachineContext,
        STATE_KEY extends Enum<STATE_KEY> & StateKey,
        INPUT,
        OUTPUT> {

    private final String machineName;
    private final StateMap<CONTEXT, STATE_KEY> stateMap;
    private final int maxTransitions;
    private final StateMachineContextFactory<CONTEXT> contextFactory;
    private final InputFunction<CONTEXT, INPUT> inputFunction;
    private final OutputFunction<CONTEXT, OUTPUT> outputFunction;
    private final BiConsumer<CONTEXT, Throwable> exceptionTrigger;
    private final Consumer<CONTEXT> finishTrigger;
    private final ExecutionEventListener eventListener;

    public StateMachineConfiguration(@NonNull StateMap<CONTEXT, STATE_KEY> stateMap) {
        this(null, stateMap, -1, null, null, null, null, null, null);
    }

    public StateMachineConfiguration(
            @NonNull StateMap<CONTEXT, STATE_KEY> stateMap,
            int maxTransitions,
            @Nullable StateMachineContextFactory<CONTEXT> contextFactory,
            @Nullable InputFunction<CONTEXT, INPUT> inputFunction,
            @Nullable OutputFunction<CONTEXT, OUTPUT> outputFunction,
            @Nullable BiConsumer<CONTEXT, Throwable> exceptionTrigger,
            @Nullable Consumer<CONTEXT> finishTrigger
    ) {
        this(null, stateMap, maxTransitions, contextFactory, inputFunction, outputFunction, exceptionTrigger, finishTrigger, null);
    }

    public StateMachineConfiguration(
            @NonNull StateMap<CONTEXT, STATE_KEY> stateMap,
            int maxTransitions,
            @Nullable StateMachineContextFactory<CONTEXT> contextFactory,
            @Nullable InputFunction<CONTEXT, INPUT> inputFunction,
            @Nullable OutputFunction<CONTEXT, OUTPUT> outputFunction,
            @Nullable BiConsumer<CONTEXT, Throwable> exceptionTrigger,
            @Nullable Consumer<CONTEXT> finishTrigger,
            @Nullable ExecutionEventListener eventListener
    ) {
        this(null, stateMap, maxTransitions, contextFactory, inputFunction, outputFunction, exceptionTrigger, finishTrigger, eventListener);
    }

    public StateMachineConfiguration(
            @Nullable String machineName,
            @NonNull StateMap<CONTEXT, STATE_KEY> stateMap,
            int maxTransitions,
            @Nullable StateMachineContextFactory<CONTEXT> contextFactory,
            @Nullable InputFunction<CONTEXT, INPUT> inputFunction,
            @Nullable OutputFunction<CONTEXT, OUTPUT> outputFunction,
            @Nullable BiConsumer<CONTEXT, Throwable> exceptionTrigger,
            @Nullable Consumer<CONTEXT> finishTrigger,
            @Nullable ExecutionEventListener eventListener
    ) {
        this.stateMap = Objects.requireNonNull(stateMap, "stateMap must not be null");
        this.machineName = (machineName != null && !machineName.isBlank())
                ? machineName
                : "atomic-" + stateMap.getStateKeyClass().getSimpleName();
        this.maxTransitions = maxTransitions;
        this.contextFactory = contextFactory;
        this.inputFunction = inputFunction;
        this.outputFunction = outputFunction;
        this.exceptionTrigger = exceptionTrigger;
        this.finishTrigger = finishTrigger;
        this.eventListener = eventListener;
    }

    @NonNull
    public String getMachineName() {
        return machineName;
    }

    @NonNull
    public StateMap<CONTEXT, STATE_KEY> getStateMap() {
        return stateMap;
    }

    public int getMaxTransitions() {
        return maxTransitions;
    }

    @Nullable
    public StateMachineContextFactory<CONTEXT> stateMachineContextFactory() {
        return contextFactory;
    }

    @Nullable
    public InputFunction<CONTEXT, INPUT> inputFunction() throws StateMachineException {
        return inputFunction;
    }

    @Nullable
    public OutputFunction<CONTEXT, OUTPUT> outputFunction() throws StateMachineException {
        return outputFunction;
    }

    @Nullable
    public BiConsumer<CONTEXT, Throwable> stateMachineExceptionTrigger() {
        return exceptionTrigger;
    }

    @Nullable
    public Consumer<CONTEXT> stateMachineFinishTrigger() {
        return finishTrigger;
    }

    @Nullable
    public ExecutionEventListener eventListener() {
        return eventListener;
    }
}
