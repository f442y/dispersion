package com.github.f442y.dispersion.atomic;

import com.github.f442y.dispersion.AbstractStateMachineCallable;
import com.github.f442y.dispersion.config.StateMachineConfiguration;
import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.exception.ActionException;
import com.github.f442y.dispersion.exception.BackpressureException;
import com.github.f442y.dispersion.exception.MaxStateVisitsExceededException;
import com.github.f442y.dispersion.exception.MaxTransitionsExceededException;
import com.github.f442y.dispersion.exception.StateMachineException;
import com.github.f442y.dispersion.exception.TransitionException;
import com.github.f442y.dispersion.state.State;
import com.github.f442y.dispersion.state.StateKey;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.UUID;

/**
 * Encapsulates the runtime execution loop of an Atomic (Micro / Thread-Bound) Finite State Machine.
 * Executes on the caller's virtual thread with strict runtime adjacency validation and loop safeguards.
 *
 * @param <CONTEXT>   The concrete type of {@link StateMachineContext} managed by the state machine
 * @param <STATE_KEY> The enum type representing state identifiers in the state machine
 * @param <INPUT>     The type of input payload accepted by the state machine
 * @param <OUTPUT>    The type of output result produced upon state machine completion
 */
public class AtomicStateMachineCallable<
        CONTEXT extends StateMachineContext,
        STATE_KEY extends Enum<STATE_KEY> & StateKey,
        INPUT,
        OUTPUT>
        extends AbstractStateMachineCallable<CONTEXT, STATE_KEY, INPUT, OUTPUT> {

    private static final Logger log = LoggerFactory.getLogger(AtomicStateMachineCallable.class);

    @Nullable
    protected final Runnable onReleasePermit;

    protected AtomicStateMachineCallable(
            @NonNull UUID uuid,
            @NonNull StateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> configuration,
            @Nullable INPUT input,
            @Nullable CONTEXT initialContext,
            @Nullable Runnable onReleasePermit
    ) {
        super(uuid, configuration, input, initialContext);
        this.onReleasePermit = onReleasePermit;
    }

    public AtomicStateMachineCallable(
            @NonNull AtomicStateMachineCallableBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> builder
    ) {
        super(
                builder.uuid != null ? builder.uuid : UUID.randomUUID(),
                Objects.requireNonNull(builder.stateMachineConfiguration, "stateMachineConfiguration must not be null"),
                builder.input,
                builder.initialContext
        );
        this.onReleasePermit = builder.onReleasePermit;
    }

    @NonNull
    @Override
    protected CONTEXT executeState(
            @NonNull STATE_KEY stateKey,
            @NonNull State<CONTEXT, STATE_KEY> state,
            @NonNull CONTEXT context
    ) throws Exception {
        try {
            return state.action().execute(context);
        } catch (Exception e) {
            throw new ActionException(e);
        }
    }

    @Nullable
    @Override
    protected OUTPUT handleExecutionFailure(
            @NonNull STATE_KEY currentStateKey,
            @NonNull CONTEXT context,
            @NonNull Throwable failure
    ) throws Exception {
        if (failure instanceof StateMachineException sme) {
            configuration.stateMachineExceptionTrigger(this, sme);
            switch (sme) {
                case ActionException actionException ->
                        log.error("Action exception in state '{}': {}", currentStateKey, actionException.getMessage(), actionException);
                case TransitionException transitionException ->
                        log.error("Transition exception in state '{}': {}", currentStateKey, transitionException.getMessage(), transitionException);
                case BackpressureException backpressureException ->
                        log.error("Backpressure exception in state '{}': {}", currentStateKey, backpressureException.getMessage(), backpressureException);
                case MaxTransitionsExceededException maxTransitionsExceededException ->
                        log.error("Loop circuit breaker triggered in state '{}': {}", currentStateKey, maxTransitionsExceededException.getMessage(), maxTransitionsExceededException);
                case MaxStateVisitsExceededException maxStateVisitsExceededException ->
                        log.error("State visit limit exceeded in state '{}': {}", currentStateKey, maxStateVisitsExceededException.getMessage(), maxStateVisitsExceededException);
            }
            throw sme;
        } else if (failure instanceof Exception ex) {
            throw ex;
        } else {
            throw new RuntimeException("State machine execution failed in state " + currentStateKey, failure);
        }
    }

    @Override
    protected void cleanup() {
        if (onReleasePermit != null) {
            try {
                onReleasePermit.run();
            } catch (Exception e) {
                log.warn("Error executing permit release callback: {}", e.getMessage(), e);
            }
        }
    }

    /**
     * Builder for creating configured {@link AtomicStateMachineCallable} instances.
     *
     * @param <CONTEXT>   The concrete type of {@link StateMachineContext}
     * @param <STATE_KEY> The enum type representing state identifiers
     * @param <INPUT>     The input type
     * @param <OUTPUT>    The output type
     */
    public static class AtomicStateMachineCallableBuilder<
            CONTEXT extends StateMachineContext,
            STATE_KEY extends Enum<STATE_KEY> & StateKey,
            INPUT,
            OUTPUT> {
        public UUID uuid;
        public StateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> stateMachineConfiguration;
        public INPUT input;
        public CONTEXT initialContext;
        public Runnable onReleasePermit;

        /**
         * Sets an optional callback executed in the finally block upon completion to release admission permits.
         *
         * @param onReleasePermit The permit release callback
         * @return This builder instance for chaining
         */
        @NonNull
        public AtomicStateMachineCallableBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> onReleasePermit(@Nullable Runnable onReleasePermit) {
            this.onReleasePermit = onReleasePermit;
            return this;
        }

        /**
         * Sets the input payload passed to the state machine's input function.
         *
         * @param input The input payload
         * @return This builder instance for chaining
         */
        @NonNull
        public AtomicStateMachineCallableBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> input(@Nullable INPUT input) {
            this.input = input;
            return this;
        }

        /**
         * Sets an explicit pre-populated context instance instead of creating a fresh one via the context factory.
         *
         * @param initialContext The pre-populated initial context
         * @return This builder instance for chaining
         */
        @NonNull
        public AtomicStateMachineCallableBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> initialContext(@Nullable CONTEXT initialContext) {
            this.initialContext = initialContext;
            return this;
        }

        /**
         * Attaches the state machine configuration and builds the {@link AtomicStateMachineCallable}.
         *
         * @param stateMachineConfiguration The configuration containing the state map and triggers
         * @return A new {@link AtomicStateMachineCallable} instance
         */
        @NonNull
        public AtomicStateMachineCallable<CONTEXT, STATE_KEY, INPUT, OUTPUT> stateMachine(
                @NonNull StateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> stateMachineConfiguration
        ) {
            this.stateMachineConfiguration = Objects.requireNonNull(stateMachineConfiguration, "stateMachineConfiguration must not be null");
            this.uuid = UUID.randomUUID();
            return new AtomicStateMachineCallable<>(this);
        }
    }
}
