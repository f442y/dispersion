package com.github.f442y.dispersion;

import com.github.f442y.dispersion.atomic.AtomicStateMachineCallable;
import com.github.f442y.dispersion.config.StateMachineConfiguration;
import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.state.StateKey;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.UUID;

/**
 * Standard alias for {@link AtomicStateMachineCallable}.
 *
 * @param <CONTEXT>   The concrete type of {@link StateMachineContext} managed by the state machine
 * @param <STATE_KEY> The enum type representing state identifiers in the state machine
 * @param <INPUT>     The type of input payload accepted by the state machine
 * @param <OUTPUT>    The type of output result produced upon state machine completion
 */
public final class StateMachineCallable<
        CONTEXT extends StateMachineContext,
        STATE_KEY extends Enum<STATE_KEY> & StateKey,
        INPUT,
        OUTPUT>
        extends AtomicStateMachineCallable<CONTEXT, STATE_KEY, INPUT, OUTPUT> {

    private StateMachineCallable(
            @NonNull StateMachineCallableBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> builder
    ) {
        super(
                builder.uuid != null ? builder.uuid : UUID.randomUUID(),
                Objects.requireNonNull(builder.stateMachineConfiguration, "stateMachineConfiguration must not be null"),
                builder.input,
                builder.initialContext,
                builder.onReleasePermit
        );
    }

    /**
     * Builder for creating configured {@link StateMachineCallable} instances.
     *
     * @param <CONTEXT>   The concrete type of {@link StateMachineContext}
     * @param <STATE_KEY> The enum type representing state identifiers
     * @param <INPUT>     The input type
     * @param <OUTPUT>    The output type
     */
    public static class StateMachineCallableBuilder<
            CONTEXT extends StateMachineContext,
            STATE_KEY extends Enum<STATE_KEY> & StateKey,
            INPUT,
            OUTPUT>
            extends AtomicStateMachineCallableBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> {

        @NonNull
        @Override
        public StateMachineCallableBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> onReleasePermit(@Nullable Runnable onReleasePermit) {
            super.onReleasePermit(onReleasePermit);
            return this;
        }

        @NonNull
        @Override
        public StateMachineCallableBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> input(@Nullable INPUT input) {
            super.input(input);
            return this;
        }

        @NonNull
        @Override
        public StateMachineCallableBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> initialContext(@Nullable CONTEXT initialContext) {
            super.initialContext(initialContext);
            return this;
        }

        @NonNull
        @Override
        public StateMachineCallable<CONTEXT, STATE_KEY, INPUT, OUTPUT> stateMachine(
                @NonNull StateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> stateMachineConfiguration
        ) {
            this.stateMachineConfiguration = Objects.requireNonNull(stateMachineConfiguration, "stateMachineConfiguration must not be null");
            this.uuid = UUID.randomUUID();
            return new StateMachineCallable<>(this);
        }
    }
}
