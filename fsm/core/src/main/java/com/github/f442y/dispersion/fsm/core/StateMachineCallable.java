package com.github.f442y.dispersion.fsm.core;

import com.github.f442y.dispersion.fsm.config.StateMachineConfiguration;
import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.fsm.state.StateKey;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.UUID;

/**
 * Standard implementation of {@link AbstractStateMachineCallable}.
 *
 * @param <CONTEXT>   The concrete type of {@link StateMachineContext}
 * @param <STATE_KEY> The state identifier enum type
 * @param <INPUT>     The input payload type
 * @param <OUTPUT>    The output return type
 */
public class StateMachineCallable<
        CONTEXT extends StateMachineContext,
        STATE_KEY extends Enum<STATE_KEY> & StateKey,
        INPUT,
        OUTPUT> extends AbstractStateMachineCallable<CONTEXT, STATE_KEY, INPUT, OUTPUT> {

    public StateMachineCallable(
            @Nullable UUID uuid,
            @NonNull StateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> configuration,
            @Nullable CONTEXT initialContext,
            @Nullable INPUT input
    ) {
        super(uuid, configuration, initialContext, input);
    }

    public static <
            CONTEXT extends StateMachineContext,
            STATE_KEY extends Enum<STATE_KEY> & StateKey,
            INPUT,
            OUTPUT>
    Builder<CONTEXT, STATE_KEY, INPUT, OUTPUT> builder(
            @NonNull StateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> configuration
    ) {
        return new Builder<>(configuration);
    }

    public static final class Builder<
            CONTEXT extends StateMachineContext,
            STATE_KEY extends Enum<STATE_KEY> & StateKey,
            INPUT,
            OUTPUT> {

        private final StateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> configuration;
        private UUID uuid;
        private CONTEXT initialContext;
        private INPUT input;

        private Builder(@NonNull StateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> configuration) {
            this.configuration = Objects.requireNonNull(configuration, "configuration must not be null");
        }

        @NonNull
        public Builder<CONTEXT, STATE_KEY, INPUT, OUTPUT> uuid(@Nullable UUID uuid) {
            this.uuid = uuid;
            return this;
        }

        @NonNull
        public Builder<CONTEXT, STATE_KEY, INPUT, OUTPUT> initialContext(@Nullable CONTEXT initialContext) {
            this.initialContext = initialContext;
            return this;
        }

        @NonNull
        public Builder<CONTEXT, STATE_KEY, INPUT, OUTPUT> input(@Nullable INPUT input) {
            this.input = input;
            return this;
        }

        @NonNull
        public StateMachineCallable<CONTEXT, STATE_KEY, INPUT, OUTPUT> build() {
            return new StateMachineCallable<>(uuid, configuration, initialContext, input);
        }
    }
}
