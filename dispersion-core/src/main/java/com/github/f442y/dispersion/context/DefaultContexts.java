package com.github.f442y.dispersion.context;

import org.jspecify.annotations.NonNull;

/**
 * Standard utility context implementations and factories.
 */
public final class DefaultContexts {

    private DefaultContexts() {}

    /**
     * Stateless empty context for state machines that do not require state persistence.
     */
    public record EmptyContext() implements StateMachineContext {}

    private static final EmptyContext EMPTY = new EmptyContext();

    @NonNull
    public static EmptyContext empty() {
        return EMPTY;
    }

    @NonNull
    public static StateMachineContextFactory<EmptyContext> emptyFactory() {
        return () -> EMPTY;
    }
}
