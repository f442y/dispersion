package com.github.f442y.dispersion.context;

import org.jspecify.annotations.NonNull;

/**
 * Utility containing default static context implementations and factories.
 */
public final class DefaultContexts {

    public static final EmptyStaticContextFactory EMPTY_STATIC_CONTEXT_FACTORY = new EmptyStaticContextFactory();
    private static final EmptyStaticContext EMPTY_STATIC_CONTEXT = new EmptyStaticContext();

    private DefaultContexts() {
        // no-op: Utility Class
    }

    /**
     * Singleton empty context instance for stateless state machines.
     */
    public record EmptyStaticContext() implements StateMachineContext {}

    /**
     * Factory providing the singleton empty context.
     */
    public record EmptyStaticContextFactory() implements StateMachineContextFactory<EmptyStaticContext> {
        @NonNull
        @Override
        public EmptyStaticContext newInstance() {
            return EMPTY_STATIC_CONTEXT;
        }
    }
}
