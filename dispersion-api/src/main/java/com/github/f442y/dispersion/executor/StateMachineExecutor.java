package com.github.f442y.dispersion.executor;

import com.github.f442y.dispersion.context.StateMachineContext;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.concurrent.Future;

/**
 * Universal execution contract for both Atomic (Micro) and Orchestration (Macro) State Machines.
 *
 * @param <CONTEXT> The state machine context type
 * @param <INPUT>   The input payload type
 * @param <OUTPUT>  The output result type
 */
public interface StateMachineExecutor<CONTEXT extends StateMachineContext, INPUT, OUTPUT> extends AutoCloseable {

    /**
     * Executes the state machine synchronously, blocking the caller until completion.
     *
     * @param input The input payload
     * @return The final output result
     * @throws Exception If state machine execution fails
     */
    @Nullable
    OUTPUT dispatchSync(@Nullable INPUT input) throws Exception;

    /**
     * Executes the state machine synchronously with a pre-populated initial context.
     *
     * @param initialContext Pre-populated context
     * @param input          The input payload
     * @return The final output result
     * @throws Exception If state machine execution fails
     */
    @Nullable
    OUTPUT dispatchSync(@Nullable CONTEXT initialContext, @Nullable INPUT input) throws Exception;

    /**
     * Dispatches the state machine asynchronously on a dedicated Virtual Thread.
     *
     * @param input The input payload
     * @return A {@link Future} tracking execution completion
     * @throws Exception If dispatch admission is rejected
     */
    @NonNull
    Future<OUTPUT> dispatchAsync(@Nullable INPUT input) throws Exception;

    /**
     * Dispatches the state machine asynchronously with a pre-populated initial context.
     *
     * @param initialContext Pre-populated context
     * @param input          The input payload
     * @return A {@link Future} tracking execution completion
     * @throws Exception If dispatch admission is rejected
     */
    @NonNull
    Future<OUTPUT> dispatchAsync(@Nullable CONTEXT initialContext, @Nullable INPUT input) throws Exception;

    @Override
    default void close() {}
}
