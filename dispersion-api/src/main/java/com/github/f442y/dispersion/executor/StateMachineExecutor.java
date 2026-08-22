package com.github.f442y.dispersion.executor;

import com.github.f442y.dispersion.StateMachineFuture;
import com.github.f442y.dispersion.context.StateMachineContext;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Universal execution contract for Finite State Machines, supporting both synchronous blocking
 * and non-blocking asynchronous dispatch on Java Virtual Threads.
 *
 * @param <CONTEXT> The concrete type of {@link StateMachineContext} managed by the state machine
 * @param <INPUT>   The input payload accepted by the state machine
 * @param <OUTPUT>  The return result produced by the state machine
 */
public interface StateMachineExecutor<CONTEXT extends StateMachineContext, INPUT, OUTPUT> extends AutoCloseable {

    /**
     * Synchronously dispatches a state machine execution with a new context instance and the given input.
     *
     * @param input The external input payload
     * @return The final output result produced by the state machine
     * @throws Exception If an unhandled execution error or backpressure exception occurs
     */
    @Nullable
    OUTPUT dispatchSync(@Nullable INPUT input) throws Exception;

    /**
     * Synchronously dispatches a state machine execution using an explicit initial context and input.
     *
     * @param initialContext The initial context instance to execute with
     * @param input          The external input payload
     * @return The final output result produced by the state machine
     * @throws Exception If an unhandled execution error or backpressure exception occurs
     */
    @Nullable
    OUTPUT dispatchSync(@NonNull CONTEXT initialContext, @Nullable INPUT input) throws Exception;

    /**
     * Asynchronously dispatches a state machine execution on a dedicated Virtual Thread.
     *
     * @param input The external input payload
     * @return A {@link StateMachineFuture} handle wrapping execution and providing cancellation and future resolution
     */
    @NonNull
    StateMachineFuture<OUTPUT> dispatchAsync(@Nullable INPUT input);

    /**
     * Asynchronously dispatches a state machine execution on a dedicated Virtual Thread with explicit context.
     *
     * @param initialContext The initial context instance to execute with
     * @param input          The external input payload
     * @return A {@link StateMachineFuture} handle wrapping execution and providing cancellation and future resolution
     */
    @NonNull
    StateMachineFuture<OUTPUT> dispatchAsync(@NonNull CONTEXT initialContext, @Nullable INPUT input);

    @Override
    void close();
}
