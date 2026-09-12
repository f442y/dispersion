package com.github.f442y.dispersion.event;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Duration;

/**
 * Pull-based, closeable event stream tailored for Java Virtual Thread consumers
 * (such as Server-Sent Events, WebSockets, or background audit workers).
 *
 * <p>Unlike reactive streams, consuming an {@link EventStream} relies on idiomatic,
 * blocking calls ({@link #take()} or {@link #poll(Duration)}) on lightweight Virtual Threads,
 * eliminating the complexity, callback chaining, and silent event drops of reactive frameworks.</p>
 */
public interface EventStream extends Iterable<ExecutionEvent>, AutoCloseable {

    /**
     * Retrieves and removes the next event, waiting up to the specified timeout on the current Virtual Thread.
     *
     * @param timeout The maximum duration to wait for an event
     * @return The next {@link ExecutionEvent}, or {@code null} if the timeout elapsed before an event was available
     * @throws InterruptedException If the calling Virtual Thread is interrupted while waiting
     */
    @Nullable
    ExecutionEvent poll(@NonNull Duration timeout) throws InterruptedException;

    /**
     * Retrieves and removes the next event, blocking the current Virtual Thread until an event becomes available.
     *
     * @return The next {@link ExecutionEvent}
     * @throws InterruptedException If the calling Virtual Thread is interrupted while waiting
     * @throws IllegalStateException If the stream was closed while waiting
     */
    @NonNull
    ExecutionEvent take() throws InterruptedException;

    /**
     * Returns {@code true} if this stream has been closed.
     *
     * @return {@code true} if closed
     */
    boolean isClosed();

    /**
     * Closes the stream and releases any allocated queue resources.
     */
    @Override
    void close();
}
