package com.github.f442y.dispersion.event.dispatcher;

import com.github.f442y.dispersion.event.EventStream;
import com.github.f442y.dispersion.event.ExecutionEvent;
import com.github.f442y.dispersion.event.OverflowPolicy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("AsyncExecutionEventDispatcher Tests")
class AsyncExecutionEventDispatcherTests {

    @Test
    @DisplayName("Should asynchronously deliver events to registered listeners")
    void testAsyncEventDelivery() throws Exception {
        CountDownLatch latch = new CountDownLatch(3);
        List<ExecutionEvent> received = Collections.synchronizedList(new ArrayList<>());

        try (AsyncExecutionEventDispatcher dispatcher = AsyncExecutionEventDispatcher.builder()
                .capacity(100)
                .overflowPolicy(OverflowPolicy.DROP_OLDEST)
                .build()) {

            dispatcher.addListener(event -> {
                received.add(event);
                latch.countDown();
            });

            UUID machineId = UUID.randomUUID();
            dispatcher.onEvent(new ExecutionEvent.TurnStartedEvent(machineId, "OrderFlow", "KEY-1", Instant.now()));
            dispatcher.onEvent(new ExecutionEvent.StateEnteredEvent(machineId, "OrderFlow", "VALIDATE", Instant.now()));
            dispatcher.onEvent(new ExecutionEvent.TurnCompletedEvent(machineId, "OrderFlow", "COMPLETED", "KEY-1", Duration.ofMillis(12), Instant.now()));

            boolean completed = latch.await(3, TimeUnit.SECONDS);
            assertTrue(completed, "Events should be dispatched asynchronously to the listener");
            assertEquals(3, received.size());
            assertInstanceOf(ExecutionEvent.TurnStartedEvent.class, received.get(0));
            assertInstanceOf(ExecutionEvent.StateEnteredEvent.class, received.get(1));
            assertInstanceOf(ExecutionEvent.TurnCompletedEvent.class, received.get(2));
            assertEquals(3, dispatcher.publishedCount());
            assertEquals(0, dispatcher.droppedCount());
        }
    }

    @Test
    @DisplayName("Should pull events via EventStream on dedicated Virtual Thread")
    void testEventStreamVirtualThreadConsumption() throws Exception {
        CountDownLatch receivedLatch = new CountDownLatch(2);
        List<ExecutionEvent> streamEvents = Collections.synchronizedList(new ArrayList<>());

        try (AsyncExecutionEventDispatcher dispatcher = new AsyncExecutionEventDispatcher(50, OverflowPolicy.DROP_OLDEST)) {
            EventStream stream = dispatcher.openStream();

            Thread reader = Thread.ofVirtual().start(() -> {
                try {
                    while (!stream.isClosed()) {
                        ExecutionEvent item = stream.poll(Duration.ofSeconds(2));
                        if (item != null) {
                            streamEvents.add(item);
                            receivedLatch.countDown();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            UUID mId = UUID.randomUUID();
            dispatcher.onEvent(new ExecutionEvent.TurnStartedEvent(mId, "FlowMachine", "CORR-99", Instant.now()));
            dispatcher.onEvent(new ExecutionEvent.StateEnteredEvent(mId, "FlowMachine", "STEP_1", Instant.now()));

            boolean receivedInTime = receivedLatch.await(3, TimeUnit.SECONDS);
            assertTrue(receivedInTime, "Stream should receive 2 items");
            assertEquals(2, streamEvents.size());

            stream.close();
            reader.join(Duration.ofSeconds(1).toMillis());
            assertTrue(stream.isClosed());
        }
    }

    @Test
    @DisplayName("Should drop oldest events when buffer overflows under DROP_OLDEST policy")
    void testOverflowDropOldest() throws Exception {
        // Buffer capacity of 2
        AsyncExecutionEventDispatcher dispatcher = new AsyncExecutionEventDispatcher(2, OverflowPolicy.DROP_OLDEST);

        UUID mId = UUID.randomUUID();
        // Fire 5 events rapidly without giving worker time to drain
        for (int i = 1; i <= 5; i++) {
            dispatcher.onEvent(new ExecutionEvent.StateEnteredEvent(mId, "BufferTest", "STATE_" + i, Instant.now()));
        }

        // Wait slightly for drain
        Thread.sleep(150);
        assertTrue(dispatcher.droppedCount() > 0, "Events should have been dropped due to buffer capacity overflow");
        assertTrue(dispatcher.publishedCount() > 0);

        dispatcher.close();
    }

    @Test
    @DisplayName("Should gracefully handle listener exceptions without stopping dispatcher")
    void testListenerExceptionResilience() throws Exception {
        CountDownLatch latch = new CountDownLatch(2);
        AtomicBoolean secondListenerFired = new AtomicBoolean(false);

        try (AsyncExecutionEventDispatcher dispatcher = AsyncExecutionEventDispatcher.create()) {
            // Faulty listener
            dispatcher.addListener(event -> {
                throw new RuntimeException("Faulty listener intentional error");
            });

            // Healthy listener
            dispatcher.addListener(event -> {
                secondListenerFired.set(true);
                latch.countDown();
            });

            UUID mId = UUID.randomUUID();
            dispatcher.onEvent(new ExecutionEvent.TurnStartedEvent(mId, "ResilientMachine", null, Instant.now()));
            dispatcher.onEvent(new ExecutionEvent.TurnCompletedEvent(mId, "ResilientMachine", "DONE", null, Duration.ZERO, Instant.now()));

            boolean done = latch.await(3, TimeUnit.SECONDS);
            assertTrue(done, "Healthy listener should still receive events despite faulty listener throwing");
            assertTrue(secondListenerFired.get());
        }
    }
}
