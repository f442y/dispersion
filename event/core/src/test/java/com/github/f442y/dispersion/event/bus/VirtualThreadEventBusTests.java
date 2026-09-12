package com.github.f442y.dispersion.event.bus;

import com.github.f442y.dispersion.event.EventBus;
import com.github.f442y.dispersion.event.EventStream;
import com.github.f442y.dispersion.event.ExecutionEvent;
import com.github.f442y.dispersion.event.Subscription;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("VirtualThreadEventBus Tests")
class VirtualThreadEventBusTests {

    @Test
    @DisplayName("Should deliver events asynchronously to general and strongly-typed subscribers")
    void testStronglyTypedAndGeneralSubscribers() throws Exception {
        CountDownLatch generalLatch = new CountDownLatch(3);
        CountDownLatch typedLatch = new CountDownLatch(1);

        List<ExecutionEvent> generalEvents = Collections.synchronizedList(new ArrayList<>());
        List<ExecutionEvent.TurnStartedEvent> startEvents = Collections.synchronizedList(new ArrayList<>());

        try (EventBus bus = VirtualThreadEventBus.builder().bufferCapacity(100).build()) {
            // General subscription
            bus.subscribe(event -> {
                generalEvents.add(event);
                generalLatch.countDown();
            });

            // Strongly-typed subscription: only TurnStartedEvent!
            bus.subscribe(ExecutionEvent.TurnStartedEvent.class, event -> {
                startEvents.add(event);
                typedLatch.countDown();
            });

            UUID machineId = UUID.randomUUID();
            bus.onEvent(new ExecutionEvent.TurnStartedEvent(machineId, "OrderFlow", "KEY-1", Instant.now()));
            bus.onEvent(new ExecutionEvent.StateEnteredEvent(machineId, "OrderFlow", "VALIDATE", Instant.now()));
            bus.onEvent(new ExecutionEvent.TurnCompletedEvent(machineId, "OrderFlow", "COMPLETED", "KEY-1", Duration.ofMillis(12), Instant.now()));

            assertTrue(generalLatch.await(3, TimeUnit.SECONDS), "General subscriber should receive all 3 events");
            assertTrue(typedLatch.await(3, TimeUnit.SECONDS), "Typed subscriber should receive only TurnStartedEvent");

            assertEquals(3, generalEvents.size());
            assertEquals(1, startEvents.size());
            assertEquals("OrderFlow", startEvents.get(0).machineName());
        }
    }

    @Test
    @DisplayName("Should pull events sequentially via EventStream on a dedicated Virtual Thread")
    void testVirtualThreadEventStreamPull() throws Exception {
        CountDownLatch streamConsumedLatch = new CountDownLatch(2);
        List<ExecutionEvent> streamedEvents = Collections.synchronizedList(new ArrayList<>());

        try (EventBus bus = VirtualThreadEventBus.create()) {
            EventStream stream = bus.openStream();

            // Spawn a virtual thread consuming from the stream using blocking take()
            Thread consumerThread = Thread.ofVirtual().start(() -> {
                try {
                    while (!stream.isClosed()) {
                        ExecutionEvent event = stream.poll(Duration.ofSeconds(2));
                        if (event != null) {
                            streamedEvents.add(event);
                            streamConsumedLatch.countDown();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            UUID mId = UUID.randomUUID();
            bus.onEvent(new ExecutionEvent.TurnStartedEvent(mId, "StreamFlow", "CORR-1", Instant.now()));
            bus.onEvent(new ExecutionEvent.StateEnteredEvent(mId, "StreamFlow", "STEP_1", Instant.now()));

            assertTrue(streamConsumedLatch.await(3, TimeUnit.SECONDS), "Stream should receive 2 events");
            assertEquals(2, streamedEvents.size());

            stream.close();
            consumerThread.join(Duration.ofSeconds(1).toMillis());
            assertTrue(stream.isClosed());
        }
    }

    @Test
    @DisplayName("Should maintain recent chronological history in circular replay buffer")
    void testCircularHistoryReplayBuffer() {
        try (EventBus bus = VirtualThreadEventBus.builder()
                .direct(true) // direct mode for instant synchronous verification
                .historyCapacity(5)
                .build()) {

            UUID mId = UUID.randomUUID();
            for (int i = 1; i <= 10; i++) {
                bus.onEvent(new ExecutionEvent.StateEnteredEvent(mId, "HistoryFlow", "STATE_" + i, Instant.now()));
            }

            List<ExecutionEvent> history = bus.history(10);
            assertEquals(5, history.size(), "History should be bounded by historyCapacity");

            // Verify oldest was evicted and newest are present (STATE_6 through STATE_10)
            ExecutionEvent.StateEnteredEvent firstInHistory = (ExecutionEvent.StateEnteredEvent) history.get(0);
            ExecutionEvent.StateEnteredEvent lastInHistory = (ExecutionEvent.StateEnteredEvent) history.get(4);
            assertEquals("STATE_6", firstInHistory.stateName());
            assertEquals("STATE_10", lastInHistory.stateName());

            // Limit sub-query
            List<ExecutionEvent> lastTwo = bus.history(2);
            assertEquals(2, lastTwo.size());
            assertEquals("STATE_9", ((ExecutionEvent.StateEnteredEvent) lastTwo.get(0)).stateName());
            assertEquals("STATE_10", ((ExecutionEvent.StateEnteredEvent) lastTwo.get(1)).stateName());
        }
    }

    @Test
    @DisplayName("Subscription handle should allow unregistering listener cleanly")
    void testSubscriptionUnsubscribe() throws Exception {
        AtomicInteger eventCount = new AtomicInteger();

        try (EventBus bus = VirtualThreadEventBus.direct()) {
            Subscription sub = bus.subscribe(_ -> eventCount.incrementAndGet());

            UUID mId = UUID.randomUUID();
            bus.onEvent(new ExecutionEvent.TurnStartedEvent(mId, "Test", null, Instant.now()));
            assertEquals(1, eventCount.get());

            sub.unsubscribe();

            bus.onEvent(new ExecutionEvent.TurnStartedEvent(mId, "Test", null, Instant.now()));
            assertEquals(1, eventCount.get(), "Listener should not receive events after unsubscribe");
        }
    }

    @Test
    @DisplayName("Direct mode executes synchronously without background threads")
    void testDirectModeSynchronousExecution() {
        AtomicBoolean received = new AtomicBoolean(false);

        try (EventBus bus = VirtualThreadEventBus.direct()) {
            bus.subscribe(_ -> received.set(true));

            bus.onEvent(new ExecutionEvent.TurnStartedEvent(UUID.randomUUID(), "DirectTest", null, Instant.now()));
            assertTrue(received.get(), "Direct mode should invoke listener synchronously");
        }
    }

    @Test
    @DisplayName("Should handle 10,000 concurrent events published across 100 virtual threads without loss or contention")
    void testConcurrentHighThroughputVirtualThreads() throws Exception {
        int threadCount = 100;
        int eventsPerThread = 100;
        int totalEvents = threadCount * eventsPerThread;

        CountDownLatch latch = new CountDownLatch(totalEvents);
        AtomicInteger receivedCount = new AtomicInteger();

        try (EventBus bus = VirtualThreadEventBus.builder()
                .bufferCapacity(totalEvents + 1000)
                .historyCapacity(100)
                .build()) {

            bus.subscribe(event -> {
                receivedCount.incrementAndGet();
                latch.countDown();
            });

            try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
                for (int t = 0; t < threadCount; t++) {
                    final int threadId = t;
                    executor.submit(() -> {
                        for (int e = 0; e < eventsPerThread; e++) {
                            bus.onEvent(new ExecutionEvent.TurnStartedEvent(
                                    UUID.randomUUID(),
                                    "StressMachine",
                                    "CORR-" + threadId + "-" + e,
                                    Instant.now()
                            ));
                        }
                    });
                }
            }

            assertTrue(latch.await(10, TimeUnit.SECONDS), "All 10,000 events must be processed within 10 seconds");
            assertEquals(totalEvents, receivedCount.get());
            assertEquals(100, bus.history(100).size());
        }
    }
}

