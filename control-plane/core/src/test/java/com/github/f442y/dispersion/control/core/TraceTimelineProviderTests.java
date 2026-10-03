package com.github.f442y.dispersion.control.core;

import com.github.f442y.dispersion.control.TraceTimelineProvider;
import com.github.f442y.dispersion.event.DynamicTapManager;
import com.github.f442y.dispersion.event.EventBus;
import com.github.f442y.dispersion.event.EventBusMetrics;
import com.github.f442y.dispersion.event.EventStream;
import com.github.f442y.dispersion.event.ExecutionEvent;
import com.github.f442y.dispersion.event.ExecutionEventListener;
import com.github.f442y.dispersion.event.LocalExecutionTraceBuffer;
import com.github.f442y.dispersion.event.Subscription;
import com.github.f442y.dispersion.event.turn.TurnCompletedEvent;
import com.github.f442y.dispersion.event.turn.TurnStartedEvent;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("Trace Timeline Provider SPI & In-Memory Implementation Tests")
class TraceTimelineProviderTests {

    @Test
    @DisplayName("Should buffer events and retrieve chronological trace up to limit")
    void testInternalRingBufferTraceTimeline() {
        InMemoryTraceTimelineProvider provider = new InMemoryTraceTimelineProvider(5);
        UUID machineId = UUID.randomUUID();
        Instant now = Instant.now();

        for (int i = 0; i < 7; i++) {
            provider.record(new TurnStartedEvent(
                    machineId,
                    "OrderFSM",
                    "corr-" + i,
                    now.plusMillis(i * 10L)
            ));
        }

        // Capacity is 5, so first 2 events dropped
        List<ExecutionEvent> allEvents = provider.getTimeline(machineId, 10);
        assertEquals(5, allEvents.size());

        TurnStartedEvent firstRetained = (TurnStartedEvent) allEvents.getFirst();
        assertEquals("corr-2", firstRetained.correlationKey());

        TurnStartedEvent lastRetained = (TurnStartedEvent) allEvents.getLast();
        assertEquals("corr-6", lastRetained.correlationKey());

        // Test limit retrieval
        List<ExecutionEvent> limited = provider.getTimeline(machineId, 2);
        assertEquals(2, limited.size());
        assertEquals("corr-5", ((TurnStartedEvent) limited.get(0)).correlationKey());
        assertEquals("corr-6", ((TurnStartedEvent) limited.get(1)).correlationKey());
    }

    @Test
    @DisplayName("Should asynchronously fetch timeline via CompletableFuture")
    void testAsyncFetchTimeline() {
        InMemoryTraceTimelineProvider provider = new InMemoryTraceTimelineProvider(10);
        UUID machineId = UUID.randomUUID();
        Instant now = Instant.now();

        provider.record(new TurnStartedEvent(machineId, "OrderFSM", "corr-1", now));
        provider.record(new TurnCompletedEvent(machineId, "OrderFSM", "FINISH", "corr-1", Duration.ofMillis(50), now.plusMillis(50)));

        CompletableFuture<List<ExecutionEvent>> future = provider.fetchTimeline(machineId, 10);
        assertThat(future).isCompleted();

        List<ExecutionEvent> events = future.join();
        assertEquals(2, events.size());
        assertTrue(events.get(0) instanceof TurnStartedEvent);
        assertTrue(events.get(1) instanceof TurnCompletedEvent);
    }

    @Test
    @DisplayName("Should delegate to custom LocalExecutionTraceBuffer when provided")
    void testDelegationToCustomTraceBuffer() {
        FakeLocalExecutionTraceBuffer fakeBuffer = new FakeLocalExecutionTraceBuffer();
        InMemoryTraceTimelineProvider provider = new InMemoryTraceTimelineProvider(fakeBuffer);

        UUID machineId = UUID.randomUUID();
        TurnStartedEvent event = new TurnStartedEvent(machineId, "PaymentFSM", "corr-99", Instant.now());

        provider.record(event);
        assertEquals(1, fakeBuffer.records.size());
        assertEquals(event, fakeBuffer.records.getFirst());

        List<ExecutionEvent> timeline = provider.getTimeline(machineId, 5);
        assertEquals(1, timeline.size());
        assertEquals(event, timeline.getFirst());
    }

    @Test
    @DisplayName("DefaultControlPlane attachTo should not duplicate events in trace buffer")
    void testAttachToDoesNotDuplicateEvents() {
        FakeLocalExecutionTraceBuffer fakeBuffer = new FakeLocalExecutionTraceBuffer();
        EventBus fakeBus = new EventBus() {
            private ExecutionEventListener listener;

            @Override
            public void onEvent(@NonNull ExecutionEvent event) {
                fakeBuffer.record(event);
                if (listener != null) {
                    listener.onEvent(event);
                }
            }

            @Override
            public @NonNull Subscription subscribe(@NonNull ExecutionEventListener listener) {
                this.listener = listener;
                return () -> this.listener = null;
            }

            @Override
            public @NonNull LocalExecutionTraceBuffer traceBuffer() {
                return fakeBuffer;
            }

            @Override public @NonNull Subscription subscribe(@NonNull Predicate<ExecutionEvent> filter, @NonNull ExecutionEventListener listener) { return subscribe(listener); }
            @Override public <E extends ExecutionEvent> @NonNull Subscription subscribe(@NonNull Class<E> type, @NonNull Consumer<E> listener) { return () -> {}; }
            @Override public @NonNull EventStream openStream() { throw new UnsupportedOperationException(); }
            @Override public @NonNull EventStream openStream(int queueCapacity) { throw new UnsupportedOperationException(); }
            @Override public @NonNull EventStream openStream(@NonNull Predicate<ExecutionEvent> filter) { throw new UnsupportedOperationException(); }
            @Override public <E extends ExecutionEvent> @NonNull EventStream openStream(@NonNull Class<E> type) { throw new UnsupportedOperationException(); }
            @Override public @NonNull List<ExecutionEvent> history(int limit) { return Collections.emptyList(); }
            @Override public @NonNull EventBusMetrics metrics() { return new EventBusMetrics(0, 0, 0, 0, 0, 0); }
            @Override public @NonNull DynamicTapManager tapManager() { return new DynamicTapManager() {
                @Override public void registerTap(@NonNull UUID machineId) {}
                @Override public void registerTap(@NonNull UUID machineId, @NonNull Duration ttl) {}
                @Override public void unregisterTap(@NonNull UUID machineId) {}
                @Override public boolean hasActiveTap(@NonNull UUID machineId) { return false; }
                @Override public int activeTapCount() { return 0; }
                @Override public void pruneExpired() {}
                @Override public void clear() {}
            }; }
            @Override public void close() {}
        };

        try (DefaultControlPlane controlPlane = new DefaultControlPlane()) {
            controlPlane.attachTo(fakeBus);

            UUID machineId = UUID.randomUUID();
            TurnStartedEvent event = new TurnStartedEvent(machineId, "OrderFSM", "corr-1", Instant.now());
            fakeBus.onEvent(event);

            // Verify traceBuffer has EXACTLY 1 event, not 2
            assertEquals(1, fakeBuffer.records.size());
            assertEquals(1, controlPlane.getExecutionTimeline(machineId.toString(), 10).size());
        }
    }

    @Test
    @DisplayName("DefaultControlPlane should allow registering custom TraceTimelineProvider")
    void testDefaultControlPlaneCustomTimelineProvider() {
        try (DefaultControlPlane controlPlane = new DefaultControlPlane()) {
            UUID machineId = UUID.randomUUID();
            Instant now = Instant.now();
            List<ExecutionEvent> mockEvents = List.of(
                    new TurnStartedEvent(machineId, "CustomFSM", "k-1", now),
                    new TurnCompletedEvent(machineId, "CustomFSM", "DONE", "k-1", Duration.ofMillis(100), now.plusMillis(100))
            );

            TraceTimelineProvider customProvider = new TraceTimelineProvider() {
                @Override
                @NonNull
                public CompletableFuture<List<ExecutionEvent>> fetchTimeline(@NonNull UUID targetId, int limit) {
                    if (targetId.equals(machineId)) {
                        return CompletableFuture.completedFuture(mockEvents.stream().limit(limit).toList());
                    }
                    return CompletableFuture.completedFuture(Collections.emptyList());
                }
            };

            controlPlane.registerTimelineProvider(customProvider);
            assertTrue(controlPlane.getTimelineProvider().isPresent());

            List<ExecutionEvent> retrieved = controlPlane.getExecutionTimeline(machineId.toString(), 10);
            assertEquals(2, retrieved.size());
            assertEquals("CustomFSM", retrieved.get(0).machineName());

            CompletableFuture<List<ExecutionEvent>> asyncRetrieved = controlPlane.fetchExecutionTimeline(machineId.toString(), 1);
            assertThat(asyncRetrieved).isCompleted();
            assertEquals(1, asyncRetrieved.join().size());

            // Non-existent machine returns empty list
            List<ExecutionEvent> emptyList = controlPlane.getExecutionTimeline(UUID.randomUUID().toString(), 10);
            assertTrue(emptyList.isEmpty());

            // Invalid UUID string returns empty list safely
            List<ExecutionEvent> invalidList = controlPlane.getExecutionTimeline("not-a-uuid", 10);
            assertTrue(invalidList.isEmpty());
        }
    }

    private static final class FakeLocalExecutionTraceBuffer implements LocalExecutionTraceBuffer {
        private final List<ExecutionEvent> records = new ArrayList<>();

        @Override
        public void record(@NonNull ExecutionEvent event) {
            records.add(event);
        }

        @Override
        @NonNull
        public List<ExecutionEvent> getTrace(@NonNull UUID machineId) {
            return Collections.unmodifiableList(records);
        }

        @Override
        @NonNull
        public List<ExecutionEvent> getTrace(@NonNull UUID machineId, int limit) {
            return records.stream().limit(limit).toList();
        }

        @Override
        public boolean containsTrace(@NonNull UUID machineId) {
            return !records.isEmpty();
        }

        @Override
        public void evict(@NonNull UUID machineId) {
            records.clear();
        }

        @Override
        public int size() {
            return records.size();
        }

        @Override
        public void pruneExpired() {
        }

        @Override
        public void clear() {
            records.clear();
        }
    }
}
