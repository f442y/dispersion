package com.github.f442y.dispersion.event.bus;

import com.github.f442y.dispersion.event.ExecutionEvent;
import com.github.f442y.dispersion.event.state.StateEnteredEvent;
import com.github.f442y.dispersion.event.turn.TurnCompletedEvent;
import com.github.f442y.dispersion.event.turn.TurnStartedEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("ConcurrentRingBufferTraceBuffer Tests")
class ConcurrentRingBufferTraceBufferTests {

    @Test
    @DisplayName("Should buffer events per machine and limit trace queries")
    void testBasicBufferingAndLimiting() {
        try (ConcurrentRingBufferTraceBuffer buffer = ConcurrentRingBufferTraceBuffer.builder()
                .eventsPerExecution(5)
                .sweepInterval(null)
                .build()) {

            UUID machineId = UUID.randomUUID();
            Instant now = Instant.now();

            for (int i = 0; i < 10; i++) {
                buffer.record(new StateEnteredEvent(machineId, "MachineA", "STATE_" + i, now.plusMillis(i)));
            }

            assertTrue(buffer.containsTrace(machineId));
            assertEquals(1, buffer.size());

            List<ExecutionEvent> fullTrace = buffer.getTrace(machineId);
            assertEquals(5, fullTrace.size(), "Should only retain the last 5 events according to capacity");
            assertEquals("STATE_5", ((StateEnteredEvent) fullTrace.getFirst()).stateName());
            assertEquals("STATE_9", ((StateEnteredEvent) fullTrace.getLast()).stateName());

            List<ExecutionEvent> limited = buffer.getTrace(machineId, 2);
            assertEquals(2, limited.size());
            assertEquals("STATE_8", ((StateEnteredEvent) limited.getFirst()).stateName());
            assertEquals("STATE_9", ((StateEnteredEvent) limited.getLast()).stateName());
        }
    }

    @Test
    @DisplayName("Active sweeper should automatically evict expired terminal traces without manual trigger")
    void testActiveSweeperEvictsExpiredTerminalTraces() throws InterruptedException {
        // Fast sweep interval: 25ms, TTL: 50ms
        try (ConcurrentRingBufferTraceBuffer buffer = ConcurrentRingBufferTraceBuffer.builder()
                .terminalTtl(Duration.ofMillis(50))
                .sweepInterval(Duration.ofMillis(25))
                .build()) {

            UUID activeMachine = UUID.randomUUID();
            UUID completedMachine = UUID.randomUUID();
            Instant now = Instant.now();

            // Active machine: non-terminal event
            buffer.record(new TurnStartedEvent(activeMachine, "MachineA", "KEY-1", now));

            // Completed machine: terminal event
            buffer.record(new TurnCompletedEvent(completedMachine, "MachineB", "SUCCESS", "KEY-2", Duration.ofMillis(10), now));

            assertEquals(2, buffer.size());

            // Await sweeper pruning the completed machine trace
            long deadline = System.currentTimeMillis() + 3000;
            while (buffer.containsTrace(completedMachine) && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }

            assertFalse(buffer.containsTrace(completedMachine), "Terminal machine should be pruned by active sweeper");

            // Active machine remains intact
            assertTrue(buffer.containsTrace(activeMachine), "Active non-terminal trace must not be pruned");
            assertEquals(1, buffer.size());
        }
    }

    @Test
    @DisplayName("Should enforce hard capacity eviction prioritising terminal traces then oldest entries")
    void testHardCapacityEviction() {
        try (ConcurrentRingBufferTraceBuffer buffer = ConcurrentRingBufferTraceBuffer.builder()
                .maxMachines(2)
                .sweepInterval(null) // sweeper disabled to test in-line capacity eviction
                .terminalTtl(Duration.ofHours(1)) // long TTL so normal pruning doesn't remove it
                .build()) {

            UUID machine1Active = UUID.randomUUID();
            UUID machine2Completed = UUID.randomUUID();
            UUID machine3New = UUID.randomUUID();
            Instant now = Instant.now();

            // Machine 1 active
            buffer.record(new TurnStartedEvent(machine1Active, "Machine1", "K1", now.minusSeconds(10)));
            // Machine 2 terminal
            buffer.record(new TurnCompletedEvent(machine2Completed, "Machine2", "DONE", "K2", Duration.ofMillis(5), now.minusSeconds(5)));

            assertEquals(2, buffer.size());

            // Add machine 3: exceeds maxMachines (2). Machine 2 is terminal, so it should be evicted first!
            buffer.record(new TurnStartedEvent(machine3New, "Machine3", "K3", now));

            assertEquals(2, buffer.size(), "Size must remain bounded at maxMachines (2)");
            assertTrue(buffer.containsTrace(machine1Active), "Active machine 1 should be retained");
            assertTrue(buffer.containsTrace(machine3New), "New machine 3 should be retained");
            assertFalse(buffer.containsTrace(machine2Completed), "Terminal machine 2 should be evicted first");
        }
    }

    @Test
    @DisplayName("Explicit evict, clear, and close should release all traces")
    void testEvictClearAndClose() {
        ConcurrentRingBufferTraceBuffer buffer = ConcurrentRingBufferTraceBuffer.builder()
                .sweepInterval(Duration.ofMillis(50))
                .build();

        UUID m1 = UUID.randomUUID();
        UUID m2 = UUID.randomUUID();
        Instant now = Instant.now();

        buffer.record(new TurnStartedEvent(m1, "M1", "K1", now));
        buffer.record(new TurnStartedEvent(m2, "M2", "K2", now));
        assertEquals(2, buffer.size());

        buffer.evict(m1);
        assertFalse(buffer.containsTrace(m1));
        assertEquals(1, buffer.size());

        buffer.clear();
        assertEquals(0, buffer.size());

        buffer.record(new TurnStartedEvent(m1, "M1", "K1", now));
        assertEquals(1, buffer.size());

        buffer.close();
        assertTrue(buffer.isClosed());
        assertEquals(0, buffer.size());

        // Recording after close is a no-op
        buffer.record(new TurnStartedEvent(m2, "M2", "K2", now));
        assertEquals(0, buffer.size());
    }
}
