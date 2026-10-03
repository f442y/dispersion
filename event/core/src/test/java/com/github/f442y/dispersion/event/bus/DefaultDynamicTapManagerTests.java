package com.github.f442y.dispersion.event.bus;

import com.github.f442y.dispersion.event.state.StateEnteredEvent;
import com.github.f442y.dispersion.event.turn.TurnStartedEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("DefaultDynamicTapManager Tests")
class DefaultDynamicTapManagerTests {

    @Test
    @DisplayName("Should register, query, and unregister tap leases")
    void testBasicTapLifecycle() {
        try (DefaultDynamicTapManager manager = new DefaultDynamicTapManager(Duration.ofMinutes(1), null)) {
            UUID machineId = UUID.randomUUID();

            assertFalse(manager.hasActiveTap(machineId));
            assertEquals(0, manager.activeTapCount());

            manager.registerTap(machineId);
            assertTrue(manager.hasActiveTap(machineId));
            assertEquals(1, manager.activeTapCount());

            // Lifecycle event routes regardless of tap
            TurnStartedEvent lifecycleEvt = new TurnStartedEvent(UUID.randomUUID(), "Workflow", "K1", Instant.now());
            assertTrue(manager.shouldRoute(lifecycleEvt));

            // Granular event routes only if tapped
            UUID untapedMachine = UUID.randomUUID();
            StateEnteredEvent tappedGranular = new StateEnteredEvent(machineId, "Workflow", "STEP1", Instant.now());
            StateEnteredEvent untapedGranular = new StateEnteredEvent(untapedMachine, "Workflow", "STEP1", Instant.now());
            assertTrue(manager.shouldRoute(tappedGranular));
            assertFalse(manager.shouldRoute(untapedGranular));

            manager.unregisterTap(machineId);
            assertFalse(manager.hasActiveTap(machineId));
            assertEquals(0, manager.activeTapCount());
        }
    }

    @Test
    @DisplayName("Multiple concurrent clients on same execution should be ref-counted without premature revocation")
    void testMultiClientReferenceCounting() {
        try (DefaultDynamicTapManager manager = new DefaultDynamicTapManager(Duration.ofMinutes(1), null)) {
            UUID machineId = UUID.randomUUID();

            // Client 1 registers
            manager.registerTap(machineId);
            assertTrue(manager.hasActiveTap(machineId));

            // Client 2 also registers for the same execution
            manager.registerTap(machineId);
            assertTrue(manager.hasActiveTap(machineId));

            // Client 1 sends heartbeat renewal
            manager.renewTap(machineId);
            assertTrue(manager.hasActiveTap(machineId));

            // Client 1 disconnects and unregisters
            manager.unregisterTap(machineId);
            // Tap MUST remain active because Client 2 is still listening!
            assertTrue(manager.hasActiveTap(machineId), "Tap must remain active while Client 2 is still listening");

            // Client 2 disconnects and unregisters
            manager.unregisterTap(machineId);
            // Now tap should be revoked
            assertFalse(manager.hasActiveTap(machineId), "Tap must be revoked once all clients unregister");
            assertEquals(0, manager.activeTapCount());
        }
    }

    @Test
    @DisplayName("Active sweeper should automatically prune expired tap leases without manual access")
    void testActiveSweeperPrunesExpiredTaps() throws InterruptedException {
        // Fast sweep interval: 25ms, TTL: 50ms
        try (DefaultDynamicTapManager manager = new DefaultDynamicTapManager(Duration.ofMillis(50), Duration.ofMillis(25))) {
            UUID m1 = UUID.randomUUID();
            UUID m2 = UUID.randomUUID();

            // Register m1 with short TTL (50ms)
            manager.registerTap(m1);
            // Register m2 with long TTL (10s)
            manager.registerTap(m2, Duration.ofSeconds(10));

            // Both active initially
            assertTrue(manager.hasActiveTap(m1));
            assertTrue(manager.hasActiveTap(m2));

            // Await sweeper pruning m1 automatically without querying hasActiveTap(m1)
            long deadline = System.currentTimeMillis() + 3000;
            while (manager.hasActiveTap(m1) && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }

            assertFalse(manager.hasActiveTap(m1), "Expired tap m1 should be purged by active sweeper");

            // m2 should still remain active
            assertTrue(manager.hasActiveTap(m2));
        }
    }

    @Test
    @DisplayName("Close should interrupt sweeper and clear all active taps")
    void testCloseCleansUpResources() {
        DefaultDynamicTapManager manager = new DefaultDynamicTapManager(Duration.ofMinutes(5), Duration.ofMillis(25));
        UUID machineId = UUID.randomUUID();

        manager.registerTap(machineId);
        assertTrue(manager.hasActiveTap(machineId));

        manager.close();
        assertTrue(manager.isClosed());
        assertFalse(manager.hasActiveTap(machineId));
        assertEquals(0, manager.activeTapCount());

        // Registering after close is a no-op
        manager.registerTap(UUID.randomUUID());
        assertEquals(0, manager.activeTapCount());
    }
}
