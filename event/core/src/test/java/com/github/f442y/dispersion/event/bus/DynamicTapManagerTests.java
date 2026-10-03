package com.github.f442y.dispersion.event.bus;

import com.github.f442y.dispersion.event.ExecutionEvent;
import com.github.f442y.dispersion.event.control.ExecutionCancelledEvent;
import com.github.f442y.dispersion.event.signal.SignalAwaitedEvent;
import com.github.f442y.dispersion.event.state.ActionExecutedEvent;
import com.github.f442y.dispersion.event.state.StateEnteredEvent;
import com.github.f442y.dispersion.event.turn.TurnCompletedEvent;
import com.github.f442y.dispersion.event.turn.TurnStartedEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("DynamicTapManager Live-Watch Lease & Routing Tests")
class DynamicTapManagerTests {

    @Test
    @DisplayName("Active tap lease is recognized and counts properly")
    void testRegisterAndCheckTap() {
        DefaultDynamicTapManager tapManager = new DefaultDynamicTapManager(Duration.ofSeconds(30));
        UUID machineId = UUID.randomUUID();

        assertThat(tapManager.hasActiveTap(machineId)).isFalse();
        assertThat(tapManager.activeTapCount()).isEqualTo(0);

        tapManager.registerTap(machineId);

        assertThat(tapManager.hasActiveTap(machineId)).isTrue();
        assertThat(tapManager.activeTapCount()).isEqualTo(1);

        tapManager.unregisterTap(machineId);

        assertThat(tapManager.hasActiveTap(machineId)).isFalse();
        assertThat(tapManager.activeTapCount()).isEqualTo(0);
    }

    @Test
    @DisplayName("Tap lease expires after configured TTL")
    void testTapLeaseExpiration() throws InterruptedException {
        DefaultDynamicTapManager tapManager = new DefaultDynamicTapManager();
        UUID machineId = UUID.randomUUID();

        // Register with very short TTL of 30ms
        tapManager.registerTap(machineId, Duration.ofMillis(30));
        assertThat(tapManager.hasActiveTap(machineId)).isTrue();

        Thread.sleep(60);

        assertThat(tapManager.hasActiveTap(machineId)).isFalse();
        assertThat(tapManager.activeTapCount()).isEqualTo(0);
    }

    @Test
    @DisplayName("Routing logic: LIFECYCLE events always routed, GRANULAR only routed when tapped")
    void testEventRoutingMatrix() {
        DefaultDynamicTapManager tapManager = new DefaultDynamicTapManager();
        UUID untappedId = UUID.randomUUID();
        UUID tappedId = UUID.randomUUID();
        Instant now = Instant.now();

        tapManager.registerTap(tappedId);

        // 1. Lifecycle events for UNTAPPED machine -> ALWAYS ROUTED
        TurnStartedEvent startUntapped = new TurnStartedEvent(untappedId, "Untapped", "c1", now);
        TurnCompletedEvent completeUntapped = new TurnCompletedEvent(untappedId, "Untapped", "End", "c1", Duration.ofMillis(10), now);
        ExecutionCancelledEvent cancelUntapped = new ExecutionCancelledEvent(untappedId, "Untapped", "State", "admin", "reason", now);

        assertThat(tapManager.shouldRoute(startUntapped)).isTrue();
        assertThat(tapManager.shouldRoute(completeUntapped)).isTrue();
        assertThat(tapManager.shouldRoute(cancelUntapped)).isTrue();

        // 2. Granular events for UNTAPPED machine -> SUPPRESSED
        StateEnteredEvent enterUntapped = new StateEnteredEvent(untappedId, "Untapped", "Step1", now);
        ActionExecutedEvent actionUntapped = new ActionExecutedEvent(untappedId, "Untapped", "Step1", Duration.ofMillis(5), now);
        SignalAwaitedEvent signalUntapped = new SignalAwaitedEvent(untappedId, "Untapped", "Step1", "Sig", "c1", now);

        assertThat(tapManager.shouldRoute(enterUntapped)).isFalse();
        assertThat(tapManager.shouldRoute(actionUntapped)).isFalse();
        assertThat(tapManager.shouldRoute(signalUntapped)).isFalse();

        // 3. Granular events for TAPPED machine -> ROUTED
        StateEnteredEvent enterTapped = new StateEnteredEvent(tappedId, "Tapped", "Step1", now);
        ActionExecutedEvent actionTapped = new ActionExecutedEvent(tappedId, "Tapped", "Step1", Duration.ofMillis(5), now);
        SignalAwaitedEvent signalTapped = new SignalAwaitedEvent(tappedId, "Tapped", "Step1", "Sig", "c1", now);

        assertThat(tapManager.shouldRoute(enterTapped)).isTrue();
        assertThat(tapManager.shouldRoute(actionTapped)).isTrue();
        assertThat(tapManager.shouldRoute(signalTapped)).isTrue();
    }

    @Test
    @DisplayName("Predicate filter adapts dynamic tap rules seamlessly")
    void testFilterPredicate() {
        DefaultDynamicTapManager tapManager = new DefaultDynamicTapManager();
        UUID tappedId = UUID.randomUUID();
        UUID untappedId = UUID.randomUUID();
        Instant now = Instant.now();

        tapManager.registerTap(tappedId);

        Predicate<ExecutionEvent> filter = tapManager.asFilter();

        ExecutionEvent lifecycle1 = new TurnStartedEvent(untappedId, "M1", "c1", now);
        ExecutionEvent granularUntapped = new StateEnteredEvent(untappedId, "M1", "S1", now);
        ExecutionEvent granularTapped = new StateEnteredEvent(tappedId, "M2", "S1", now);

        List<ExecutionEvent> filtered = List.of(lifecycle1, granularUntapped, granularTapped)
                .stream()
                .filter(filter)
                .toList();

        assertThat(filtered).containsExactly(lifecycle1, granularTapped);
    }

    @Test
    @DisplayName("VirtualThreadEventBus automatically records into traceBuffer and exposes tapManager")
    void testEventBusIntegrationWithTraceBufferAndTapManager() {
        VirtualThreadEventBus bus = (VirtualThreadEventBus) VirtualThreadEventBus.direct();
        UUID machineId = UUID.randomUUID();
        Instant now = Instant.now();

        assertThat(bus.traceBuffer()).isNotNull();
        assertThat(bus.tapManager()).isNotNull();

        TurnStartedEvent e1 = new TurnStartedEvent(machineId, "BusMachine", "c1", now);
        StateEnteredEvent e2 = new StateEnteredEvent(machineId, "BusMachine", "Active", now);

        bus.onEvent(e1);
        bus.onEvent(e2);

        List<ExecutionEvent> trace = bus.traceBuffer().getTrace(machineId);
        assertThat(trace).containsExactly(e1, e2);
    }
}
