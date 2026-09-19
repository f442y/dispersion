package com.github.f442y.dispersion.control.test;

import com.github.f442y.dispersion.control.InspectableMachine;
import com.github.f442y.dispersion.control.MachineDescriptor;
import com.github.f442y.dispersion.control.MachineType;
import com.github.f442y.dispersion.control.SignalDeliveryResult;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

/**
 * Thread-safe, in-memory {@link InspectableMachine} test double for Control Plane unit testing.
 *
 * <p>Enables testing Control Plane queries, O(1) eviction, timeline storage, and signal
 * routing in complete isolation without spinning up real execution engines.</p>
 */
public class FakeInspectableMachine implements InspectableMachine {

    public record ReceivedSignal(
            @NonNull String correlationKey,
            @NonNull String signalName,
            @Nullable Object payload
    ) {}

    private final MachineDescriptor descriptor;
    private final List<ReceivedSignal> receivedSignals = new CopyOnWriteArrayList<>();
    private final Map<String, Object> checkpoints = new ConcurrentHashMap<>();
    private Function<ReceivedSignal, SignalDeliveryResult> signalHandler;

    public FakeInspectableMachine(@NonNull String name) {
        this(name, MachineType.ORCHESTRATION);
    }

    public FakeInspectableMachine(@NonNull String name, @NonNull MachineType type) {
        this(new MachineDescriptor(
                name,
                type,
                "INITIAL",
                Set.of("COMPLETED"),
                List.of("INITIAL", "PROCESSING", "COMPLETED"),
                "stateDiagram-v2\n    [*] --> INITIAL\n    INITIAL --> PROCESSING\n    PROCESSING --> COMPLETED\n    COMPLETED --> [*]\n"
        ));
    }

    public FakeInspectableMachine(@NonNull MachineDescriptor descriptor) {
        this.descriptor = Objects.requireNonNull(descriptor, "descriptor must not be null");
        this.signalHandler = signal -> new SignalDeliveryResult(
                true,
                "Signal delivered to FakeInspectableMachine",
                descriptor.name(),
                signal.correlationKey(),
                signal.signalName(),
                true,
                false,
                "COMPLETED",
                null
        );
    }

    @Override
    @NonNull
    public MachineDescriptor descriptor() {
        return descriptor;
    }

    @Override
    @NonNull
    public CompletableFuture<SignalDeliveryResult> sendSignal(
            @NonNull String correlationKey,
            @NonNull String signalName,
            @Nullable Object payload
    ) {
        Objects.requireNonNull(correlationKey, "correlationKey must not be null");
        Objects.requireNonNull(signalName, "signalName must not be null");

        ReceivedSignal signal = new ReceivedSignal(correlationKey, signalName, payload);
        receivedSignals.add(signal);

        SignalDeliveryResult result = signalHandler.apply(signal);
        return CompletableFuture.completedFuture(result);
    }

    @Override
    @NonNull
    public Optional<Object> inspectCheckpoint(@NonNull String correlationKey) {
        Objects.requireNonNull(correlationKey, "correlationKey must not be null");
        return Optional.ofNullable(checkpoints.get(correlationKey));
    }

    // =========================================================================
    // Test Helpers
    // =========================================================================

    public FakeInspectableMachine withCheckpoint(@NonNull String correlationKey, @NonNull Object checkpoint) {
        Objects.requireNonNull(correlationKey, "correlationKey must not be null");
        Objects.requireNonNull(checkpoint, "checkpoint must not be null");
        checkpoints.put(correlationKey, checkpoint);
        return this;
    }

    public FakeInspectableMachine withSignalHandler(@NonNull Function<ReceivedSignal, SignalDeliveryResult> handler) {
        this.signalHandler = Objects.requireNonNull(handler, "handler must not be null");
        return this;
    }

    @NonNull
    public List<ReceivedSignal> receivedSignals() {
        return Collections.unmodifiableList(receivedSignals);
    }

    public boolean hasReceivedSignal(@NonNull String signalName) {
        return receivedSignals.stream().anyMatch(s -> signalName.equals(s.signalName()));
    }

    public boolean hasReceivedSignal(@NonNull String signalName, @NonNull String correlationKey) {
        return receivedSignals.stream()
                .anyMatch(s -> signalName.equals(s.signalName()) && correlationKey.equals(s.correlationKey()));
    }

    public void assertSignalReceived(@NonNull String signalName) {
        if (!hasReceivedSignal(signalName)) {
            throw new AssertionError("Expected signal [" + signalName + "] on [" + descriptor.name() + "], but was not received. Received: " + receivedSignals);
        }
    }

    public void assertSignalReceived(@NonNull String signalName, @NonNull String correlationKey) {
        if (!hasReceivedSignal(signalName, correlationKey)) {
            throw new AssertionError("Expected signal [" + signalName + "] for correlationKey [" + correlationKey + "], but none was received.");
        }
    }

    public void clear() {
        receivedSignals.clear();
        checkpoints.clear();
    }
}
