package com.github.f442y.dispersion.control;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Universal SPI for state machine executors that can be inspected, monitored,
 * and controlled via the {@link ControlPlane}.
 *
 * <p>Decouples the Control Plane from concrete executor implementations, enabling any
 * state machine (micro-FSM, distributed Saga, batch, or custom) to register its topology
 * and signal routing capabilities dynamically.</p>
 */
public interface InspectableMachine {

    /**
     * Returns the topology descriptor defining the machine's name, type, and states.
     *
     * @return The {@link MachineDescriptor}
     */
    @NonNull
    MachineDescriptor descriptor();

    /**
     * Dispatches an external control signal to a workflow instance associated with the correlation key.
     *
     * @param correlationKey The correlation key of the target workflow instance
     * @param signalName     The name of the signal to deliver
     * @param payload        Optional payload data
     * @return A future completing with the {@link SignalDeliveryResult}
     */
    @NonNull
    CompletableFuture<SignalDeliveryResult> sendSignal(
            @NonNull String correlationKey,
            @NonNull String signalName,
            @Nullable Object payload
    );

    /**
     * Inspects a saved checkpoint for an execution instance identified by correlation key.
     *
     * @param correlationKey The correlation key
     * @return Optional containing the checkpoint snapshot, or empty if not found or unsupported
     */
    @NonNull
    default Optional<Object> inspectCheckpoint(@NonNull String correlationKey) {
        return Optional.empty();
    }
}
