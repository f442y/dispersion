package com.github.f442y.dispersion.serialization.json;

import com.github.f442y.dispersion.control.ExecutionSummary;
import com.github.f442y.dispersion.control.MachineDescriptor;
import com.github.f442y.dispersion.control.SignalDeliveryResult;
import com.github.f442y.dispersion.control.SignalRequest;
import com.github.f442y.dispersion.event.ExecutionEvent;
import org.jspecify.annotations.NonNull;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.ServiceLoader;

/**
 * Service Provider Interface (SPI) for JSON serialization and deserialization across Dispersion
 * Control Plane entities, state machine topology descriptors, and telemetry execution events.
 */
public interface JsonSerializer {

    /**
     * Serializes an {@link ExecutionEvent} to a JSON string.
     *
     * @param event The execution event to serialize
     * @return The JSON string representation
     * @throws JsonSerializationException If serialization fails
     */
    @NonNull
    String serializeEvent(@NonNull ExecutionEvent event);

    /**
     * Deserializes an {@link ExecutionEvent} from a JSON string.
     *
     * @param json The JSON string
     * @return The deserialized execution event
     * @throws JsonSerializationException If deserialization fails
     */
    @NonNull
    ExecutionEvent deserializeEvent(@NonNull String json);

    /**
     * Serializes a list of {@link ExecutionEvent} instances to a JSON string.
     *
     * @param events The events list
     * @return The JSON string representation
     * @throws JsonSerializationException If serialization fails
     */
    @NonNull
    String serializeEvents(@NonNull List<ExecutionEvent> events);

    /**
     * Serializes a {@link MachineDescriptor} to a JSON string.
     *
     * @param descriptor The machine descriptor
     * @return The JSON string representation
     * @throws JsonSerializationException If serialization fails
     */
    @NonNull
    String serializeDescriptor(@NonNull MachineDescriptor descriptor);

    /**
     * Deserializes a {@link MachineDescriptor} from a JSON string.
     *
     * @param json The JSON string
     * @return The deserialized machine descriptor
     * @throws JsonSerializationException If deserialization fails
     */
    @NonNull
    MachineDescriptor deserializeDescriptor(@NonNull String json);

    /**
     * Serializes a list of {@link MachineDescriptor} instances to a JSON string.
     *
     * @param descriptors The machine descriptors list
     * @return The JSON string representation
     * @throws JsonSerializationException If serialization fails
     */
    @NonNull
    String serializeDescriptors(@NonNull List<MachineDescriptor> descriptors);

    /**
     * Serializes an {@link ExecutionSummary} to a JSON string.
     *
     * @param summary The execution summary
     * @return The JSON string representation
     * @throws JsonSerializationException If serialization fails
     */
    @NonNull
    String serializeSummary(@NonNull ExecutionSummary summary);

    /**
     * Deserializes an {@link ExecutionSummary} from a JSON string.
     *
     * @param json The JSON string
     * @return The deserialized execution summary
     * @throws JsonSerializationException If deserialization fails
     */
    @NonNull
    ExecutionSummary deserializeSummary(@NonNull String json);

    /**
     * Serializes a list of {@link ExecutionSummary} instances to a JSON string.
     *
     * @param summaries The summaries list
     * @return The JSON string representation
     * @throws JsonSerializationException If serialization fails
     */
    @NonNull
    String serializeSummaries(@NonNull List<ExecutionSummary> summaries);

    /**
     * Serializes a {@link SignalDeliveryResult} to a JSON string.
     *
     * @param result The signal delivery result
     * @return The JSON string representation
     * @throws JsonSerializationException If serialization fails
     */
    @NonNull
    String serializeSignalResult(@NonNull SignalDeliveryResult result);

    /**
     * Deserializes a {@link SignalDeliveryResult} from a JSON string.
     *
     * @param json The JSON string
     * @return The deserialized signal delivery result
     * @throws JsonSerializationException If deserialization fails
     */
    @NonNull
    SignalDeliveryResult deserializeSignalResult(@NonNull String json);

    /**
     * Serializes a {@link SignalRequest} to a JSON string.
     *
     * @param request The signal request
     * @return The JSON string representation
     * @throws JsonSerializationException If serialization fails
     */
    @NonNull
    String serializeSignalRequest(@NonNull SignalRequest request);

    /**
     * Deserializes a {@link SignalRequest} from a JSON string.
     *
     * @param json The JSON string
     * @return The deserialized signal request
     * @throws JsonSerializationException If deserialization fails
     */
    @NonNull
    SignalRequest deserializeSignalRequest(@NonNull String json);

    /**
     * Serializes an arbitrary object to a JSON string.
     *
     * @param value The value to serialize
     * @param <T> The value type
     * @return The JSON string representation
     * @throws JsonSerializationException If serialization fails
     */
    @NonNull
    <T> String serialize(@NonNull T value);

    /**
     * Deserializes a JSON string into the target class type.
     *
     * @param json The JSON string
     * @param targetType The class of type T
     * @param <T> The target deserialization type
     * @return The deserialized object
     * @throws JsonSerializationException If deserialization fails
     */
    @NonNull
    <T> T deserialize(@NonNull String json, @NonNull Class<T> targetType);

    /**
     * Discovers and loads the default {@link JsonSerializer} implementation using the Java
     * {@link ServiceLoader} mechanism.
     *
     * @return The registered {@link JsonSerializer}
     * @throws NoSuchElementException If no implementation is found on the classpath/module-path
     */
    @NonNull
    static JsonSerializer load() {
        ServiceLoader<JsonSerializer> loader = ServiceLoader.load(JsonSerializer.class);
        Iterator<JsonSerializer> iterator = loader.iterator();
        if (iterator.hasNext()) {
            return iterator.next();
        }
        throw new NoSuchElementException("No implementation of JsonSerializer found via ServiceLoader");
    }
}
