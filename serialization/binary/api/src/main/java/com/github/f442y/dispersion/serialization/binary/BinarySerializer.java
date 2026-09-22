package com.github.f442y.dispersion.serialization.binary;

import org.jspecify.annotations.NonNull;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.ServiceLoader;

/**
 * Service Provider Interface (SPI) for high-performance, compact binary serialization and deserialization
 * across Dispersion state snapshots, turn checkpoints, routing envelopes, and persistent journals.
 */
public interface BinarySerializer {

    /**
     * Serializes an arbitrary object to a compact binary payload.
     *
     * @param object The object to serialize
     * @param <OBJECT_TYPE> The type of the object
     * @return The serialized binary byte array
     * @throws BinarySerializationException If serialization fails
     */
    @NonNull
    <OBJECT_TYPE> byte[] serialize(@NonNull OBJECT_TYPE object);

    /**
     * Deserializes a binary payload into an expected target type.
     *
     * @param bytes      The binary payload
     * @param targetType The expected class of the deserialized object
     * @param <OBJECT_TYPE> The target type
     * @return The deserialized object instance
     * @throws BinarySerializationException If deserialization fails
     */
    @NonNull
    <OBJECT_TYPE> OBJECT_TYPE deserialize(@NonNull byte[] bytes, @NonNull Class<OBJECT_TYPE> targetType);

    /**
     * Deserializes a binary payload into its original dynamic object type.
     *
     * @param bytes The binary payload containing encoded type metadata
     * @return The deserialized object
     * @throws BinarySerializationException If deserialization fails
     */
    @NonNull
    Object deserialize(@NonNull byte[] bytes);

    /**
     * Discovers and loads the primary registered {@link BinarySerializer} implementation
     * using standard Java {@link ServiceLoader}.
     *
     * @return An instance of {@link BinarySerializer}
     * @throws NoSuchElementException If no provider is available on the module-path or classpath
     */
    @NonNull
    static BinarySerializer load() {
        ServiceLoader<BinarySerializer> loader = ServiceLoader.load(BinarySerializer.class);
        Iterator<BinarySerializer> iterator = loader.iterator();
        if (iterator.hasNext()) {
            return iterator.next();
        }
        throw new NoSuchElementException("No BinarySerializer implementation discovered via ServiceLoader");
    }
}
