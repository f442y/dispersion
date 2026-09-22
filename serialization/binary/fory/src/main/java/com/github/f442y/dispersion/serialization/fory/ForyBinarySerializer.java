package com.github.f442y.dispersion.serialization.fory;

import com.github.f442y.dispersion.serialization.binary.BinarySerializationException;
import com.github.f442y.dispersion.serialization.binary.BinarySerializer;
import org.apache.fory.Fory;
import org.apache.fory.ThreadSafeFory;
import org.apache.fory.config.Language;
import org.jspecify.annotations.NonNull;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * High-throughput Apache Fory implementation of {@link BinarySerializer}.
 * <p>
 * Optimized for Java 25+ Virtual Threads via {@link ThreadSafeFory} using Fory's
 * carrier-thread-proportional {@link org.apache.fory.pool.ThreadPoolFory} rather than
 * {@link org.apache.fory.ThreadLocalFory}.
 * <p>
 * Supports strict class registration (enabled by default) for minimal serialized payload size
 * (skipping full class name strings) and protection against deserialization vulnerabilities.
 */
public final class ForyBinarySerializer implements BinarySerializer {

    public static final int DEFAULT_POOL_SIZE = Math.max(Runtime.getRuntime().availableProcessors() * 4, 1);

    private final ThreadSafeFory fory;

    public ForyBinarySerializer() {
        this(createDefaultFory());
    }

    public ForyBinarySerializer(@NonNull ThreadSafeFory fory) {
        this.fory = Objects.requireNonNull(fory, "fory must not be null");
    }

    @NonNull
    public static ThreadSafeFory createDefaultFory() {
        return createDefaultFory(true);
    }

    @NonNull
    public static ThreadSafeFory createDefaultFory(boolean requireClassRegistration) {
        return createPooledFory(DEFAULT_POOL_SIZE, requireClassRegistration, null);
    }

    @NonNull
    public static ThreadSafeFory createPooledFory(int poolSize, boolean requireClassRegistration) {
        return createPooledFory(poolSize, requireClassRegistration, null);
    }

    @NonNull
    public static ThreadSafeFory createPooledFory(
            int poolSize,
            boolean requireClassRegistration,
            Consumer<ThreadSafeFory> configurator
    ) {
        ThreadSafeFory pooled = Fory.builder()
                .withLanguage(Language.JAVA)
                .requireClassRegistration(requireClassRegistration)
                .buildThreadSafeForyPool(poolSize);

        if (configurator != null) {
            configurator.accept(pooled);
        }
        return pooled;
    }

    @NonNull
    public ForyBinarySerializer register(@NonNull Class<?> type) {
        Objects.requireNonNull(type, "type must not be null");
        fory.register(type);
        return this;
    }

    @NonNull
    public ForyBinarySerializer register(@NonNull Class<?> type, int typeId) {
        Objects.requireNonNull(type, "type must not be null");
        fory.register(type, typeId);
        return this;
    }

    @NonNull
    public ForyBinarySerializer register(@NonNull Class<?> type, @NonNull String tag) {
        Objects.requireNonNull(type, "type must not be null");
        Objects.requireNonNull(tag, "tag must not be null");
        fory.register(type, tag);
        return this;
    }

    @Override
    @NonNull
    public <OBJECT_TYPE> byte[] serialize(@NonNull OBJECT_TYPE object) {
        Objects.requireNonNull(object, "object must not be null");
        try {
            return fory.serialize(object);
        } catch (BinarySerializationException ex) {
            throw ex;
        } catch (RuntimeException ex) {
            throw new BinarySerializationException("Failed to serialize object with Fory: " + object.getClass().getName(), ex);
        }
    }

    @Override
    @NonNull
    public <OBJECT_TYPE> OBJECT_TYPE deserialize(@NonNull byte[] bytes, @NonNull Class<OBJECT_TYPE> targetType) {
        Objects.requireNonNull(bytes, "bytes must not be null");
        Objects.requireNonNull(targetType, "targetType must not be null");
        try {
            Object deserialized = fory.deserialize(bytes);
            if (deserialized == null) {
                throw new BinarySerializationException("Deserialized payload resulted in null for type: " + targetType.getName());
            }
            if (!targetType.isInstance(deserialized)) {
                throw new BinarySerializationException("Deserialized object of type " + deserialized.getClass().getName()
                        + " cannot be cast to target type " + targetType.getName());
            }
            return targetType.cast(deserialized);
        } catch (BinarySerializationException ex) {
            throw ex;
        } catch (RuntimeException ex) {
            throw new BinarySerializationException("Failed to deserialize payload to target type: " + targetType.getName(), ex);
        }
    }

    @Override
    @NonNull
    public Object deserialize(@NonNull byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes must not be null");
        try {
            Object deserialized = fory.deserialize(bytes);
            if (deserialized == null) {
                throw new BinarySerializationException("Deserialized payload resulted in null");
            }
            return deserialized;
        } catch (BinarySerializationException ex) {
            throw ex;
        } catch (RuntimeException ex) {
            throw new BinarySerializationException("Failed to deserialize payload with Fory", ex);
        }
    }

    @NonNull
    public ThreadSafeFory fory() {
        return fory;
    }
}
