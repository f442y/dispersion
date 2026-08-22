package com.github.f442y.dispersion.orchestration.messaging;

import org.jspecify.annotations.NonNull;

/**
 * Codec SPI for serializing and deserializing signal command payloads to and from binary data.
 */
public interface SignalCodec {

    byte @NonNull [] serialize(@NonNull Object payload) throws Exception;

    @NonNull
    <T> T deserialize(byte @NonNull [] data, @NonNull Class<T> targetClass) throws Exception;
}
