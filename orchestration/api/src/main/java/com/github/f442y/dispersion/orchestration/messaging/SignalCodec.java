package com.github.f442y.dispersion.orchestration.messaging;

import org.jspecify.annotations.NonNull;

/**
 * Serialization codec SPI for converting signal payloads to and from byte arrays or strings.
 */
public interface SignalCodec {

    /**
     * Serializes an object payload to bytes.
     *
     * @param payload The object to serialize
     * @return Serialized byte array
     * @throws Exception If serialization fails
     */
    byte @NonNull [] encode(@NonNull Object payload) throws Exception;

    /**
     * Deserializes bytes back to the target payload class.
     *
     * @param <PAYLOAD_TYPE> The target type
     * @param bytes          The serialized byte array
     * @param targetClass    The target class
     * @return Deserialized object
     * @throws Exception If deserialization fails
     */
    @NonNull
    <PAYLOAD_TYPE> PAYLOAD_TYPE decode(byte @NonNull [] bytes, @NonNull Class<PAYLOAD_TYPE> targetClass) throws Exception;
}
