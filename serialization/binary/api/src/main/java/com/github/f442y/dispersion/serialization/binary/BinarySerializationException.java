package com.github.f442y.dispersion.serialization.binary;

/**
 * Unchecked exception thrown when binary serialization or deserialization fails.
 */
public class BinarySerializationException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public BinarySerializationException(String message) {
        super(message);
    }

    public BinarySerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
