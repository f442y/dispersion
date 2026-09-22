package com.github.f442y.dispersion.serialization.json;

/**
 * Unchecked exception thrown when JSON serialization or deserialization fails.
 */
public class JsonSerializationException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public JsonSerializationException(String message) {
        super(message);
    }

    public JsonSerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
