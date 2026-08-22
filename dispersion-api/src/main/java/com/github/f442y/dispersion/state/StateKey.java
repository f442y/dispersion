package com.github.f442y.dispersion.state;

import org.jspecify.annotations.NonNull;

/**
 * Marker interface for all state identifier keys within a Finite State Machine.
 * <p>
 * Typically implemented by an {@link Enum} constant representing discrete states in the graph.
 */
public interface StateKey {

    /**
     * Returns the string name or identifier of this state key.
     *
     * @return The state key name
     */
    @NonNull
    default String name() {
        return toString();
    }
}
