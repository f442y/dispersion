package com.github.f442y.dispersion.control;

import org.jspecify.annotations.NonNull;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Immutable metadata and graph topology descriptor for a registered state machine.
 *
 * @param name         The unique identifier name of the state machine
 * @param type         The architectural machine type ({@link MachineType})
 * @param initialState The initial starting state key name
 * @param endStates    Set of terminal end-state key names
 * @param allStates    List of all declared state key names in the graph
 * @param mermaidGraph The Mermaid stateDiagram-v2 definition representing graph topology
 */
public record MachineDescriptor(
        @NonNull String name,
        @NonNull MachineType type,
        @NonNull String initialState,
        @NonNull Set<String> endStates,
        @NonNull List<String> allStates,
        @NonNull String mermaidGraph
) {
    public MachineDescriptor {
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(type, "type must not be null");
        Objects.requireNonNull(initialState, "initialState must not be null");
        endStates = Set.copyOf(Objects.requireNonNull(endStates, "endStates must not be null"));
        allStates = List.copyOf(Objects.requireNonNull(allStates, "allStates must not be null"));
        Objects.requireNonNull(mermaidGraph, "mermaidGraph must not be null");
    }
}
