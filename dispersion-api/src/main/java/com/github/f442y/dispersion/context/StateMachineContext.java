package com.github.f442y.dispersion.context;

/**
 * Marker interface for state machine execution contexts.
 * <p>
 * State machine contexts hold domain data, variables, and accumulated state during execution.
 * Because state machines are thread-confined on dedicated virtual threads, context implementations
 * do not require internal synchronization.
 */
public interface StateMachineContext {
}
