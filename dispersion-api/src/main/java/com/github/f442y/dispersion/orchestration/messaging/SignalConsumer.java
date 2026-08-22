package com.github.f442y.dispersion.orchestration.messaging;

import com.github.f442y.dispersion.orchestration.OrchestrationTurnResult;
import org.jspecify.annotations.NonNull;

import java.util.concurrent.CompletableFuture;

/**
 * Inbound listener contract for receiving {@link SignalMessage} instances from a message broker.
 */
@FunctionalInterface
public interface SignalConsumer {

    @NonNull
    CompletableFuture<OrchestrationTurnResult<?, ?, ?>> onMessage(@NonNull SignalMessage message);
}
