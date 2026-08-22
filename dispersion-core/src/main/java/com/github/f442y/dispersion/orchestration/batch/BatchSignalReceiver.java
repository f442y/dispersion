package com.github.f442y.dispersion.orchestration.batch;

import com.github.f442y.dispersion.orchestration.command.CommandEnvelope;
import com.github.f442y.dispersion.orchestration.command.ItemSignalCommand;
import com.github.f442y.dispersion.orchestration.command.SignalCommand;
import com.github.f442y.dispersion.orchestration.messaging.SignalConsumer;
import com.github.f442y.dispersion.orchestration.messaging.SignalMessage;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Inbound adapter routing {@link SignalMessage} instances from message brokers to batch orchestrations.
 */
public class BatchSignalReceiver implements SignalConsumer {

    private static final Logger log = LoggerFactory.getLogger(BatchSignalReceiver.class);

    private final BatchOrchestrationExecutor<?, ?, ?, ?> batchExecutor;

    public BatchSignalReceiver(@NonNull BatchOrchestrationExecutor<?, ?, ?, ?> batchExecutor) {
        this.batchExecutor = Objects.requireNonNull(batchExecutor, "batchExecutor must not be null");
    }

    public static BatchSignalReceiver forExecutor(@NonNull BatchOrchestrationExecutor<?, ?, ?, ?> batchExecutor) {
        return new BatchSignalReceiver(batchExecutor);
    }

    @Override
    @NonNull
    @SuppressWarnings({"unchecked", "rawtypes"})
    public CompletableFuture onMessage(@NonNull SignalMessage message) {
        Objects.requireNonNull(message, "message must not be null");

        log.debug("BatchSignalReceiver processing message [{}] (signal: {}, corr: {})",
                message.messageId(), message.signalName(), message.correlationKey());

        try {
            Object payload = message.payload();

            if (payload instanceof CommandEnvelope<?> env && env.command() instanceof SignalCommand) {
                return batchExecutor.handleCommand((CommandEnvelope) env);
            }

            if (payload instanceof SignalCommand cmd) {
                CommandEnvelope<SignalCommand> env = new CommandEnvelope<>(message.messageId(), message.timestamp(), cmd);
                return batchExecutor.handleCommand(env);
            }

            String itemKeyHeader = message.headers().get("itemKey");
            String batchKey = message.correlationKey();

            if (batchKey != null && itemKeyHeader != null) {
                return batchExecutor.sendItemSignal(batchKey, itemKeyHeader, message.signalName(), payload);
            }

            if (batchKey != null) {
                return batchExecutor.sendBatchSignal(batchKey, message.signalName(), payload);
            }

            CompletableFuture failed = new CompletableFuture<>();
            failed.completeExceptionally(new IllegalArgumentException("Cannot route batch SignalMessage: missing correlationKey/batchKey"));
            return failed;

        } catch (Throwable t) {
            log.error("Error routing batch SignalMessage [{}]", message.messageId(), t);
            CompletableFuture failed = new CompletableFuture<>();
            failed.completeExceptionally(t);
            return failed;
        }
    }
}
