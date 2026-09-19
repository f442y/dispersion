package com.github.f442y.dispersion.orchestration.messaging.broker;

import com.github.f442y.dispersion.orchestration.OrchestrationTurnResult;
import com.github.f442y.dispersion.orchestration.command.CommandEnvelope;
import com.github.f442y.dispersion.orchestration.command.SignalCommand;
import com.github.f442y.dispersion.orchestration.messaging.SignalConsumer;
import com.github.f442y.dispersion.orchestration.messaging.SignalDispatcher;
import com.github.f442y.dispersion.orchestration.messaging.SignalMessage;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Inbound adapter routing {@link SignalMessage} instances from message brokers to state machines.
 */
public class BrokerSignalReceiver implements SignalConsumer {

    private static final Logger log = LoggerFactory.getLogger(BrokerSignalReceiver.class);

    private final SignalDispatcher dispatcher;

    public BrokerSignalReceiver(@NonNull SignalDispatcher dispatcher) {
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher must not be null");
    }

    public static BrokerSignalReceiver forDispatcher(@NonNull SignalDispatcher dispatcher) {
        return new BrokerSignalReceiver(dispatcher);
    }

    public static BrokerSignalReceiver forExecutor(@NonNull SignalDispatcher dispatcher) {
        return new BrokerSignalReceiver(dispatcher);
    }

    @Override
    @NonNull
    @SuppressWarnings({"unchecked", "rawtypes"})
    public CompletableFuture<OrchestrationTurnResult<?, ?, ?>> onMessage(@NonNull SignalMessage message) {
        Objects.requireNonNull(message, "message must not be null");

        log.atDebug()
                .addKeyValue("message_id", message.messageId())
                .addKeyValue("signal_name", message.signalName())
                .addKeyValue("correlation_key", message.correlationKey())
                .log("Processing signal message");

        try {
            Object payload = message.payload();

            if (payload instanceof CommandEnvelope<?> env) {
                return (CompletableFuture) dispatcher.handleCommand(env);
            }

            if (payload instanceof SignalCommand cmd) {
                CommandEnvelope<SignalCommand> env = new CommandEnvelope<>(message.messageId(), message.timestamp(), cmd);
                return (CompletableFuture) dispatcher.handleCommand(env);
            }

            String corrKey = message.correlationKey();
            if (corrKey != null && !corrKey.isBlank()) {
                return (CompletableFuture) dispatcher.sendSignalByCorrelationKey(corrKey, message.signalName(), payload);
            }

            Map<String, String> headers = message.headers();
            String machineIdHeader = headers != null ? headers.get("machineId") : null;
            if (machineIdHeader != null && !machineIdHeader.isBlank()) {
                UUID machineId = UUID.fromString(machineIdHeader);
                return (CompletableFuture) dispatcher.sendSignal(machineId, message.signalName(), payload);
            }

            CompletableFuture<OrchestrationTurnResult<?, ?, ?>> failed = new CompletableFuture<>();
            failed.completeExceptionally(new IllegalArgumentException(
                    "Cannot route SignalMessage [" + message.messageId() + "]: missing correlationKey, SignalCommand, or machineId"
            ));
            return failed;

        } catch (Throwable t) {
            log.atError()
                    .addKeyValue("message_id", message.messageId())
                    .setCause(t)
                    .log("Error routing signal message");
            CompletableFuture<OrchestrationTurnResult<?, ?, ?>> failed = new CompletableFuture<>();
            failed.completeExceptionally(t);
            return failed;
        }
    }
}
