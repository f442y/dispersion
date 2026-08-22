package com.github.f442y.dispersion.orchestration.messaging;

import com.github.f442y.dispersion.orchestration.OrchestrationStateMachineExecutor;
import com.github.f442y.dispersion.orchestration.OrchestrationTurnResult;
import com.github.f442y.dispersion.orchestration.command.CommandEnvelope;
import com.github.f442y.dispersion.orchestration.command.SignalCommand;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Inbound adapter routing {@link SignalMessage} instances from message brokers to state machines.
 */
public class SignalReceiver implements SignalConsumer {

    private static final Logger log = LoggerFactory.getLogger(SignalReceiver.class);

    private final OrchestrationStateMachineExecutor<?, ?, ?, ?> executor;

    public SignalReceiver(@NonNull OrchestrationStateMachineExecutor<?, ?, ?, ?> executor) {
        this.executor = Objects.requireNonNull(executor, "executor must not be null");
    }

    public static SignalReceiver forExecutor(@NonNull OrchestrationStateMachineExecutor<?, ?, ?, ?> executor) {
        return new SignalReceiver(executor);
    }

    @Override
    @NonNull
    @SuppressWarnings({"unchecked", "rawtypes"})
    public CompletableFuture<OrchestrationTurnResult<?, ?, ?>> onMessage(@NonNull SignalMessage message) {
        Objects.requireNonNull(message, "message must not be null");

        log.debug("Processing signal message [{}] (signal: {}, corr: {})",
                message.messageId(), message.signalName(), message.correlationKey());

        try {
            Object payload = message.payload();

            if (payload instanceof CommandEnvelope<?> env && env.command() instanceof SignalCommand) {
                return (CompletableFuture) executor.handleCommand((CommandEnvelope) env);
            }

            if (payload instanceof SignalCommand cmd) {
                CommandEnvelope<SignalCommand> env = new CommandEnvelope<>(message.messageId(), message.timestamp(), cmd);
                return (CompletableFuture) executor.handleCommand(env);
            }

            String corrKey = message.correlationKey();
            if (corrKey != null && !corrKey.isBlank()) {
                return (CompletableFuture) executor.sendSignalByCorrelationKey(corrKey, message.signalName(), payload);
            }

            String machineIdHeader = message.headers().get("machineId");
            if (machineIdHeader != null && !machineIdHeader.isBlank()) {
                UUID machineId = UUID.fromString(machineIdHeader);
                return (CompletableFuture) executor.sendSignal(machineId, message.signalName(), payload);
            }

            CompletableFuture<OrchestrationTurnResult<?, ?, ?>> failed = new CompletableFuture<>();
            failed.completeExceptionally(new IllegalArgumentException(
                    "Cannot route SignalMessage [" + message.messageId() + "]: missing correlationKey, SignalCommand, or machineId"
            ));
            return failed;

        } catch (Throwable t) {
            log.error("Error routing SignalMessage [{}]", message.messageId(), t);
            CompletableFuture<OrchestrationTurnResult<?, ?, ?>> failed = new CompletableFuture<>();
            failed.completeExceptionally(t);
            return failed;
        }
    }
}
