package com.github.f442y.dispersion.serialization.avaje;

import com.github.f442y.dispersion.event.ExecutionEvent;
import com.github.f442y.dispersion.event.child.ChildMachineCompletedEvent;
import com.github.f442y.dispersion.event.child.ChildMachineSpawnedEvent;
import com.github.f442y.dispersion.event.compensation.CompensationStepCompletedEvent;
import com.github.f442y.dispersion.event.compensation.CompensationStepFailedEvent;
import com.github.f442y.dispersion.event.compensation.CompensationStepStartedEvent;
import com.github.f442y.dispersion.event.control.ExecutionCancelledEvent;
import com.github.f442y.dispersion.event.control.ExecutionPausedEvent;
import com.github.f442y.dispersion.event.control.ExecutionResumedEvent;
import com.github.f442y.dispersion.event.guard.CircuitBreakerTrippedEvent;
import com.github.f442y.dispersion.event.guard.StateVisitLimitExceededEvent;
import com.github.f442y.dispersion.event.parallel.ParallelBranchCompletedEvent;
import com.github.f442y.dispersion.event.parallel.ParallelForkStartedEvent;
import com.github.f442y.dispersion.event.parallel.ParallelJoinCompletedEvent;
import com.github.f442y.dispersion.event.retry.RetryAttemptedEvent;
import com.github.f442y.dispersion.event.retry.RetryExhaustedEvent;
import com.github.f442y.dispersion.event.signal.SignalAwaitedEvent;
import com.github.f442y.dispersion.event.signal.SignalDeliveredEvent;
import com.github.f442y.dispersion.event.signal.SignalDiscardedEvent;
import com.github.f442y.dispersion.event.signal.SignalTimedOutEvent;
import com.github.f442y.dispersion.event.state.ActionExecutedEvent;
import com.github.f442y.dispersion.event.state.StateEnteredEvent;
import com.github.f442y.dispersion.event.state.StateExitedEvent;
import com.github.f442y.dispersion.event.state.TransitionEvaluatedEvent;
import com.github.f442y.dispersion.event.turn.TurnCompensatedEvent;
import com.github.f442y.dispersion.event.turn.TurnCompletedEvent;
import com.github.f442y.dispersion.event.turn.TurnFailedEvent;
import com.github.f442y.dispersion.event.turn.TurnStartedEvent;
import com.github.f442y.dispersion.event.turn.TurnSuspendedEvent;
import com.github.f442y.dispersion.serialization.json.JsonSerializationException;
import io.avaje.json.JsonAdapter;
import io.avaje.json.JsonReader;
import io.avaje.json.JsonWriter;
import io.avaje.jsonb.CustomAdapter;
import io.avaje.jsonb.JsonType;
import io.avaje.jsonb.Jsonb;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Avaje JSONB custom adapter for polymorphic {@link ExecutionEvent} instances.
 */
@CustomAdapter
public final class ExecutionEventJsonAdapter implements JsonAdapter<ExecutionEvent> {

    private final JsonType<Object> objectType;

    public ExecutionEventJsonAdapter(@NonNull Jsonb jsonb) {
        Objects.requireNonNull(jsonb, "jsonb must not be null");
        this.objectType = jsonb.type(Object.class);
    }

    @Override
    public void toJson(@NonNull JsonWriter writer, @Nullable ExecutionEvent event) {
        if (event == null) {
            writer.nullValue();
            return;
        }

        writer.beginObject();
        writeField(writer, "eventType", event.getClass().getSimpleName());
        writeField(writer, "machineId", event.machineId().toString());
        writeField(writer, "machineName", event.machineName());
        writeField(writer, "timestamp", event.timestamp().toString());

        switch (event) {
            case TurnStartedEvent e -> writeNullable(writer, "correlationKey", e.correlationKey());
            case TurnCompletedEvent e -> {
                writeNullable(writer, "finalStateName", e.finalStateName());
                writeNullable(writer, "correlationKey", e.correlationKey());
                writeField(writer, "duration", e.duration().toString());
                writeField(writer, "durationMillis", e.duration().toMillis());
            }
            case TurnFailedEvent e -> {
                writeField(writer, "failedStateName", e.failedStateName());
                String message = e.cause().getMessage() != null ? e.cause().getMessage() : e.cause().getClass().getSimpleName();
                writeField(writer, "errorMessage", message);
                writeField(writer, "errorType", e.cause().getClass().getName());
                writeNullable(writer, "correlationKey", e.correlationKey());
                writeField(writer, "duration", e.duration().toString());
                writeField(writer, "durationMillis", e.duration().toMillis());
            }
            case TurnSuspendedEvent e -> {
                writeField(writer, "stateName", e.stateName());
                writeNullable(writer, "expectedSignal", e.expectedSignal());
                writeNullable(writer, "correlationKey", e.correlationKey());
                writeField(writer, "duration", e.duration().toString());
                writeField(writer, "durationMillis", e.duration().toMillis());
            }
            case TurnCompensatedEvent e -> {
                writeField(writer, "failedStateName", e.failedStateName());
                writeStringList(writer, "compensatedStates", e.compensatedStates());
                if (e.cause() != null) {
                    String message = e.cause().getMessage() != null ? e.cause().getMessage() : e.cause().getClass().getSimpleName();
                    writeField(writer, "errorMessage", message);
                    writeField(writer, "errorType", e.cause().getClass().getName());
                } else {
                    writeNullable(writer, "errorMessage", null);
                    writeNullable(writer, "errorType", null);
                }
                writeNullable(writer, "correlationKey", e.correlationKey());
                writeField(writer, "duration", e.duration().toString());
                writeField(writer, "durationMillis", e.duration().toMillis());
            }

            case StateEnteredEvent e -> writeField(writer, "stateName", e.stateName());
            case StateExitedEvent e -> {
                writeField(writer, "stateName", e.stateName());
                writeField(writer, "duration", e.duration().toString());
                writeField(writer, "durationMillis", e.duration().toMillis());
            }
            case TransitionEvaluatedEvent e -> {
                writeField(writer, "sourceState", e.sourceState());
                writeField(writer, "targetState", e.targetState());
            }
            case ActionExecutedEvent e -> {
                writeField(writer, "stateName", e.stateName());
                writeField(writer, "duration", e.duration().toString());
                writeField(writer, "durationMillis", e.duration().toMillis());
            }

            case SignalAwaitedEvent e -> {
                writeField(writer, "stateName", e.stateName());
                writeField(writer, "expectedSignal", e.expectedSignal());
                writeNullable(writer, "correlationKey", e.correlationKey());
            }
            case SignalDeliveredEvent e -> {
                writeField(writer, "stateName", e.stateName());
                writeField(writer, "signalName", e.signalName());
                writeNullable(writer, "correlationKey", e.correlationKey());
            }
            case SignalDiscardedEvent e -> {
                writeField(writer, "signalName", e.signalName());
                writeNullable(writer, "correlationKey", e.correlationKey());
                writeField(writer, "reason", e.reason());
            }
            case SignalTimedOutEvent e -> {
                writeField(writer, "stateName", e.stateName());
                writeField(writer, "expectedSignal", e.expectedSignal());
                writeNullable(writer, "correlationKey", e.correlationKey());
                writeField(writer, "timeout", e.timeout().toString());
                writeField(writer, "timeoutMillis", e.timeout().toMillis());
            }

            case CompensationStepStartedEvent e -> {
                writeField(writer, "stateName", e.stateName());
                writeField(writer, "isRouted", e.isRouted());
            }
            case CompensationStepCompletedEvent e -> {
                writeField(writer, "stateName", e.stateName());
                writeField(writer, "duration", e.duration().toString());
                writeField(writer, "durationMillis", e.duration().toMillis());
            }
            case CompensationStepFailedEvent e -> {
                writeField(writer, "stateName", e.stateName());
                String message = e.cause().getMessage() != null ? e.cause().getMessage() : e.cause().getClass().getSimpleName();
                writeField(writer, "errorMessage", message);
                writeField(writer, "errorType", e.cause().getClass().getName());
            }

            case RetryAttemptedEvent e -> {
                writeField(writer, "stateName", e.stateName());
                writeField(writer, "attempt", e.attempt());
                writeField(writer, "maxAttempts", e.maxAttempts());
                writeField(writer, "delay", e.delay().toString());
                writeField(writer, "delayMillis", e.delay().toMillis());
                String message = e.lastCause().getMessage() != null ? e.lastCause().getMessage() : e.lastCause().getClass().getSimpleName();
                writeField(writer, "errorMessage", message);
                writeField(writer, "errorType", e.lastCause().getClass().getName());
            }
            case RetryExhaustedEvent e -> {
                writeField(writer, "stateName", e.stateName());
                writeField(writer, "attempts", e.attempts());
                String message = e.finalCause().getMessage() != null ? e.finalCause().getMessage() : e.finalCause().getClass().getSimpleName();
                writeField(writer, "errorMessage", message);
                writeField(writer, "errorType", e.finalCause().getClass().getName());
            }

            case ChildMachineSpawnedEvent e -> {
                writeField(writer, "childMachineId", e.childMachineId().toString());
                writeField(writer, "childMachineName", e.childMachineName());
                writeField(writer, "parentStateName", e.parentStateName());
            }
            case ChildMachineCompletedEvent e -> {
                writeField(writer, "childMachineId", e.childMachineId().toString());
                writeField(writer, "childMachineName", e.childMachineName());
            }

            case ParallelForkStartedEvent e -> {
                writeField(writer, "stateName", e.stateName());
                writeStringList(writer, "branchNames", e.branchNames());
            }
            case ParallelBranchCompletedEvent e -> {
                writeField(writer, "stateName", e.stateName());
                writeField(writer, "branchName", e.branchName());
                writeField(writer, "duration", e.duration().toString());
                writeField(writer, "durationMillis", e.duration().toMillis());
            }
            case ParallelJoinCompletedEvent e -> {
                writeField(writer, "stateName", e.stateName());
                writeField(writer, "totalBranches", e.totalBranches());
                writeField(writer, "duration", e.duration().toString());
                writeField(writer, "durationMillis", e.duration().toMillis());
            }

            case StateVisitLimitExceededEvent e -> {
                writeField(writer, "stateName", e.stateName());
                writeField(writer, "visitLimit", e.visitLimit());
                writeNullable(writer, "fallbackState", e.fallbackState());
            }
            case CircuitBreakerTrippedEvent e -> writeField(writer, "maxTransitions", e.maxTransitions());

            case ExecutionPausedEvent e -> {
                writeField(writer, "stateName", e.stateName());
                writeField(writer, "reason", e.reason());
            }
            case ExecutionResumedEvent e -> writeField(writer, "stateName", e.stateName());
            case ExecutionCancelledEvent e -> {
                writeNullable(writer, "stateName", e.stateName());
                writeField(writer, "operatorId", e.operatorId());
                writeField(writer, "reason", e.reason());
            }

            default -> {
                // Extension points
            }
        }

        writer.endObject();
    }

    @Override
    @NonNull
    public ExecutionEvent fromJson(@NonNull JsonReader reader) {
        if (reader.isNullValue()) {
            throw new JsonSerializationException("Cannot deserialize null to ExecutionEvent");
        }

        Object parsed = objectType.fromJson(reader);
        if (!(parsed instanceof Map<?, ?> map)) {
            throw new JsonSerializationException("Expected JSON object for ExecutionEvent, got: " + parsed);
        }

        String eventType = requireString(map, "eventType");
        UUID machineId = UUID.fromString(requireString(map, "machineId"));
        String machineName = requireString(map, "machineName");
        Instant timestamp = Instant.parse(requireString(map, "timestamp"));

        return switch (eventType) {
            case "TurnStartedEvent" -> new TurnStartedEvent(
                    machineId,
                    machineName,
                    optString(map, "correlationKey"),
                    timestamp
            );
            case "TurnCompletedEvent" -> new TurnCompletedEvent(
                    machineId,
                    machineName,
                    optString(map, "finalStateName"),
                    optString(map, "correlationKey"),
                    parseDuration(map, "duration", "durationMillis"),
                    timestamp
            );
            case "TurnFailedEvent" -> new TurnFailedEvent(
                    machineId,
                    machineName,
                    requireString(map, "failedStateName"),
                    new RuntimeException(optString(map, "errorMessage", "Turn failed")),
                    optString(map, "correlationKey"),
                    parseDuration(map, "duration", "durationMillis"),
                    timestamp
            );
            case "TurnSuspendedEvent" -> new TurnSuspendedEvent(
                    machineId,
                    machineName,
                    requireString(map, "stateName"),
                    optString(map, "expectedSignal"),
                    optString(map, "correlationKey"),
                    parseDuration(map, "duration", "durationMillis"),
                    timestamp
            );
            case "TurnCompensatedEvent" -> new TurnCompensatedEvent(
                    machineId,
                    machineName,
                    requireString(map, "failedStateName"),
                    optStringList(map, "compensatedStates"),
                    map.get("errorMessage") != null
                            ? new RuntimeException(map.get("errorMessage").toString())
                            : null,
                    optString(map, "correlationKey"),
                    parseDuration(map, "duration", "durationMillis"),
                    timestamp
            );

            case "StateEnteredEvent" -> new StateEnteredEvent(
                    machineId,
                    machineName,
                    requireString(map, "stateName"),
                    timestamp
            );
            case "StateExitedEvent" -> new StateExitedEvent(
                    machineId,
                    machineName,
                    requireString(map, "stateName"),
                    parseDuration(map, "duration", "durationMillis"),
                    timestamp
            );
            case "TransitionEvaluatedEvent" -> new TransitionEvaluatedEvent(
                    machineId,
                    machineName,
                    requireString(map, "sourceState"),
                    requireString(map, "targetState"),
                    timestamp
            );
            case "ActionExecutedEvent" -> new ActionExecutedEvent(
                    machineId,
                    machineName,
                    requireString(map, "stateName"),
                    parseDuration(map, "duration", "durationMillis"),
                    timestamp
            );

            case "SignalAwaitedEvent" -> new SignalAwaitedEvent(
                    machineId,
                    machineName,
                    requireString(map, "stateName"),
                    requireString(map, "expectedSignal"),
                    optString(map, "correlationKey"),
                    timestamp
            );
            case "SignalDeliveredEvent" -> new SignalDeliveredEvent(
                    machineId,
                    machineName,
                    requireString(map, "stateName"),
                    requireString(map, "signalName"),
                    optString(map, "correlationKey"),
                    timestamp
            );
            case "SignalDiscardedEvent" -> new SignalDiscardedEvent(
                    machineId,
                    machineName,
                    requireString(map, "signalName"),
                    optString(map, "correlationKey"),
                    requireString(map, "reason"),
                    timestamp
            );
            case "SignalTimedOutEvent" -> new SignalTimedOutEvent(
                    machineId,
                    machineName,
                    requireString(map, "stateName"),
                    requireString(map, "expectedSignal"),
                    optString(map, "correlationKey"),
                    parseDuration(map, "timeout", "timeoutMillis"),
                    timestamp
            );

            case "CompensationStepStartedEvent" -> new CompensationStepStartedEvent(
                    machineId,
                    machineName,
                    requireString(map, "stateName"),
                    requireBoolean(map, "isRouted"),
                    timestamp
            );
            case "CompensationStepCompletedEvent" -> new CompensationStepCompletedEvent(
                    machineId,
                    machineName,
                    requireString(map, "stateName"),
                    parseDuration(map, "duration", "durationMillis"),
                    timestamp
            );
            case "CompensationStepFailedEvent" -> new CompensationStepFailedEvent(
                    machineId,
                    machineName,
                    requireString(map, "stateName"),
                    new RuntimeException(optString(map, "errorMessage", "Compensation step failed")),
                    timestamp
            );

            case "RetryAttemptedEvent" -> new RetryAttemptedEvent(
                    machineId,
                    machineName,
                    requireString(map, "stateName"),
                    requireInt(map, "attempt"),
                    requireInt(map, "maxAttempts"),
                    parseDuration(map, "delay", "delayMillis"),
                    new RuntimeException(optString(map, "errorMessage", "Retry attempt")),
                    timestamp
            );
            case "RetryExhaustedEvent" -> new RetryExhaustedEvent(
                    machineId,
                    machineName,
                    requireString(map, "stateName"),
                    requireInt(map, "attempts"),
                    new RuntimeException(optString(map, "errorMessage", "Retry exhausted")),
                    timestamp
            );

            case "ChildMachineSpawnedEvent" -> new ChildMachineSpawnedEvent(
                    machineId,
                    machineName,
                    UUID.fromString(requireString(map, "childMachineId")),
                    requireString(map, "childMachineName"),
                    requireString(map, "parentStateName"),
                    timestamp
            );
            case "ChildMachineCompletedEvent" -> new ChildMachineCompletedEvent(
                    machineId,
                    machineName,
                    UUID.fromString(requireString(map, "childMachineId")),
                    requireString(map, "childMachineName"),
                    timestamp
            );

            case "ParallelForkStartedEvent" -> new ParallelForkStartedEvent(
                    machineId,
                    machineName,
                    requireString(map, "stateName"),
                    optStringList(map, "branchNames"),
                    timestamp
            );
            case "ParallelBranchCompletedEvent" -> new ParallelBranchCompletedEvent(
                    machineId,
                    machineName,
                    requireString(map, "stateName"),
                    requireString(map, "branchName"),
                    parseDuration(map, "duration", "durationMillis"),
                    timestamp
            );
            case "ParallelJoinCompletedEvent" -> new ParallelJoinCompletedEvent(
                    machineId,
                    machineName,
                    requireString(map, "stateName"),
                    requireInt(map, "totalBranches"),
                    parseDuration(map, "duration", "durationMillis"),
                    timestamp
            );

            case "StateVisitLimitExceededEvent" -> new StateVisitLimitExceededEvent(
                    machineId,
                    machineName,
                    requireString(map, "stateName"),
                    requireInt(map, "visitLimit"),
                    optString(map, "fallbackState"),
                    timestamp
            );
            case "CircuitBreakerTrippedEvent" -> new CircuitBreakerTrippedEvent(
                    machineId,
                    machineName,
                    requireInt(map, "maxTransitions"),
                    timestamp
            );

            case "ExecutionPausedEvent" -> new ExecutionPausedEvent(
                    machineId,
                    machineName,
                    requireString(map, "stateName"),
                    requireString(map, "reason"),
                    timestamp
            );
            case "ExecutionResumedEvent" -> new ExecutionResumedEvent(
                    machineId,
                    machineName,
                    requireString(map, "stateName"),
                    timestamp
            );
            case "ExecutionCancelledEvent" -> new ExecutionCancelledEvent(
                    machineId,
                    machineName,
                    optString(map, "stateName"),
                    requireString(map, "operatorId"),
                    requireString(map, "reason"),
                    timestamp
            );

            default -> throw new JsonSerializationException("Unrecognized eventType: " + eventType);
        };
    }

    private static void writeField(@NonNull JsonWriter writer, @NonNull String name, @NonNull String value) {
        writer.name(name);
        writer.value(value);
    }

    private static void writeField(@NonNull JsonWriter writer, @NonNull String name, long value) {
        writer.name(name);
        writer.value(value);
    }

    private static void writeField(@NonNull JsonWriter writer, @NonNull String name, boolean value) {
        writer.name(name);
        writer.value(value);
    }

    private static void writeNullable(@NonNull JsonWriter writer, @NonNull String fieldName, @Nullable String value) {
        writer.name(fieldName);
        if (value != null) {
            writer.value(value);
        } else {
            writer.nullValue();
        }
    }

    private static void writeStringList(@NonNull JsonWriter writer, @NonNull String fieldName, @NonNull List<String> list) {
        writer.name(fieldName);
        writer.beginArray();
        for (String item : list) {
            writer.value(item);
        }
        writer.endArray();
    }

    @NonNull
    private static String requireString(@NonNull Map<?, ?> map, @NonNull String field) {
        Object val = map.get(field);
        if (val == null) {
            throw new JsonSerializationException("Missing required field: '" + field + "'");
        }
        return val.toString();
    }

    @Nullable
    private static String optString(@NonNull Map<?, ?> map, @NonNull String field) {
        Object val = map.get(field);
        return val != null ? val.toString() : null;
    }

    @NonNull
    private static String optString(@NonNull Map<?, ?> map, @NonNull String field, @NonNull String defaultValue) {
        Object val = map.get(field);
        return val != null ? val.toString() : defaultValue;
    }

    private static int requireInt(@NonNull Map<?, ?> map, @NonNull String field) {
        Object val = map.get(field);
        if (val instanceof Number number) {
            return number.intValue();
        }
        if (val instanceof String str) {
            return Integer.parseInt(str);
        }
        throw new JsonSerializationException("Missing or invalid int field: '" + field + "'");
    }

    private static boolean requireBoolean(@NonNull Map<?, ?> map, @NonNull String field) {
        Object val = map.get(field);
        if (val instanceof Boolean b) {
            return b;
        }
        if (val instanceof String str) {
            return Boolean.parseBoolean(str);
        }
        throw new JsonSerializationException("Missing or invalid boolean field: '" + field + "'");
    }

    @NonNull
    private static Duration parseDuration(@NonNull Map<?, ?> map, @NonNull String isoField, @NonNull String millisField) {
        Object isoVal = map.get(isoField);
        if (isoVal != null) {
            return Duration.parse(isoVal.toString());
        }
        Object millisVal = map.get(millisField);
        if (millisVal instanceof Number number) {
            return Duration.ofMillis(number.longValue());
        }
        return Duration.ZERO;
    }

    @NonNull
    private static List<String> optStringList(@NonNull Map<?, ?> map, @NonNull String field) {
        Object val = map.get(field);
        if (val instanceof List<?> list) {
            List<String> result = new ArrayList<>(list.size());
            for (Object item : list) {
                if (item != null) {
                    result.add(item.toString());
                }
            }
            return result;
        }
        return List.of();
    }
}
