package com.github.f442y.dispersion.serialization.avaje;

import com.github.f442y.dispersion.control.ExecutionSummary;
import com.github.f442y.dispersion.control.MachineDescriptor;
import com.github.f442y.dispersion.control.SignalDeliveryResult;
import com.github.f442y.dispersion.control.SignalRequest;
import com.github.f442y.dispersion.event.ExecutionEvent;
import com.github.f442y.dispersion.serialization.json.JsonSerializationException;
import com.github.f442y.dispersion.serialization.json.JsonSerializer;
import io.avaje.jsonb.JsonType;
import io.avaje.jsonb.Jsonb;
import io.avaje.jsonb.Types;
import org.jspecify.annotations.NonNull;

import java.util.List;
import java.util.Objects;

/**
 * High-performance Avaje JSONB implementation of {@link JsonSerializer}.
 */
public final class AvajeJsonSerializer implements JsonSerializer {

    private final Jsonb jsonb;
    private final JsonType<ExecutionEvent> eventType;
    private final JsonType<List<ExecutionEvent>> eventsListType;
    private final JsonType<MachineDescriptor> descriptorType;
    private final JsonType<List<MachineDescriptor>> descriptorsListType;
    private final JsonType<ExecutionSummary> summaryType;
    private final JsonType<List<ExecutionSummary>> summariesListType;
    private final JsonType<SignalDeliveryResult> signalResultType;
    private final JsonType<SignalRequest> signalRequestType;

    public AvajeJsonSerializer() {
        this(createDefaultJsonb());
    }

    public AvajeJsonSerializer(@NonNull Jsonb jsonb) {
        this.jsonb = Objects.requireNonNull(jsonb, "jsonb must not be null");
        this.eventType = jsonb.type(ExecutionEvent.class);
        this.eventsListType = jsonb.type(Types.listOf(ExecutionEvent.class));
        this.descriptorType = jsonb.type(MachineDescriptor.class);
        this.descriptorsListType = jsonb.type(Types.listOf(MachineDescriptor.class));
        this.summaryType = jsonb.type(ExecutionSummary.class);
        this.summariesListType = jsonb.type(Types.listOf(ExecutionSummary.class));
        this.signalResultType = jsonb.type(SignalDeliveryResult.class);
        this.signalRequestType = jsonb.type(SignalRequest.class);
    }

    @NonNull
    public static Jsonb createDefaultJsonb() {
        return Jsonb.builder().build();
    }

    @Override
    @NonNull
    public String serializeEvent(@NonNull ExecutionEvent event) {
        Objects.requireNonNull(event, "event must not be null");
        try {
            return eventType.toJson(event);
        } catch (RuntimeException ex) {
            throw new JsonSerializationException("Failed to serialize ExecutionEvent: " + event.getClass().getSimpleName(), ex);
        }
    }

    @Override
    @NonNull
    public ExecutionEvent deserializeEvent(@NonNull String json) {
        Objects.requireNonNull(json, "json must not be null");
        try {
            return eventType.fromJson(json);
        } catch (RuntimeException ex) {
            throw new JsonSerializationException("Failed to deserialize ExecutionEvent", ex);
        }
    }

    @Override
    @NonNull
    public String serializeEvents(@NonNull List<ExecutionEvent> events) {
        Objects.requireNonNull(events, "events must not be null");
        try {
            return eventsListType.toJson(events);
        } catch (RuntimeException ex) {
            throw new JsonSerializationException("Failed to serialize events list", ex);
        }
    }

    @Override
    @NonNull
    public String serializeDescriptor(@NonNull MachineDescriptor descriptor) {
        Objects.requireNonNull(descriptor, "descriptor must not be null");
        try {
            return descriptorType.toJson(descriptor);
        } catch (RuntimeException ex) {
            throw new JsonSerializationException("Failed to serialize MachineDescriptor: " + descriptor.name(), ex);
        }
    }

    @Override
    @NonNull
    public MachineDescriptor deserializeDescriptor(@NonNull String json) {
        Objects.requireNonNull(json, "json must not be null");
        try {
            return descriptorType.fromJson(json);
        } catch (RuntimeException ex) {
            throw new JsonSerializationException("Failed to deserialize MachineDescriptor", ex);
        }
    }

    @Override
    @NonNull
    public String serializeDescriptors(@NonNull List<MachineDescriptor> descriptors) {
        Objects.requireNonNull(descriptors, "descriptors must not be null");
        try {
            return descriptorsListType.toJson(descriptors);
        } catch (RuntimeException ex) {
            throw new JsonSerializationException("Failed to serialize descriptors list", ex);
        }
    }

    @Override
    @NonNull
    public String serializeSummary(@NonNull ExecutionSummary summary) {
        Objects.requireNonNull(summary, "summary must not be null");
        try {
            return summaryType.toJson(summary);
        } catch (RuntimeException ex) {
            throw new JsonSerializationException("Failed to serialize ExecutionSummary: " + summary.executionId(), ex);
        }
    }

    @Override
    @NonNull
    public ExecutionSummary deserializeSummary(@NonNull String json) {
        Objects.requireNonNull(json, "json must not be null");
        try {
            return summaryType.fromJson(json);
        } catch (RuntimeException ex) {
            throw new JsonSerializationException("Failed to deserialize ExecutionSummary", ex);
        }
    }

    @Override
    @NonNull
    public String serializeSummaries(@NonNull List<ExecutionSummary> summaries) {
        Objects.requireNonNull(summaries, "summaries must not be null");
        try {
            return summariesListType.toJson(summaries);
        } catch (RuntimeException ex) {
            throw new JsonSerializationException("Failed to serialize summaries list", ex);
        }
    }

    @Override
    @NonNull
    public String serializeSignalResult(@NonNull SignalDeliveryResult result) {
        Objects.requireNonNull(result, "result must not be null");
        try {
            return signalResultType.toJson(result);
        } catch (RuntimeException ex) {
            throw new JsonSerializationException("Failed to serialize SignalDeliveryResult", ex);
        }
    }

    @Override
    @NonNull
    public SignalDeliveryResult deserializeSignalResult(@NonNull String json) {
        Objects.requireNonNull(json, "json must not be null");
        try {
            return signalResultType.fromJson(json);
        } catch (RuntimeException ex) {
            throw new JsonSerializationException("Failed to deserialize SignalDeliveryResult", ex);
        }
    }

    @Override
    @NonNull
    public String serializeSignalRequest(@NonNull SignalRequest request) {
        Objects.requireNonNull(request, "request must not be null");
        try {
            return signalRequestType.toJson(request);
        } catch (RuntimeException ex) {
            throw new JsonSerializationException("Failed to serialize SignalRequest", ex);
        }
    }

    @Override
    @NonNull
    public SignalRequest deserializeSignalRequest(@NonNull String json) {
        Objects.requireNonNull(json, "json must not be null");
        try {
            return signalRequestType.fromJson(json);
        } catch (RuntimeException ex) {
            throw new JsonSerializationException("Failed to deserialize SignalRequest", ex);
        }
    }

    @Override
    @NonNull
    public <T> String serialize(@NonNull T value) {
        Objects.requireNonNull(value, "value must not be null");
        try {
            return jsonb.toJson(value);
        } catch (RuntimeException ex) {
            throw new JsonSerializationException("Failed to serialize value of type " + value.getClass().getName(), ex);
        }
    }

    @Override
    @NonNull
    public <T> T deserialize(@NonNull String json, @NonNull Class<T> targetType) {
        Objects.requireNonNull(json, "json must not be null");
        Objects.requireNonNull(targetType, "targetType must not be null");
        try {
            return jsonb.type(targetType).fromJson(json);
        } catch (RuntimeException ex) {
            throw new JsonSerializationException("Failed to deserialize to type " + targetType.getName(), ex);
        }
    }

    @NonNull
    public Jsonb jsonb() {
        return jsonb;
    }
}
