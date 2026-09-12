package com.github.f442y.dispersion.event.test;

import com.github.f442y.dispersion.event.ExecutionEvent;
import com.github.f442y.dispersion.event.ExecutionEventListener;
import org.jspecify.annotations.NonNull;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;

/**
 * Thread-safe test listener that records emitted execution events into an inspectable list.
 */
public class CapturingEventListener implements ExecutionEventListener {

    private final List<ExecutionEvent> events = new CopyOnWriteArrayList<>();

    @Override
    public void onEvent(@NonNull ExecutionEvent event) {
        Objects.requireNonNull(event, "event must not be null");
        events.add(event);
    }

    @NonNull
    public List<ExecutionEvent> events() {
        return Collections.unmodifiableList(events);
    }

    @SuppressWarnings("unchecked")
    @NonNull
    public <E extends ExecutionEvent> List<E> eventsOfType(@NonNull Class<E> type) {
        Objects.requireNonNull(type, "type must not be null");
        return events.stream()
                .filter(type::isInstance)
                .map(e -> (E) e)
                .toList();
    }

    public boolean hasEvent(@NonNull Predicate<ExecutionEvent> filter) {
        Objects.requireNonNull(filter, "filter must not be null");
        return events.stream().anyMatch(filter);
    }

    public int count() {
        return events.size();
    }

    public void clear() {
        events.clear();
    }
}
