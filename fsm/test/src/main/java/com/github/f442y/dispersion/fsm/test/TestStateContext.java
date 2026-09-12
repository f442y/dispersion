package com.github.f442y.dispersion.fsm.test;

import com.github.f442y.dispersion.fsm.context.StateMachineContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Mutable context fixture implementing {@link StateMachineContext} for state machine tests.
 */
public class TestStateContext implements StateMachineContext {

    private final String id;
    private final List<String> auditLog = new ArrayList<>();
    private int counter = 0;
    private String status = "INITIAL";

    public TestStateContext() {
        this(UUID.randomUUID().toString());
    }

    public TestStateContext(String id) {
        this.id = id;
    }

    public String id() {
        return id;
    }

    public int counter() {
        return counter;
    }

    public void increment() {
        this.counter++;
    }

    public String status() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public void record(String step) {
        this.auditLog.add(step);
    }

    public List<String> auditLog() {
        return Collections.unmodifiableList(auditLog);
    }
}
