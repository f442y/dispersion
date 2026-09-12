package com.github.f442y.dispersion.testing;

import com.github.f442y.dispersion.control.MachineType;
import com.github.f442y.dispersion.control.test.FakeInspectableMachine;
import com.github.f442y.dispersion.event.test.CapturingEventListener;
import com.github.f442y.dispersion.event.test.RecordingEventBus;
import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.fsm.state.StateKey;
import com.github.f442y.dispersion.fsm.test.TestStateContext;
import com.github.f442y.dispersion.orchestration.test.FakeSignalBroker;
import com.github.f442y.dispersion.orchestration.test.RecordingCheckpointStore;
import org.jspecify.annotations.NonNull;

/**
 * Convenient fluent factory providing instant access to all Dispersion test doubles, fakes, and harnesses.
 */
public final class DispersionTestKit {

    private DispersionTestKit() {}

    @NonNull
    public static RecordingEventBus recordingEventBus() {
        return new RecordingEventBus();
    }

    @NonNull
    public static CapturingEventListener capturingListener() {
        return new CapturingEventListener();
    }

    @NonNull
    public static FakeSignalBroker fakeSignalBroker() {
        return new FakeSignalBroker();
    }

    @NonNull
    public static <C extends StateMachineContext, S extends Enum<S> & StateKey>
    RecordingCheckpointStore<C, S> recordingCheckpointStore() {
        return new RecordingCheckpointStore<>();
    }

    @NonNull
    public static FakeInspectableMachine fakeInspectableMachine(@NonNull String name) {
        return new FakeInspectableMachine(name);
    }

    @NonNull
    public static FakeInspectableMachine fakeInspectableMachine(@NonNull String name, @NonNull MachineType type) {
        return new FakeInspectableMachine(name, type);
    }

    @NonNull
    public static FakeInspectableMachine fakeMachine(@NonNull String name) {
        return fakeInspectableMachine(name);
    }

    @NonNull
    public static FakeInspectableMachine fakeMachine(@NonNull String name, @NonNull MachineType type) {
        return fakeInspectableMachine(name, type);
    }

    @NonNull
    public static TestStateContext testContext() {
        return new TestStateContext();
    }

    @NonNull
    public static TestStateContext testContext(@NonNull String id) {
        return new TestStateContext(id);
    }
}
