package com.github.f442y.dispersion.fixture.states;

import com.github.f442y.dispersion.fixture.TestStateMachine.StateKeys;
import com.github.f442y.dispersion.fixture.TestStateMachine.TestStateMachineContext;
import com.github.f442y.dispersion.state.Action;
import com.github.f442y.dispersion.state.State;
import com.github.f442y.dispersion.state.Transition;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.Set;

public class StateC implements State<TestStateMachineContext, StateKeys> {

    @NonNull
    @Override
    public Action<TestStateMachineContext> action() {
        return Action.identity();
    }

    @NonNull
    @Override
    public Transition<TestStateMachineContext, StateKeys> transition() {
        return Transition.to(StateKeys.END);
    }

    @NonNull
    @Override
    public Set<StateKeys> permittedTargets() {
        return Set.of(StateKeys.END);
    }

    @Override
    public boolean isTerminal() {
        return false;
    }

    @Override
    public int maxVisits() {
        return -1;
    }

    @Nullable
    @Override
    public StateKeys maxVisitsFallback() {
        return null;
    }
}
