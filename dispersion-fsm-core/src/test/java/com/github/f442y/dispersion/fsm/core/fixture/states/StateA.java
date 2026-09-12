package com.github.f442y.dispersion.fsm.core.fixture.states;

import com.github.f442y.dispersion.fsm.core.fixture.TestStateMachine.StateKeys;
import com.github.f442y.dispersion.fsm.core.fixture.TestStateMachine.TestStateMachineContext;
import com.github.f442y.dispersion.fsm.state.Action;
import com.github.f442y.dispersion.fsm.state.State;
import com.github.f442y.dispersion.fsm.state.Transition;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.Set;

public class StateA implements State<TestStateMachineContext, StateKeys> {

    @NonNull
    @Override
    public Action<TestStateMachineContext> action() {
        return (TestStateMachineContext ctx) -> {
            for (int i = 0; i < 10; i++) {
                ctx.num++;
                ctx.string = String.valueOf(ctx.num);
            }
            return ctx;
        };
    }

    @NonNull
    @Override
    public Transition<TestStateMachineContext, StateKeys> transition() {
        return Transition.to(StateKeys.B);
    }

    @NonNull
    @Override
    public Set<StateKeys> permittedTargets() {
        return Set.of(StateKeys.B);
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
