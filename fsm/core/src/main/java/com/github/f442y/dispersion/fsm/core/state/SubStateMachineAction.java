package com.github.f442y.dispersion.fsm.core.state;

import com.github.f442y.dispersion.fsm.state.Action;
import com.github.f442y.dispersion.fsm.state.StateKey;
import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.fsm.config.StateMachineConfiguration;

import com.github.f442y.dispersion.fsm.core.AbstractStateMachineCallable;
import com.github.f442y.dispersion.fsm.config.StateMachineConfiguration;
import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import org.jspecify.annotations.NonNull;

import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Action executing a nested child State Machine synchronously within the calling thread's virtual thread context
 * with zero-allocation direct execution.
 *
 * <h2>Hierarchical State Machine Nesting</h2>
 * Allows atomic micro-state machines to be embedded directly as steps within parent state machines:
 * <ul>
 *   <li>Maps parent context variables to child input via {@code inputMapper}.</li>
 *   <li>Executes the child state machine graph synchronously using {@link AbstractStateMachineCallable#executeDirect}.</li>
 *   <li>Merges the child result back into the parent context via {@code outputMerger}.</li>
 *   <li>Incurs zero thread context switching or task wrapper allocations.</li>
 * </ul>
 *
 * @param <PARENT_CONTEXT>  The parent context type
 * @param <CHILD_CONTEXT>   The child context type
 * @param <CHILD_STATE_KEY> The child state key enum type
 * @param <CHILD_INPUT>     The child input type
 * @param <CHILD_OUTPUT>    The child output type
 */
public class SubStateMachineAction<
        PARENT_CONTEXT extends StateMachineContext,
        CHILD_CONTEXT extends StateMachineContext,
        CHILD_STATE_KEY extends Enum<CHILD_STATE_KEY> & StateKey,
        CHILD_INPUT,
        CHILD_OUTPUT> implements Action<PARENT_CONTEXT> {

    private final StateMachineConfiguration<CHILD_CONTEXT, CHILD_STATE_KEY, CHILD_INPUT, CHILD_OUTPUT> childConfiguration;
    private final Function<PARENT_CONTEXT, CHILD_INPUT> inputMapper;
    private final BiFunction<PARENT_CONTEXT, CHILD_OUTPUT, PARENT_CONTEXT> outputMerger;

    public SubStateMachineAction(
            @NonNull StateMachineConfiguration<CHILD_CONTEXT, CHILD_STATE_KEY, CHILD_INPUT, CHILD_OUTPUT> childConfiguration,
            @NonNull Function<PARENT_CONTEXT, CHILD_INPUT> inputMapper,
            @NonNull BiFunction<PARENT_CONTEXT, CHILD_OUTPUT, PARENT_CONTEXT> outputMerger
    ) {
        this.childConfiguration = Objects.requireNonNull(childConfiguration, "childConfiguration must not be null");
        this.inputMapper = Objects.requireNonNull(inputMapper, "inputMapper must not be null");
        this.outputMerger = Objects.requireNonNull(outputMerger, "outputMerger must not be null");
    }

    @NonNull
    @Override
    public PARENT_CONTEXT execute(@NonNull PARENT_CONTEXT parentContext) throws Exception {
        CHILD_INPUT childInput = inputMapper.apply(parentContext);
        CHILD_OUTPUT childOutput = AbstractStateMachineCallable.executeDirect(null, childConfiguration, null, childInput);
        return outputMerger.apply(parentContext, childOutput);
    }
}
