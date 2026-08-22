package com.github.f442y.dispersion.state;

import com.github.f442y.dispersion.StateMachineCallable;
import com.github.f442y.dispersion.config.StateMachineConfiguration;
import com.github.f442y.dispersion.context.StateMachineContext;
import org.jspecify.annotations.NonNull;

import java.util.Objects;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Action executing a nested child State Machine synchronously within the calling thread's virtual thread context.
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

        StateMachineCallable<CHILD_CONTEXT, CHILD_STATE_KEY, CHILD_INPUT, CHILD_OUTPUT> childCallable =
                StateMachineCallable.<CHILD_CONTEXT, CHILD_STATE_KEY, CHILD_INPUT, CHILD_OUTPUT>builder(childConfiguration)
                        .uuid(UUID.randomUUID())
                        .input(childInput)
                        .build();

        CHILD_OUTPUT childOutput = childCallable.call();
        return outputMerger.apply(parentContext, childOutput);
    }
}
