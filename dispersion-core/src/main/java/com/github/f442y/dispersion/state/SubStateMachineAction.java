package com.github.f442y.dispersion.state;

import com.github.f442y.dispersion.StateMachineCallable;
import com.github.f442y.dispersion.config.StateMachineConfiguration;
import com.github.f442y.dispersion.context.StateMachineContext;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * An {@link Action} that encapsulates and executes a nested (child) state machine synchronously
 * within the context of the caller's virtual thread.
 * <p>
 * This maintains transactional atomicity: if the child state machine throws an unhandled exception,
 * the exception bubbles up directly into the parent state machine's execution flow.
 *
 * @param <PARENT_CONTEXT>  The concrete type of the parent {@link StateMachineContext}
 * @param <CHILD_CONTEXT>   The concrete type of the child {@link StateMachineContext}
 * @param <CHILD_STATE_KEY> The enum type representing child state identifiers
 * @param <CHILD_INPUT>     The type of input accepted by the child state machine
 * @param <CHILD_OUTPUT>    The type of output produced by the child state machine
 */
public class SubStateMachineAction<
        PARENT_CONTEXT extends StateMachineContext,
        CHILD_CONTEXT extends StateMachineContext,
        CHILD_STATE_KEY extends Enum<CHILD_STATE_KEY> & StateKey,
        CHILD_INPUT,
        CHILD_OUTPUT>
        implements Action<PARENT_CONTEXT> {

    @NonNull
    private final StateMachineConfiguration<CHILD_CONTEXT, CHILD_STATE_KEY, CHILD_INPUT, CHILD_OUTPUT> childStateMachine;

    @NonNull
    private final Function<PARENT_CONTEXT, CHILD_INPUT> inputMapper;

    @NonNull
    private final BiFunction<PARENT_CONTEXT, CHILD_OUTPUT, PARENT_CONTEXT> outputMerger;

    @Nullable
    private final Function<PARENT_CONTEXT, CHILD_CONTEXT> initialContextMapper;

    /**
     * Constructs a {@link SubStateMachineAction} with input mapping and output merging.
     *
     * @param childStateMachine The child state machine configuration to invoke
     * @param inputMapper       Function mapping the parent context to the child input payload
     * @param outputMerger      Function merging the child execution result back into the parent context
     */
    public SubStateMachineAction(
            @NonNull StateMachineConfiguration<CHILD_CONTEXT, CHILD_STATE_KEY, CHILD_INPUT, CHILD_OUTPUT> childStateMachine,
            @NonNull Function<PARENT_CONTEXT, CHILD_INPUT> inputMapper,
            @NonNull BiFunction<PARENT_CONTEXT, CHILD_OUTPUT, PARENT_CONTEXT> outputMerger
    ) {
        this(childStateMachine, inputMapper, null, outputMerger);
    }

    /**
     * Constructs a {@link SubStateMachineAction} with input mapping, custom child context initialization,
     * and output merging.
     *
     * @param childStateMachine    The child state machine configuration to invoke
     * @param inputMapper          Function mapping the parent context to the child input payload
     * @param initialContextMapper Function creating or populating the initial child context
     * @param outputMerger         Function merging the child execution result back into the parent context
     */
    public SubStateMachineAction(
            @NonNull StateMachineConfiguration<CHILD_CONTEXT, CHILD_STATE_KEY, CHILD_INPUT, CHILD_OUTPUT> childStateMachine,
            @NonNull Function<PARENT_CONTEXT, CHILD_INPUT> inputMapper,
            @Nullable Function<PARENT_CONTEXT, CHILD_CONTEXT> initialContextMapper,
            @NonNull BiFunction<PARENT_CONTEXT, CHILD_OUTPUT, PARENT_CONTEXT> outputMerger
    ) {
        this.childStateMachine = Objects.requireNonNull(childStateMachine, "childStateMachine must not be null");
        this.inputMapper = Objects.requireNonNull(inputMapper, "inputMapper must not be null");
        this.initialContextMapper = initialContextMapper;
        this.outputMerger = Objects.requireNonNull(outputMerger, "outputMerger must not be null");
    }

    /**
     * Executes the child state machine on the current virtual thread and merges the result.
     *
     * @param parentContext The current parent state machine context
     * @return The updated parent state machine context
     * @throws Exception If child state machine execution encounters an unhandled fault
     */
    @NonNull
    @Override
    public PARENT_CONTEXT execute(@NonNull PARENT_CONTEXT parentContext) throws Exception {
        CHILD_INPUT childInput = inputMapper.apply(parentContext);
        CHILD_CONTEXT childInitialContext = (initialContextMapper != null)
                ? initialContextMapper.apply(parentContext)
                : null;

        StateMachineCallable.StateMachineCallableBuilder<CHILD_CONTEXT, CHILD_STATE_KEY, CHILD_INPUT, CHILD_OUTPUT> builder =
                new StateMachineCallable.StateMachineCallableBuilder<CHILD_CONTEXT, CHILD_STATE_KEY, CHILD_INPUT, CHILD_OUTPUT>()
                        .input(childInput);

        if (childInitialContext != null) {
            builder.initialContext(childInitialContext);
        }

        StateMachineCallable<CHILD_CONTEXT, CHILD_STATE_KEY, CHILD_INPUT, CHILD_OUTPUT> callable =
                builder.stateMachine(childStateMachine);

        CHILD_OUTPUT childOutput = callable.call();
        return outputMerger.apply(parentContext, childOutput);
    }
}
