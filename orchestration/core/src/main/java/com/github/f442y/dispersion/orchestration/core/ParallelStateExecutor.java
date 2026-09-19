package com.github.f442y.dispersion.orchestration.core;

import com.github.f442y.dispersion.event.ExecutionEvent;
import com.github.f442y.dispersion.event.ExecutionEventListener;
import com.github.f442y.dispersion.event.parallel.ParallelBranchCompletedEvent;
import com.github.f442y.dispersion.event.parallel.ParallelForkStartedEvent;
import com.github.f442y.dispersion.event.parallel.ParallelJoinCompletedEvent;
import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.orchestration.ParallelBranch;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BinaryOperator;
import java.util.function.Function;

/**
 * Concurrent fork-join executor for parallel branches on Java 25 Virtual Threads with
 * context isolation, fork-join result reduction, fast-failing cancellation, and
 * branch-level Saga compensation rollback.\
 */
public final class ParallelStateExecutor {

    private static final Logger log = LoggerFactory.getLogger(ParallelStateExecutor.class);

    private ParallelStateExecutor() {}

    /**
     * Concurrently executes all branches on virtual threads without context isolation.
     *
     * @param <CONTEXT>  The context type
     * @param branches   The list of parallel branches
     * @param context    The shared context
     * @param executor   The virtual thread executor
     * @return The updated context after all parallel branches finish
     * @throws Exception If any branch fails
     */
    @NonNull
    public static <CONTEXT extends StateMachineContext> CONTEXT executeParallel(
            @NonNull List<ParallelBranch<CONTEXT>> branches,
            @NonNull CONTEXT context,
            @NonNull ExecutorService executor
    ) throws Exception {
        return executeParallel(branches, context, null, null, executor);
    }

    /**
     * Concurrently executes parallel branches on Java Virtual Threads with optional context isolation
     * and fork-join result reduction.
     */
    @NonNull
    public static <CONTEXT extends StateMachineContext> CONTEXT executeParallel(
            @NonNull List<ParallelBranch<CONTEXT>> branches,
            @NonNull CONTEXT context,
            @Nullable Function<CONTEXT, CONTEXT> cloner,
            @Nullable BinaryOperator<CONTEXT> reducer,
            @NonNull ExecutorService executor
    ) throws Exception {
        return executeParallel(null, null, null, null, branches, context, cloner, reducer, executor);
    }

    /**
     * Concurrently executes parallel branches on Java Virtual Threads with execution telemetry,
     * optional context isolation, and fork-join result reduction.
     *
     * @param <CONTEXT>      The context type
     * @param machineId      Optional machine execution ID for telemetry
     * @param machineName    Optional machine name for telemetry
     * @param stateName      Optional state name for telemetry
     * @param eventListener  Optional event listener for telemetry emission
     * @param branches       The list of parallel branches to execute concurrently
     * @param context        The primary input context
     * @param cloner         Optional function creating an isolated context copy for each branch
     * @param reducer        Optional reducer combining branch outputs back into the primary context
     * @param executor       The virtual thread executor
     * @return The updated or merged context after all branches finish
     * @throws Exception If any branch fails
     */
    @NonNull
    public static <CONTEXT extends StateMachineContext> CONTEXT executeParallel(
            @Nullable UUID machineId,
            @Nullable String machineName,
            @Nullable String stateName,
            @Nullable ExecutionEventListener eventListener,
            @NonNull List<ParallelBranch<CONTEXT>> branches,
            @NonNull CONTEXT context,
            @Nullable Function<CONTEXT, CONTEXT> cloner,
            @Nullable BinaryOperator<CONTEXT> reducer,
            @NonNull ExecutorService executor
    ) throws Exception {
        Objects.requireNonNull(branches, "branches must not be null");
        Objects.requireNonNull(context, "context must not be null");
        Objects.requireNonNull(executor, "executor must not be null");

        if (branches.isEmpty()) {
            return context;
        }

        int count = branches.size();
        @SuppressWarnings("unchecked")
        CONTEXT[] branchResults = (CONTEXT[]) new StateMachineContext[count];
        List<ParallelBranch<CONTEXT>> completedBranches = new CopyOnWriteArrayList<>();
        AtomicReference<Throwable> firstFailure = new AtomicReference<>();
        List<CompletableFuture<Void>> futures = new ArrayList<>(count);

        long forkStartNanos = System.nanoTime();
        if (eventListener != null && machineId != null && machineName != null && stateName != null) {
            List<String> branchNames = branches.stream().map(ParallelBranch::name).toList();
            safeNotify(eventListener, new ParallelForkStartedEvent(
                    machineId,
                    machineName,
                    stateName,
                    branchNames,
                    Instant.now()
            ));
        }

        for (int i = 0; i < count; i++) {
            final int branchIndex = i;
            ParallelBranch<CONTEXT> branch = branches.get(i);
            CompletableFuture<Void> branchFuture = new CompletableFuture<>();
            futures.add(branchFuture);

            // If cloner is provided, isolate input context per branch; otherwise share context instance
            CONTEXT branchInput = (cloner != null) ? cloner.apply(context) : context;

            try {
                executor.submit(() -> {
                    long branchStartNanos = System.nanoTime();
                    try {
                        if (firstFailure.get() != null) {
                            branchFuture.cancel(true);
                            return;
                        }
                        CONTEXT result = branch.action().execute(branchInput);
                        branchResults[branchIndex] = (result != null) ? result : branchInput;
                        completedBranches.add(branch);

                        if (eventListener != null && machineId != null && machineName != null && stateName != null) {
                            safeNotify(eventListener, new ParallelBranchCompletedEvent(
                                    machineId,
                                    machineName,
                                    stateName,
                                    branch.name(),
                                    Duration.ofNanos(System.nanoTime() - branchStartNanos),
                                    Instant.now()
                            ));
                        }

                        branchFuture.complete(null);
                    } catch (Throwable t) {
                        firstFailure.compareAndSet(null, t);
                        branchFuture.completeExceptionally(t);
                    }
                });
            } catch (Throwable t) {
                firstFailure.compareAndSet(null, t);
                branchFuture.completeExceptionally(t);
            }
        }

        // Wait for all to complete or first failure
        for (CompletableFuture<Void> future : futures) {
            try {
                future.join();
            } catch (Throwable ignored) {
                // Captured by firstFailure
            }
        }

        Throwable failure = firstFailure.get();
        if (failure != null) {
            log.atError()
                    .setCause(failure)
                    .log("Parallel branch failure detected; executing branch-level compensations in LIFO order");
            // Compensate completed branches in reverse order (LIFO)
            for (int i = completedBranches.size() - 1; i >= 0; i--) {
                ParallelBranch<CONTEXT> branch = completedBranches.get(i);
                if (branch.compensationAction() != null) {
                    try {
                        log.atDebug()
                                .addKeyValue("branch_name", branch.name())
                                .log("Compensating parallel branch");
                        branch.compensationAction().compensate(context);
                    } catch (Throwable compErr) {
                        log.atError()
                                .setCause(compErr)
                                .addKeyValue("branch_name", branch.name())
                                .log("Error compensating parallel branch");
                    }
                }
            }

            if (failure instanceof Exception ex) {
                throw ex;
            }
            throw new RuntimeException(failure);
        }

        // Merge branch results into final context
        CONTEXT merged = context;
        if (reducer != null) {
            for (CONTEXT branchResult : branchResults) {
                if (branchResult != null) {
                    merged = reducer.apply(merged, branchResult);
                }
            }
        } else if (count == 1 && branchResults[0] != null) {
            merged = branchResults[0];
        }

        if (eventListener != null && machineId != null && machineName != null && stateName != null) {
            safeNotify(eventListener, new ParallelJoinCompletedEvent(
                    machineId,
                    machineName,
                    stateName,
                    count,
                    Duration.ofNanos(System.nanoTime() - forkStartNanos),
                    Instant.now()
            ));
        }

        return merged;
    }

    private static void safeNotify(@NonNull ExecutionEventListener listener, @NonNull ExecutionEvent event) {
        try {
            listener.onEvent(event);
        } catch (Throwable t) {
            log.atError()
                    .setCause(t)
                    .addKeyValue("listener_class", listener.getClass().getName())
                    .log("ExecutionEventListener threw exception in ParallelStateExecutor");
        }
    }
}
