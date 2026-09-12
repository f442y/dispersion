package com.github.f442y.dispersion.orchestration.core;

import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.orchestration.ParallelBranch;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BinaryOperator;
import java.util.function.Function;

/**
 * Concurrent fork-join executor for parallel branches on Java 25 Virtual Threads with
 * context isolation, fork-join result reduction, fast-failing cancellation, and
 * branch-level Saga compensation rollback.
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
     *
     * <h2>Concurrency &amp; Isolation Semantics</h2>
     * <ul>
     *   <li>If a {@code cloner} is configured, each branch receives an independent context snapshot,
     *       protecting non-thread-safe context variables from concurrent write hazards.</li>
     *   <li>Branch actions return an updated {@link StateMachineContext} instance. These results are
     *       preserved in branch declaration order.</li>
     *   <li>When all branches complete successfully, the results are folded sequentially into the
     *       primary context via {@code reducer}. If no reducer is specified, the primary context
     *       (or sole branch result) is returned.</li>
     *   <li>If any branch encounters a failure, remaining pending branches are cancelled, and all
     *       completed sibling branches undergo LIFO Saga compensation rollback.</li>
     * </ul>
     *
     * @param <CONTEXT>  The context type
     * @param branches   The list of parallel branches to execute concurrently
     * @param context    The primary input context
     * @param cloner     Optional function creating an isolated context copy for each branch
     * @param reducer    Optional reducer combining branch outputs back into the primary context
     * @param executor   The virtual thread executor
     * @return The updated or merged context after all branches finish
     * @throws Exception If any branch fails
     */
    @NonNull
    public static <CONTEXT extends StateMachineContext> CONTEXT executeParallel(
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

        for (int i = 0; i < count; i++) {
            final int branchIndex = i;
            ParallelBranch<CONTEXT> branch = branches.get(i);
            CompletableFuture<Void> branchFuture = new CompletableFuture<>();
            futures.add(branchFuture);

            // If cloner is provided, isolate input context per branch; otherwise share context instance
            CONTEXT branchInput = (cloner != null) ? cloner.apply(context) : context;

            try {
                executor.submit(() -> {
                    try {
                        if (firstFailure.get() != null) {
                            branchFuture.cancel(true);
                            return;
                        }
                        CONTEXT result = branch.action().execute(branchInput);
                        branchResults[branchIndex] = (result != null) ? result : branchInput;
                        completedBranches.add(branch);
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
            log.error("Parallel branch failure detected; executing branch-level compensations in LIFO order", failure);
            // Compensate completed branches in reverse order (LIFO)
            for (int i = completedBranches.size() - 1; i >= 0; i--) {
                ParallelBranch<CONTEXT> branch = completedBranches.get(i);
                if (branch.compensationAction() != null) {
                    try {
                        log.debug("Compensating parallel branch [{}]", branch.name());
                        branch.compensationAction().compensate(context);
                    } catch (Throwable compErr) {
                        log.error("Error compensating parallel branch [{}]", branch.name(), compErr);
                    }
                }
            }

            if (failure instanceof Exception ex) {
                throw ex;
            }
            throw new RuntimeException(failure);
        }

        // Merge branch results into final context
        if (reducer != null) {
            CONTEXT merged = context;
            for (CONTEXT branchResult : branchResults) {
                if (branchResult != null) {
                    merged = reducer.apply(merged, branchResult);
                }
            }
            return merged;
        }

        // If no reducer, return context (or sole branch result if count == 1)
        return (count == 1 && branchResults[0] != null) ? branchResults[0] : context;
    }
}
