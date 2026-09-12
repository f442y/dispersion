package com.github.f442y.dispersion.orchestration.core;

import com.github.f442y.dispersion.orchestration.*;
import com.github.f442y.dispersion.orchestration.batch.*;
import com.github.f442y.dispersion.orchestration.command.*;
import com.github.f442y.dispersion.orchestration.messaging.*;

import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Concurrent fork-join executor for parallel branches on Java 25 Virtual Threads with
 * fast-failing cancellation and branch-level Saga compensation rollback.
 */
public final class ParallelStateExecutor {

    private static final Logger log = LoggerFactory.getLogger(ParallelStateExecutor.class);

    private ParallelStateExecutor() {}

    /**
     * Concurrently executes all branches on virtual threads. If any branch fails, rolls back completed sibling branches.
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
        Objects.requireNonNull(branches, "branches must not be null");
        Objects.requireNonNull(context, "context must not be null");
        Objects.requireNonNull(executor, "executor must not be null");

        if (branches.isEmpty()) {
            return context;
        }

        List<ParallelBranch<CONTEXT>> completedBranches = new CopyOnWriteArrayList<>();
        AtomicReference<Throwable> firstFailure = new AtomicReference<>();
        List<CompletableFuture<Void>> futures = new ArrayList<>(branches.size());

        for (ParallelBranch<CONTEXT> branch : branches) {
            CompletableFuture<Void> branchFuture = new CompletableFuture<>();
            futures.add(branchFuture);

            try {
                executor.submit(() -> {
                    try {
                        if (firstFailure.get() != null) {
                            branchFuture.cancel(true);
                            return;
                        }
                        branch.action().execute(context);
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

        return context;
    }
}
