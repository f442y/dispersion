package com.github.f442y.dispersion.orchestration;

import com.github.f442y.dispersion.context.StateMachineContext;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Coordinates concurrent fork-join execution of independent {@link ParallelBranch} tasks on Java Virtual Threads.
 * Aggregates branch mutations into the orchestration context and guarantees branch-level compensation
 * rollbacks if a sibling branch fails during concurrent execution.
 */
public final class ParallelStateExecutor {

    private static final Logger log = LoggerFactory.getLogger(ParallelStateExecutor.class);

    private ParallelStateExecutor() {}

    /**
     * Executes all parallel branches concurrently across virtual threads.
     *
     * @param <C> The orchestration context type
     * @param branches The list of parallel branches to execute
     * @param context The initial context before entering the parallel state
     * @param executor The virtual thread executor service
     * @return The aggregated orchestration context after all branches succeed
     * @throws Exception If any branch fails
     */
    @NonNull
    public static <C extends StateMachineContext> C executeParallelBranches(
            @NonNull List<ParallelBranch<C>> branches,
            @NonNull C context,
            @NonNull ExecutorService executor
    ) throws Exception {
        Objects.requireNonNull(branches, "branches must not be null");
        Objects.requireNonNull(context, "context must not be null");
        Objects.requireNonNull(executor, "executor must not be null");

        if (branches.isEmpty()) {
            return context;
        }

        record BranchExecution<CTX extends StateMachineContext>(
                ParallelBranch<CTX> branch,
                Future<CTX> future
        ) {}

        List<BranchExecution<C>> executions = new ArrayList<>(branches.size());
        for (ParallelBranch<C> branch : branches) {
            Future<C> future = executor.submit(() -> {
                log.debug("Executing parallel branch [{}] on virtual thread...", branch.name());
                return branch.execute(context);
            });
            executions.add(new BranchExecution<>(branch, future));
        }

        List<ParallelBranch<C>> successfulBranches = new ArrayList<>();
        Throwable firstFailure = null;
        C accumulatedContext = context;

        for (BranchExecution<C> exec : executions) {
            try {
                C branchResult = exec.future.get();
                successfulBranches.add(exec.branch);
                // Fold the branch result into the accumulated context if distinct
                if (branchResult != null) {
                    accumulatedContext = branchResult;
                }
            } catch (Exception e) {
                if (firstFailure == null) {
                    firstFailure = (e instanceof ExecutionException) ? e.getCause() : e;
                    log.error("Parallel branch [{}] failed: {}", exec.branch.name(), firstFailure != null ? firstFailure.getMessage() : "Unknown error");
                }
            }
        }

        if (firstFailure != null) {
            log.warn("One or more parallel branches failed. Rolling back {} completed branch(es)...", successfulBranches.size());
            for (int i = successfulBranches.size() - 1; i >= 0; i--) {
                ParallelBranch<C> branch = successfulBranches.get(i);
                try {
                    log.info("Executing branch compensation for [{}]...", branch.name());
                    accumulatedContext = branch.compensationAction().compensate(accumulatedContext);
                } catch (Exception compEx) {
                    log.error("Error compensating branch [{}]: {}", branch.name(), compEx.getMessage(), compEx);
                }
            }

            if (firstFailure instanceof Exception ex) {
                throw ex;
            } else {
                throw new RuntimeException("Parallel execution failed", firstFailure);
            }
        }

        return accumulatedContext;
    }
}
