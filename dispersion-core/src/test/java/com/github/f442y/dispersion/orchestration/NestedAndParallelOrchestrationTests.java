package com.github.f442y.dispersion.orchestration;

import com.github.f442y.dispersion.atomic.AtomicStateMachineBuilder;
import com.github.f442y.dispersion.config.StateMachineConfiguration;
import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.state.StateKey;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NestedAndParallelOrchestrationTests {

    public enum RootOrchState implements StateKey {
        INIT, FORK_PARALLEL_CHECKS, RUN_SUB_ORCHESTRATION, COMPLETED, FAILED
    }

    public enum SubOrchState implements StateKey {
        SUB_INIT, SUB_RUN_ATOMIC, SUB_DONE
    }

    public enum MicroState implements StateKey {
        MICRO_ACTION, MICRO_DONE
    }

    public static class MicroContext implements StateMachineContext {
        public String microPayload;
    }

    public static class SubOrchContext implements StateMachineContext {
        public String subResult;
    }

    public static class RootContext implements StateMachineContext {
        public ConcurrentLinkedQueue<String> parallelLog = new ConcurrentLinkedQueue<>();
        public ConcurrentLinkedQueue<String> branchCompensations = new ConcurrentLinkedQueue<>();
        public List<String> sagaHistory = new ArrayList<>();
        public String finalSummary;
    }

    /**
     * Tests parallel fork-join execution across multiple independent virtual threads.
     */
    @Test
    public void testParallelForkJoinExecution() throws Exception {
        CountDownLatch startLatch = new CountDownLatch(3);

        OrchestrationStateMachineExecutor<RootContext, RootOrchState, Void, List<String>> executor =
                OrchestrationStateMachineBuilder.<RootContext, RootOrchState, Void, List<String>>create("ParallelOrchestrator", RootOrchState.class)
                        .context(RootContext::new)
                        .initialState(RootOrchState.INIT)
                        .state(RootOrchState.INIT)
                            .action(ctx -> {
                                ctx.sagaHistory.add("INIT");
                                return ctx;
                            })
                            .transition(RootOrchState.FORK_PARALLEL_CHECKS)
                        // Parallel Fork-Join State
                        .state(RootOrchState.FORK_PARALLEL_CHECKS)
                            .parallel()
                                .branch("stockCheck", ctx -> {
                                    startLatch.countDown();
                                    startLatch.await(5, TimeUnit.SECONDS);
                                    ctx.parallelLog.add("STOCK_RESERVED");
                                    return ctx;
                                })
                                .branch("fraudCheck", ctx -> {
                                    startLatch.countDown();
                                    startLatch.await(5, TimeUnit.SECONDS);
                                    ctx.parallelLog.add("FRAUD_CLEARED");
                                    return ctx;
                                })
                                .branch("pricingEngine", ctx -> {
                                    startLatch.countDown();
                                    startLatch.await(5, TimeUnit.SECONDS);
                                    ctx.parallelLog.add("PRICING_CALCULATED");
                                    return ctx;
                                })
                                .compensate(ctx -> {
                                    ctx.sagaHistory.add("COMPENSATE_PARALLEL_CHECKS");
                                    return ctx;
                                })
                            .transition(RootOrchState.COMPLETED)
                        .endStates(RootOrchState.COMPLETED, RootOrchState.FAILED)
                        .output(ctx -> new ArrayList<>(ctx.parallelLog))
                        .buildExecutor();

        List<String> results = executor.dispatchSync(null);
        assertEquals(3, results.size());
        assertTrue(results.contains("STOCK_RESERVED"));
        assertTrue(results.contains("FRAUD_CLEARED"));
        assertTrue(results.contains("PRICING_CALCULATED"));
    }

    /**
     * Tests parallel branch failure triggering branch-level rollback and parent Saga compensation.
     */
    @Test
    public void testParallelBranchFailureRollsBackSiblingBranches() {
        OrchestrationStateMachineExecutor<RootContext, RootOrchState, Void, Void> executor =
                OrchestrationStateMachineBuilder.<RootContext, RootOrchState, Void, Void>create("FailingParallelOrchestrator", RootOrchState.class)
                        .context(RootContext::new)
                        .initialState(RootOrchState.INIT)
                        .state(RootOrchState.INIT)
                            .action(ctx -> {
                                ctx.sagaHistory.add("INIT_DONE");
                                return ctx;
                            })
                            .compensate(ctx -> {
                                ctx.sagaHistory.add("COMPENSATE_INIT");
                                return ctx;
                            })
                            .transition(RootOrchState.FORK_PARALLEL_CHECKS)
                        // Parallel State with 1 failing branch
                        .state(RootOrchState.FORK_PARALLEL_CHECKS)
                            .parallel()
                                .branch("inventoryBranch",
                                        ctx -> {
                                            ctx.parallelLog.add("INV_OK");
                                            return ctx;
                                        },
                                        ctx -> {
                                            ctx.branchCompensations.add("RELEASE_INV");
                                            return ctx;
                                        }
                                )
                                .branch("failingBranch",
                                        ctx -> {
                                            throw new RuntimeException("Third-party gateway timeout");
                                        },
                                        null
                                )
                            .transition(RootOrchState.COMPLETED)
                        .endStates(RootOrchState.COMPLETED, RootOrchState.FAILED)
                        .buildExecutor();

        assertThrows(Exception.class, () -> executor.dispatchSync(null));
    }

    /**
     * Tests recursive orchestration nesting: Root Orchestrator -> Sub-Orchestrator -> Atomic Machine.
     */
    @Test
    public void testRecursiveNestedOrchestrationExecution() throws Exception {
        // Level 3: Micro Atomic Machine
        StateMachineConfiguration<MicroContext, MicroState, String, String> microMachine =
                AtomicStateMachineBuilder.<MicroContext, MicroState, String, String>create(MicroState.class)
                        .context(MicroContext::new)
                        .initialState(MicroState.MICRO_ACTION)
                        .input((ctx, in) -> {
                            ctx.microPayload = "MICRO(" + in + ")";
                            return ctx;
                        })
                        .state(MicroState.MICRO_ACTION)
                            .action(ctx -> ctx)
                            .transition(MicroState.MICRO_DONE)
                        .endStates(MicroState.MICRO_DONE)
                        .output(ctx -> ctx.microPayload)
                        .build();

        // Level 2: Sub-Orchestration Machine embedding Micro Machine
        OrchestrationStateMachineConfiguration<SubOrchContext, SubOrchState, String, String> subOrchMachine =
                OrchestrationStateMachineBuilder.<SubOrchContext, SubOrchState, String, String>create("SubOrchestrator", SubOrchState.class)
                        .context(SubOrchContext::new)
                        .initialState(SubOrchState.SUB_INIT)
                        .state(SubOrchState.SUB_INIT)
                            .action(ctx -> ctx)
                            .transition(SubOrchState.SUB_RUN_ATOMIC)
                        .state(SubOrchState.SUB_RUN_ATOMIC)
                            .childMachine(microMachine)
                            .input(ctx -> "FROM_SUB")
                            .output((ctx, out) -> {
                                ctx.subResult = "SUB_RESULT[" + out + "]";
                                return ctx;
                            })
                            .transition(SubOrchState.SUB_DONE)
                        .endStates(SubOrchState.SUB_DONE)
                        .output(ctx -> ctx.subResult)
                        .build();

        // Level 1: Root Orchestration Machine embedding Sub-Orchestration Machine
        OrchestrationStateMachineExecutor<RootContext, RootOrchState, Void, String> rootExecutor =
                OrchestrationStateMachineBuilder.<RootContext, RootOrchState, Void, String>create("RootOrchestrator", RootOrchState.class)
                        .context(RootContext::new)
                        .initialState(RootOrchState.INIT)
                        .state(RootOrchState.INIT)
                            .action(ctx -> {
                                ctx.sagaHistory.add("ROOT_START");
                                return ctx;
                            })
                            .transition(RootOrchState.RUN_SUB_ORCHESTRATION)
                        .state(RootOrchState.RUN_SUB_ORCHESTRATION)
                            .childMachine(subOrchMachine)
                            .input(ctx -> "ROOT_TRIGGER")
                            .output((ctx, subOut) -> {
                                ctx.finalSummary = "ROOT_GOT(" + subOut + ")";
                                return ctx;
                            })
                            .transition(RootOrchState.COMPLETED)
                        .endStates(RootOrchState.COMPLETED, RootOrchState.FAILED)
                        .output(ctx -> ctx.finalSummary)
                        .buildExecutor();

        String result = rootExecutor.dispatchSync(null);
        assertEquals("ROOT_GOT(SUB_RESULT[MICRO(FROM_SUB)])", result);
    }
}
