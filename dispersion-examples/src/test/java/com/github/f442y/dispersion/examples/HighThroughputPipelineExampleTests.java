package com.github.f442y.dispersion.examples;

import com.github.f442y.dispersion.StateMachineFuture;
import com.github.f442y.dispersion.atomic.AtomicStateMachineBuilder;
import com.github.f442y.dispersion.atomic.AtomicStateMachineExecutor;
import com.github.f442y.dispersion.config.StateMachineConfiguration;
import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.state.StateKey;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Demonstrates high-throughput concurrent dispatch of Atomic State Machines
 * on Java 25 Virtual Threads with admission control and backpressure management.
 */
public class HighThroughputPipelineExampleTests {

    public enum PipelineState implements StateKey {
        INGEST, TRANSFORM, ENRICH, FINALIZE, DONE
    }

    public static class EventContext implements StateMachineContext {
        public String eventId;
        public long timestamp;
        public String payload;
        public int processingScore;
    }

    public record EventInput(String eventId, String rawData) {}
    public record EventOutput(String eventId, int score, String status) {}

    @Test
    @DisplayName("Should process burst of 500 concurrent events with high throughput on Virtual Threads")
    public void testHighThroughputBurst() throws Exception {
        StateMachineConfiguration<EventContext, PipelineState, EventInput, EventOutput> pipeline =
                AtomicStateMachineBuilder.<EventContext, PipelineState, EventInput, EventOutput>create(PipelineState.class)
                        .context(EventContext::new)
                        .initialState(PipelineState.INGEST)
                        .input((ctx, in) -> {
                            if (in != null) {
                                ctx.eventId = in.eventId();
                                ctx.payload = in.rawData();
                            }
                            ctx.timestamp = System.currentTimeMillis();
                            return ctx;
                        })
                        .state(PipelineState.INGEST)
                            .action(ctx -> {
                                ctx.processingScore += 10;
                                return ctx;
                            })
                            .transition(PipelineState.TRANSFORM)
                        .state(PipelineState.TRANSFORM)
                            .action(ctx -> {
                                if (ctx.payload != null) {
                                    ctx.payload = ctx.payload.toUpperCase();
                                }
                                ctx.processingScore *= 2;
                                return ctx;
                            })
                            .transition(PipelineState.ENRICH)
                        .state(PipelineState.ENRICH)
                            .action(ctx -> {
                                ctx.processingScore += 5;
                                return ctx;
                            })
                            .transition(PipelineState.FINALIZE)
                        .state(PipelineState.FINALIZE)
                            .action(ctx -> ctx)
                            .transition(PipelineState.DONE)
                        .endStates(PipelineState.DONE)
                        .output(ctx -> new EventOutput(ctx.eventId, ctx.processingScore, "PROCESSED:" + ctx.payload))
                        .build();

        int concurrentEvents = 500;
        try (AtomicStateMachineExecutor<EventContext, PipelineState, EventInput, EventOutput> executor =
                     new AtomicStateMachineExecutor<>("event-pipeline", pipeline, 100)) {

            List<StateMachineFuture<EventOutput>> futures = new ArrayList<>(concurrentEvents);

            for (int i = 0; i < concurrentEvents; i++) {
                EventInput input = new EventInput("EVT-" + i, "data-payload-" + i);
                futures.add(executor.dispatchAsync(input));
            }

            for (int i = 0; i < concurrentEvents; i++) {
                EventOutput result = futures.get(i).get();
                assertThat(result).isNotNull();
                assertThat(result.eventId()).isEqualTo("EVT-" + i);
                // (10 * 2) + 5 = 25
                assertThat(result.score()).isEqualTo(25);
                assertThat(result.status()).isEqualTo("PROCESSED:" + ("data-payload-" + i).toUpperCase());
            }
        }
    }
}
