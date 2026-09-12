package com.github.f442y.dispersion.orchestration.core.batch;

import com.github.f442y.dispersion.fsm.StateMachine;
import com.github.f442y.dispersion.fsm.StateMachineFuture;
import com.github.f442y.dispersion.fsm.config.*;
import com.github.f442y.dispersion.fsm.context.*;
import com.github.f442y.dispersion.fsm.exception.*;
import com.github.f442y.dispersion.fsm.executor.*;
import com.github.f442y.dispersion.fsm.state.*;
import com.github.f442y.dispersion.fsm.core.*;
import com.github.f442y.dispersion.fsm.core.atomic.*;
import com.github.f442y.dispersion.fsm.core.builder.*;
import com.github.f442y.dispersion.event.*;
import com.github.f442y.dispersion.event.dispatcher.*;
import com.github.f442y.dispersion.orchestration.*;
import com.github.f442y.dispersion.orchestration.batch.*;
import com.github.f442y.dispersion.orchestration.command.*;
import com.github.f442y.dispersion.orchestration.messaging.*;
import com.github.f442y.dispersion.orchestration.core.*;
import com.github.f442y.dispersion.orchestration.core.batch.*;
import com.github.f442y.dispersion.orchestration.core.messaging.*;

import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BatchOrchestrationTests {

    public enum DocumentState implements StateKey {
        UPLOAD,
        OCR_EXTRACT,
        AWAIT_HUMAN_REVIEW,
        AGGREGATION_BARRIER,
        FINALIZE_BATCH,
        COMPLETED
    }

    public static class BatchContext implements StateMachineContext {
        public String batchId;
    }

    public static class DocumentContext implements StateMachineContext {
        public String batchId;
        public String documentId;
        public boolean ocrCompleted = false;
        public boolean humanApproved = false;
        public String reviewerNote;
        public List<String> itemLog = new ArrayList<>();
    }

    public record ApproveDocumentCommand(
            String batchKey,
            String documentId,
            String note
    ) implements ItemSignalCommand {

        @Override
        @NonNull
        public String signalName() {
            return "ApproveDocumentCommand";
        }

        @Override
        public String correlationKey() {
            return batchKey;
        }

        @Override
        @NonNull
        public String itemKey() {
            return documentId;
        }
    }

    /**
     * Tests independent streaming: Item 1 gets its signal and advances to the barrier,
     * while Item 2 stays suspended waiting for its signal.
     */
    @Test
    public void testIndependentStreamingAndItemizedSignalProgression() throws Exception {
        BatchOrchestrationExecutor<BatchContext, DocumentContext, DocumentState, String> executor =
                BatchOrchestrationStateMachineBuilder.<BatchContext, DocumentContext, DocumentState, String>create("DocumentBatchJob", DocumentState.class)
                .batchContext(BatchContext::new)
                .batchKey(ctx -> ctx.batchId)
                .itemKey(ctx -> ctx.documentId)
                .initialState(DocumentState.UPLOAD)
                .itemState(DocumentState.UPLOAD)
                    .action(ctx -> {
                        ctx.itemLog.add("UPLOAD");
                        return ctx;
                    })
                    .transition(DocumentState.OCR_EXTRACT)
                .itemState(DocumentState.OCR_EXTRACT)
                    .action(ctx -> {
                        ctx.ocrCompleted = true;
                        ctx.itemLog.add("OCR");
                        return ctx;
                    })
                    .transition(DocumentState.AWAIT_HUMAN_REVIEW)
                // Item waits for item-specific approval command
                .itemState(DocumentState.AWAIT_HUMAN_REVIEW)
                    .waitForCommand(ApproveDocumentCommand.class, (ctx, cmd) -> {
                        if (cmd != null) {
                            ctx.humanApproved = true;
                            ctx.reviewerNote = cmd.note();
                            ctx.itemLog.add("APPROVED:" + cmd.note());
                        }
                        return ctx;
                    })
                    .transition(DocumentState.AGGREGATION_BARRIER)
                // Barrier: waits for all items in batch
                .itemState(DocumentState.AGGREGATION_BARRIER)
                    .barrier(BarrierPolicy.ALL_ITEMS_ARRIVED)
                    .transition(DocumentState.COMPLETED)
                .endStates(DocumentState.COMPLETED)
                .output(ctx -> "Batch " + ctx.batchId + " completed")
                .buildExecutor();

        BatchContext batch = new BatchContext();
        batch.batchId = "BATCH-707";

        DocumentContext docA = new DocumentContext();
        docA.batchId = "BATCH-707";
        docA.documentId = "DOC-A";

        DocumentContext docB = new DocumentContext();
        docB.batchId = "BATCH-707";
        docB.documentId = "DOC-B";

        // 1. Dispatch batch turn: Both Doc A & Doc B run UPLOAD -> OCR -> pause at AWAIT_HUMAN_REVIEW
        BatchTurnResult<BatchContext, DocumentContext, DocumentState, String> initialTurn =
                executor.dispatchBatchSync(batch, List.of(docA, docB));

        assertTrue(initialTurn.isSuspended());
        assertFalse(initialTurn.isCompleted());
        assertEquals(DocumentState.AWAIT_HUMAN_REVIEW, initialTurn.itemStates().get("DOC-A"));
        assertEquals(DocumentState.AWAIT_HUMAN_REVIEW, initialTurn.itemStates().get("DOC-B"));

        // 2. Stream signal only to Doc A!
        CompletableFuture<BatchTurnResult<BatchContext, DocumentContext, DocumentState, String>> futureA =
                executor.sendItemSignal("BATCH-707", "DOC-A", "ApproveDocumentCommand", new ApproveDocumentCommand("BATCH-707", "DOC-A", "Passed OCR check"));
        BatchTurnResult<BatchContext, DocumentContext, DocumentState, String> turnAfterA = futureA.get();

        assertTrue(turnAfterA.isSuspended());
        // Doc A reached the barrier, Doc B is still suspended at review
        assertEquals(DocumentState.AGGREGATION_BARRIER, turnAfterA.itemStates().get("DOC-A"));
        assertEquals(DocumentState.AWAIT_HUMAN_REVIEW, turnAfterA.itemStates().get("DOC-B"));
        assertTrue(turnAfterA.itemContexts().get("DOC-A").humanApproved);
        assertFalse(turnAfterA.itemContexts().get("DOC-B").humanApproved);

        // 3. Stream signal to Doc B -> Barrier releases!
        CompletableFuture<BatchTurnResult<BatchContext, DocumentContext, DocumentState, String>> futureB =
                executor.sendItemSignal("BATCH-707", "DOC-B", "ApproveDocumentCommand", new ApproveDocumentCommand("BATCH-707", "DOC-B", "Signed off"));
        BatchTurnResult<BatchContext, DocumentContext, DocumentState, String> finalTurn = futureB.get();

        assertTrue(finalTurn.isCompleted());
        assertEquals(DocumentState.COMPLETED, finalTurn.itemStates().get("DOC-A"));
        assertEquals(DocumentState.COMPLETED, finalTurn.itemStates().get("DOC-B"));
        assertEquals("Batch BATCH-707 completed", finalTurn.output());

        executor.close();
    }

    @Test
    public void testSingularItemAndCommandExecution() throws Exception {
        try (BatchOrchestrationExecutor<BatchContext, DocumentContext, DocumentState, String> executor = createSingularWorkflow()) {
            BatchContext batchCtx = new BatchContext();
            batchCtx.batchId = "SOLO-1";

            DocumentContext soloDoc = new DocumentContext();
            soloDoc.batchId = "SOLO-1";
            soloDoc.documentId = "DOC-SOLO";

            // 1. Singular item dispatch
            BatchTurnResult<BatchContext, DocumentContext, DocumentState, String> initialTurn =
                    executor.dispatchSync(batchCtx, soloDoc);

            assertTrue(initialTurn.isSuspended());
            assertEquals(DocumentState.AWAIT_HUMAN_REVIEW, initialTurn.itemStates().get("DOC-SOLO"));

            // 2. Singular command handle
            CompletableFuture<BatchTurnResult<BatchContext, DocumentContext, DocumentState, String>> future =
                    executor.handleCommand(new ApproveDocumentCommand("SOLO-1", "DOC-SOLO", "Approved instantly"));
            BatchTurnResult<BatchContext, DocumentContext, DocumentState, String> finalTurn = future.get();

            assertTrue(finalTurn.isCompleted());
            assertTrue(finalTurn.itemContexts().get("DOC-SOLO").humanApproved);
            assertEquals("Approved instantly", finalTurn.itemContexts().get("DOC-SOLO").reviewerNote);
        }
    }

    @Test
    public void testBatchSignalReceiverRouting() throws Exception {
        try (BatchOrchestrationExecutor<BatchContext, DocumentContext, DocumentState, String> executor = createSingularWorkflow()) {
            BatchContext batchCtx = new BatchContext();
            batchCtx.batchId = "SOLO-RCV";

            DocumentContext doc = new DocumentContext();
            doc.batchId = "SOLO-RCV";
            doc.documentId = "DOC-RCV";

            BatchTurnResult<BatchContext, DocumentContext, DocumentState, String> initialTurn =
                    executor.dispatchSync(batchCtx, doc);
            assertTrue(initialTurn.isSuspended());

            BatchSignalReceiver receiver = BatchSignalReceiver.forExecutor(executor);
            SignalMessage message = new SignalMessage(
                    "documents.inbound",
                    "ApproveDocumentCommand",
                    "SOLO-RCV",
                    UUID.randomUUID(),
                    Instant.now(),
                    Map.of("itemKey", "DOC-RCV"),
                    new ApproveDocumentCommand("SOLO-RCV", "DOC-RCV", "Approved via BatchSignalReceiver")
            );

            Object rawResult = receiver.onMessage(message).get();
            assertInstanceOf(BatchTurnResult.class, rawResult);
            @SuppressWarnings("unchecked")
            BatchTurnResult<BatchContext, DocumentContext, DocumentState, String> result =
                    (BatchTurnResult<BatchContext, DocumentContext, DocumentState, String>) rawResult;

            assertTrue(result.isCompleted());
            DocumentContext rcvDoc = result.itemContexts().get("DOC-RCV");
            assertNotNull(rcvDoc);
            assertTrue(rcvDoc.humanApproved);
            assertEquals("Approved via BatchSignalReceiver", rcvDoc.reviewerNote);
        }
    }

    private BatchOrchestrationExecutor<BatchContext, DocumentContext, DocumentState, String> createSingularWorkflow() {
        return BatchOrchestrationStateMachineBuilder.<BatchContext, DocumentContext, DocumentState, String>create("SingularDocWorkflow", DocumentState.class)
                .batchContext(BatchContext::new)
                .batchKey(ctx -> ctx.batchId)
                .itemKey(ctx -> ctx.documentId)
                .initialState(DocumentState.UPLOAD)
                .itemState(DocumentState.UPLOAD)
                    .action(ctx -> {
                        ctx.itemLog.add("UPLOAD");
                        return ctx;
                    })
                    .transition(DocumentState.OCR_EXTRACT)
                .itemState(DocumentState.OCR_EXTRACT)
                    .action(ctx -> {
                        ctx.ocrCompleted = true;
                        return ctx;
                    })
                    .transition(DocumentState.AWAIT_HUMAN_REVIEW)
                .itemState(DocumentState.AWAIT_HUMAN_REVIEW)
                    .waitForCommand(ApproveDocumentCommand.class, (ctx, cmd) -> {
                        if (cmd != null) {
                            ctx.humanApproved = true;
                            ctx.reviewerNote = cmd.note();
                        }
                        return ctx;
                    })
                    .transition(DocumentState.COMPLETED)
                .endStates(DocumentState.COMPLETED)
                .output(_ -> "Processed document")
                .buildExecutor();
    }
}
