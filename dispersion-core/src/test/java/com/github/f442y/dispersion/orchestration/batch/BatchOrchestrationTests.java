package com.github.f442y.dispersion.orchestration.batch;

import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.orchestration.command.ItemSignalCommand;
import com.github.f442y.dispersion.state.StateKey;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
        public int totalDocuments;
        public boolean batchFinalized = false;
        public List<String> batchLog = new ArrayList<>();
    }

    public static class DocumentContext implements StateMachineContext {
        public String batchId;
        public String documentId;
        public boolean ocrCompleted = false;
        public boolean humanApproved = false;
        public String reviewerNote;
        public List<String> itemLog = new ArrayList<>();
    }

    public record ApproveDocumentCommand(String batchId, String documentId, String note) implements ItemSignalCommand {
        @Override
        public String batchKey() {
            return batchId;
        }

        @Override
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
        var executor = BatchOrchestrationStateMachineBuilder.<BatchContext, DocumentContext, DocumentState, String>create("DocumentBatchJob", DocumentState.class)
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
                        ctx.humanApproved = true;
                        ctx.reviewerNote = cmd.note();
                        ctx.itemLog.add("APPROVED:" + cmd.note());
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
        assertEquals(DocumentState.AWAIT_HUMAN_REVIEW, initialTurn.itemStates().get("DOC-A"));
        assertEquals(DocumentState.AWAIT_HUMAN_REVIEW, initialTurn.itemStates().get("DOC-B"));
        assertTrue(initialTurn.itemContexts().get("DOC-A").ocrCompleted);
        assertFalse(initialTurn.itemContexts().get("DOC-A").humanApproved);

        // 2. Deliver signal ONLY for DOC-A: DOC-A must wake up, ingest signal, and advance to AGGREGATION_BARRIER
        CompletableFuture<BatchTurnResult<BatchContext, DocumentContext, DocumentState, String>> futureA =
                executor.handleCommand(new ApproveDocumentCommand("BATCH-707", "DOC-A", "Good quality scan"));

        BatchTurnResult<BatchContext, DocumentContext, DocumentState, String> turnAfterA = futureA.get();

        // DOC-A moved to barrier, but DOC-B is still waiting for review!
        assertTrue(turnAfterA.isSuspended());
        assertEquals(DocumentState.AGGREGATION_BARRIER, turnAfterA.itemStates().get("DOC-A"));
        assertEquals(DocumentState.AWAIT_HUMAN_REVIEW, turnAfterA.itemStates().get("DOC-B"));
        assertTrue(turnAfterA.itemContexts().get("DOC-A").humanApproved);
        assertEquals("Good quality scan", turnAfterA.itemContexts().get("DOC-A").reviewerNote);
        assertFalse(turnAfterA.itemContexts().get("DOC-B").humanApproved);

        // 3. Deliver signal for DOC-B: DOC-B reaches barrier, satisfying ALL_ITEMS_ARRIVED, and whole batch completes!
        CompletableFuture<BatchTurnResult<BatchContext, DocumentContext, DocumentState, String>> futureB =
                executor.handleCommand(new ApproveDocumentCommand("BATCH-707", "DOC-B", "Signed correctly"));

        BatchTurnResult<BatchContext, DocumentContext, DocumentState, String> turnAfterB = futureB.get();

        assertTrue(turnAfterB.isCompleted());
        assertEquals(DocumentState.COMPLETED, turnAfterB.itemStates().get("DOC-A"));
        assertEquals(DocumentState.COMPLETED, turnAfterB.itemStates().get("DOC-B"));
        assertTrue(turnAfterB.itemContexts().get("DOC-B").humanApproved);

        executor.close();
    }

    /**
     * Tests singular item execution using the same workflow definition.
     */
    @Test
    public void testSingularItemAndSingularCommandExecution() throws Exception {
        var executor = BatchOrchestrationStateMachineBuilder.<BatchContext, DocumentContext, DocumentState, String>create("SingleDocPipeline", DocumentState.class)
                .batchContext(BatchContext::new)
                .batchKey(ctx -> ctx.batchId)
                .itemKey(ctx -> ctx.documentId)
                .initialState(DocumentState.UPLOAD)
                .itemState(DocumentState.UPLOAD)
                    .action(ctx -> ctx)
                    .transition(DocumentState.OCR_EXTRACT)
                .itemState(DocumentState.OCR_EXTRACT)
                    .action(ctx -> {
                        ctx.ocrCompleted = true;
                        return ctx;
                    })
                    .transition(DocumentState.AWAIT_HUMAN_REVIEW)
                .itemState(DocumentState.AWAIT_HUMAN_REVIEW)
                    .waitForCommand(ApproveDocumentCommand.class, (ctx, cmd) -> {
                        ctx.humanApproved = true;
                        ctx.reviewerNote = cmd.note();
                        return ctx;
                    })
                    .transition(DocumentState.COMPLETED)
                .endStates(DocumentState.COMPLETED)
                .output(ctx -> "Processed document")
                .buildExecutor();

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
        var future = executor.handleCommand(new ApproveDocumentCommand("SOLO-1", "DOC-SOLO", "Approved instantly"));
        BatchTurnResult<BatchContext, DocumentContext, DocumentState, String> finalTurn = future.get();

        assertTrue(finalTurn.isCompleted());
        assertTrue(finalTurn.itemContexts().get("DOC-SOLO").humanApproved);
        assertEquals("Approved instantly", finalTurn.itemContexts().get("DOC-SOLO").reviewerNote);

        executor.close();
    }
}
