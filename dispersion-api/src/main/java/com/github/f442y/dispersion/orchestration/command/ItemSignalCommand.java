package com.github.f442y.dispersion.orchestration.command;

import org.jspecify.annotations.NonNull;

/**
 * Signal command targeting a specific item unit inside a batch orchestration.
 */
public interface ItemSignalCommand extends SignalCommand {

    /**
     * The parent batch correlation key (e.g. orderId, batchJobId).
     */
    @NonNull
    String batchKey();

    /**
     * The individual unit identifier within the batch (e.g. lineItemId, sku, documentId).
     */
    @NonNull
    String itemKey();

    @Override
    @NonNull
    default String correlationKey() {
        return batchKey();
    }
}
