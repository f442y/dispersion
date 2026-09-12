package com.github.f442y.dispersion.orchestration.command;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Signal command targeting a specific item within a Set/Batch Orchestration.
 */
public interface ItemSignalCommand extends SignalCommand {

    /**
     * Domain key identifying the batch collection.
     *
     * @return The batch key
     */
    @NonNull
    String batchKey();

    /**
     * Domain key identifying the specific item within the batch.
     *
     * @return The item key
     */
    @NonNull
    String itemKey();

    @Override
    @Nullable
    default String correlationKey() {
        return batchKey();
    }
}
