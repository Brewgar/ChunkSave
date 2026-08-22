package dev.quicksave;

import dev.quicksave.config.QuickSaveConfig;
import dev.quicksave.io.QuickSavePipeline;
import net.fabricmc.api.ModInitializer;
import net.fabricmc.fabric.api.event.lifecycle.v1.ServerLifecycleEvents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * QuickSave - asynchronous, backpressured chunk-save offloading for Fabric 1.20.1.
 *
 * <p>The heavy lifting lives in {@link QuickSavePipeline} (instantiated by the
 * {@code StorageIoWorker} mixin) and the mixin itself. This entrypoint only
 * wires up configuration and server lifecycle flush hooks.</p>
 */
public final class QuickSaveMod implements ModInitializer {

    public static final String MOD_ID = "quicksave";
    public static final Logger LOGGER = LoggerFactory.getLogger(MOD_ID);

    @Override
    public void onInitialize() {
        QuickSaveConfig.load();

        // ------------------------------------------------------------------
        // Graceful-shutdown integrity gate #1 (of three):
        //
        // SERVER_STOPPING fires BEFORE worlds are saved/closed. We block the
        // server thread here until every pipeline has drained its pending
        // chunk writes into the vanilla sink. Vanilla then performs its own
        // completeAll(true)/close(), whose HEAD injections wait for us again
        // (no-op if already empty). Three independent gates guarantee no
        // single missed hook can strand dirty chunk data in RAM.
        // ------------------------------------------------------------------
        ServerLifecycleEvents.SERVER_STOPPING.register(server -> {
            var pipelines = QuickSavePipeline.activePipelines();
            if (!pipelines.isEmpty()) {
                LOGGER.warn("[QuickSave] SERVER_STOPPING: flushing {} active pipeline(s) before world save...", pipelines.size());
                for (QuickSavePipeline pipeline : pipelines) {
                    pipeline.flushBlocking("server stop");
                }
                LOGGER.info("[QuickSave] All pipelines drained; handing control back to vanilla save.");
            }
        });

        LOGGER.info("[QuickSave] Initialized. Writers: {} (0=auto), stripe capacity: {}, soft warn at: {} pending writes.",
                QuickSaveConfig.get().ioThreads,
                QuickSaveConfig.get().stripeQueueCapacity,
                QuickSaveConfig.get().softWarningThreshold);
    }
}
