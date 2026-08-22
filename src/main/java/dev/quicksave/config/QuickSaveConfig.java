package dev.quicksave.config;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import dev.quicksave.QuickSaveMod;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Simple Gson-backed config stored at {@code config/quicksave.json}.
 *
 * <p>No third-party config library is used on purpose: Gson is already bundled
 * with Minecraft, keeping the mod's dependency surface at Fabric API only.</p>
 */
public final class QuickSaveConfig {

    private static final Path CONFIG_PATH = Path.of("config", "quicksave.json");
    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

    private static QuickSaveConfig instance;

    // ---------------------------------------------------------------------
    // Tunables (all fields are deserialized from JSON)
    // ---------------------------------------------------------------------

    /**
     * Number of dedicated writer threads (== number of stripes).
     * {@code 0} means auto: {@code clamp(availableProcessors / 4, 2, 6)}.
     *
     * <p>Rationale: NVMe devices saturate around queue-depth 4-8. More threads
     * than that only add context-switch overhead and steal CPU from the server
     * thread, which is exactly what we must NOT starve during pre-generation.</p>
     */
    public int ioThreads = 0;

    /**
     * Bounded capacity of each stripe's queue. The HARD backpressure limit is
     * {@code ioThreads * stripeQueueCapacity}; producers (usually the server
     * thread) block once it is reached, which naturally throttles generation.
     */
    public int stripeQueueCapacity = 8192;

    /**
     * Soft threshold: once the total number of pending writes across all
     * stripes exceeds this, QuickSave emits aggressive (rate-limited) ERROR
     * logs so the user knows the SSD is falling behind BEFORE hard blocking
     * starts.
     */
    public int softWarningThreshold = 24000;

    /**
     * Time budget (milliseconds) for the JVM shutdown-hook emergency flush.
     * On a crash, we try to drain everything within this budget before the
     * JVM dies. Note: {@code SIGKILL} / power loss cannot be intercepted by
     * any software (this is equally true of vanilla).
     */
    public int shutdownFlushBudgetMs = 30_000;

    /**
     * Which {@code StorageIoWorker}s are hijacked, matched against the name
     * string passed to the worker's constructor. Vanilla constructs them with
     * {@code "chunk"}, {@code "entity"} and {@code "poi"}. We deliberately
     * capture ONLY the chunk worker by default: entity and POI writes are
     * low-volume and hijacking them would change behavior for systems we do
     * not need to touch.
     */
    public List<String> capturedWorkerNames = List.of("chunk");

    // ---------------------------------------------------------------------
    // Access / persistence
    // ---------------------------------------------------------------------

    public static QuickSaveConfig get() {
        if (instance == null) {
            load();
        }
        return instance;
    }

    public static synchronized void load() {
        if (instance != null) {
            return;
        }
        if (Files.exists(CONFIG_PATH)) {
            try {
                instance = GSON.fromJson(Files.readString(CONFIG_PATH), QuickSaveConfig.class);
            } catch (Exception e) {
                QuickSaveMod.LOGGER.error("[QuickSave] Failed to read config, using defaults.", e);
            }
        }
        if (instance == null) {
            instance = new QuickSaveConfig();
        }
        instance.sanitize();
        try {
            Files.createDirectories(CONFIG_PATH.getParent());
            Files.writeString(CONFIG_PATH, GSON.toJson(instance));
        } catch (IOException e) {
            QuickSaveMod.LOGGER.error("[QuickSave] Failed to write config file.", e);
        }
    }

    /** Clamps user input into safe ranges so a bad config can never wedge the pipeline. */
    private void sanitize() {
        ioThreads = Math.max(0, Math.min(ioThreads, 16));
        stripeQueueCapacity = Math.max(256, Math.min(stripeQueueCapacity, 1 << 20));
        softWarningThreshold = Math.max(1, softWarningThreshold);
        shutdownFlushBudgetMs = Math.max(1_000, shutdownFlushBudgetMs);
        if (capturedWorkerNames == null || capturedWorkerNames.isEmpty()) {
            capturedWorkerNames = List.of("chunk");
        }
    }

    /** Resolves the effective writer/stripe count. Guaranteed to be &ge; 1. */
    public int resolveIoThreadCount() {
        if (ioThreads > 0) {
            return ioThreads;
        }
        int cores = Runtime.getRuntime().availableProcessors();
        return Math.max(2, Math.min(6, cores / 4));
    }
}
