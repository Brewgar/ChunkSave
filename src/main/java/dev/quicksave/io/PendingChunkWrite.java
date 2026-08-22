package dev.quicksave.io;

import net.minecraft.nbt.NbtCompound;
import net.minecraft.util.math.ChunkPos;

import java.util.concurrent.CompletableFuture;

/**
 * A single pending chunk-write work item.
 *
 * <p><b>GC strategy (zero-allocation hot path):</b> instances are pooled and
 * recycled through {@link dev.quicksave.io.QuickSavePipeline} after their write
 * completes. The only unavoidable per-save allocation is the
 * {@link CompletableFuture} (it can only be completed once) and the tiny
 * dispatcher lambda created by the mixin. Both are dwarfed by the
 * multi-kilobyte NBT compound vanilla allocates per save anyway.</p>
 *
 * <p><b>Threading:</b> all fields except {@link #tag} are written exactly once
 * by the producer before the entry becomes visible to a consumer, and cleared
 * on recycle. {@link #tag} is {@code volatile} because the coalescing fast path
 * swaps it from producer threads while a consumer may be about to read it — see
 * the memory-visibility proof in {@code QuickSavePipeline}.</p>
 *
 * <p>This class is {@code public} only because the Mixin class lives in a
 * different package; treat all fields as pipeline-internal.</p>
 */
public final class PendingChunkWrite {

    /** Hash-map key ({@link ChunkPos#toLong()}) used for coalescing. */
    public long key;

    /** Immutable position; safe to share across threads. */
    public ChunkPos pos;

    /**
     * The serialized chunk data snapshot. Volatile: producers may replace this
     * with a newer snapshot while the entry is still queued (coalescing);
     * volatile guarantees the writer thread observes the latest swap.
     */
    public volatile NbtCompound tag;

    /** Callback into the ORIGINAL (unwrapped) vanilla {@code setResult} body. */
    public QuickSavePipeline.Dispatcher dispatcher;

    /** Completed when the data has physically passed through the vanilla sink. */
    public final CompletableFuture<Void> future = new CompletableFuture<>();
}
