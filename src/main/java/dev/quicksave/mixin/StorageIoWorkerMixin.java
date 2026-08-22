package dev.quicksave.mixin;

import com.llamalad7.mixinextras.injector.wrapmethod.WrapMethod;
import com.llamalad7.mixinextras.injector.wrapoperation.Operation;
import dev.quicksave.config.QuickSaveConfig;
import dev.quicksave.io.QuickSavePipeline;
import net.minecraft.nbt.NbtCompound;
import net.minecraft.util.math.ChunkPos;
import net.minecraft.world.storage.StorageIoWorker;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Unique;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

/**
 * Intercepts vanilla chunk saving at its single choke point:
 * {@link StorageIoWorker#setResult(ChunkPos, NbtCompound)}.
 *
 * <p>This is where every serialized chunk (from
 * {@code ThreadedAnvilChunkStorage} → {@code ChunkHolder} → {@code ChunkCache})
 * enters the unbounded pending-write map that causes OOM crashes during mass
 * pre-generation. We divert it into a bounded, backpressured QuickSave pipeline,
 * whose writer threads re-enter the ORIGINAL method body later.</p>
 */
@Mixin(StorageIoWorker.class)
public abstract class StorageIoWorkerMixin {

    @Unique
    private static final Logger QUICKSAVE$LOGGER = LoggerFactory.getLogger("QuickSave/Mixin");

    /**
     * Per-worker pipeline. Null when this worker is NOT captured.
     * Written once in the constructor (safe publication: mixin-injected state on
     * a freshly constructed object before it escapes) and only read afterwards.
     */
    @Unique
    private QuickSavePipeline quicksave$pipeline;

    /**
     * Worker construction. The third parameter is the human name string vanilla
     * passes: "chunk", "entity" or "poi". We capture ONLY the names listed in
     * the config (default: just "chunk") so entity/POI storage behavior remains
     * 100% vanilla.
     */
    @Inject(method = "<init>(Ljava/nio/file/Path;ZLjava/lang/String;)V", at = @At("TAIL"))
    private void quicksave$onConstruct(Path directory, boolean dsync, String name, CallbackInfo ci) {
        if (QuickSaveConfig.get().capturedWorkerNames.contains(name)) {
            this.quicksave$pipeline = QuickSavePipeline.create((StorageIoWorker) (Object) this, name);
        }
    }

    /**
     * THE hijack point.
     *
     * <p>{@code setResult(ChunkPos, NbtCompound)} receives an already-built,
     * immutable-by-contract NBT snapshot of the chunk. Instead of letting it
     * pile up in vanilla's unbounded internal map, we hand it to our pipeline;
     * one of our dedicated writer threads later invokes
     * {@code original.call(...)} — i.e. the untouched vanilla body — so all real
     * disk work still flows through Mojang's RegionBasedStorage / RegionFile
     * code. Nothing about the on-disk format or write logic is changed; only
     * WHEN and HOW MANY writes are in flight changes.</p>
     *
     * <p><b>Data isolation:</b> the tag is fully materialized BEFORE this method
     * returns (built by the world-thread serializer). After enqueueing, neither
     * we nor vanilla mutate it — the same opaque-snapshot contract vanilla
     * already relies upon. No live Chunk objects ever cross threads here, so no
     * lock on world state is required.</p>
     *
     * <p><b>Null tags</b> mean "delete this chunk" — rare, cheap, ordering-
     * sensitive. They pass straight through to vanilla without queueing.</p>
     *
     * <p><b>Re-entrancy:</b> calls made from our own writer threads bypass the
     * pipeline entirely (otherwise a drainer could block against itself).</p>
     */
    @WrapMethod(
            method = "setResult(Lnet/minecraft/util/math/ChunkPos;Lnet/minecraft/nbt/NbtCompound;)Ljava/util/concurrent/CompletableFuture;"
    )
    private CompletableFuture<Void> quicksave$wrapSetResult(
            ChunkPos pos, @Nullable NbtCompound nbt, Operation<CompletableFuture<Void>> original) {
        final QuickSavePipeline pipeline = this.quicksave$pipeline;
        if (pipeline == null || nbt == null || QuickSavePipeline.isOnWriterThread()) {
            return original.call(pos, nbt);
        }
        // The dispatcher reads entry.tag at EXECUTION time (it may have been
        // coalesced to a newer snapshot while queued) — never the stale local.
        return pipeline.submit(pos, nbt, entry -> original.call(entry.pos, entry.tag));
    }

    /**
     * Integrity gate #2: whenever vanilla asks the worker to complete ALL
     * pending work ("Saving chunks…" screen, world unload, autosave sync), our
     * queue must drain FIRST so that ordering guarantees hold and the returned
     * future truly means "everything is on disk".
     */
    @Inject(method = "completeAll(Z)Ljava/util/concurrent/CompletableFuture;", at = @At("HEAD"))
    private void quicksave$beforeCompleteAll(boolean sync, CallbackInfoReturnable<CompletableFuture<Void>> cir) {
        QuickSavePipeline pipeline = this.quicksave$pipeline;
        if (pipeline != null) {
            pipeline.flushBlocking("vanilla completeAll");
        }
    }

    /**
     * Integrity gate #3: the worker cannot be closed underneath us. We flush,
     * then dispose (which removes the crash shutdown hook), and only then let
     * vanilla close the underlying region files.
     */
    @Inject(method = "close()V", at = @At("HEAD"))
    private void quicksave$beforeClose(CallbackInfo ci) throws IOException {
        QuickSavePipeline pipeline = this.quicksave$pipeline;
        if (pipeline != null) {
            pipeline.flushBlocking("vanilla close");
            pipeline.dispose();
        }
    }
}
