# ChunkSave

A Fabric 1.20.1 mod that eliminates Out-Of-Memory crashes during massive world
pre-generation (Chunky, jjthunder, etc.) by interposing a **bounded,
backpressured, multi-threaded asynchronous buffer** between vanilla chunk
serialization and disk I/O.

## The problem it solves

Vanilla deposits every serialized chunk into an unbounded pending-write map
inside `StorageIoWorker`, drained for disk writing through a narrow lane into
`RegionBasedStorage` / `RegionFile`. When generation outpaces your SSD, chunks
accumulate in RAM until the JVM dies with OOM.

## How ChunkSave works

```
Server thread ──► setResult(pos, nbt)   [wrapped via MixinExtras @WrapMethod]
                        │
                        ▼
        ┌───────────────────────────────────┐
        │ ChunkSavePipeline                  │
        │  • S striped bounded queues        │  ◄─ HARD CAP: producers block here
        │  • per-position coalescing map     │     (world-gen throttles naturally)
        │  • recycled entry objects (low GC) │
        └──────────────┬─────────────────────┘
                       ▼  S dedicated low-priority daemon writer threads
        original vanilla setResult body ──► RegionFile writes (100% vanilla logic)
```

Key properties:

* **No reimplementation of Anvil/RegionFile** — all real disk writing still runs
  Mojang's tested code. Only *when* and *how many concurrent* writes happen change.
* **Per-chunk write ordering is mathematically guaranteed** — position → stripe is
  deterministic and each stripe is strictly FIFO with one consumer thread.
* **OOM impossible by construction** — pending entries can never exceed
  `ioThreads × stripeQueueCapacity`; beyond that, generation is throttled by
  blocking backpressure (with aggressive rate-limited warnings before that).
* **Triple integrity gating on shutdown** — `SERVER_STOPPING`, worker
  `completeAll(...)`, and worker `close()` each block until everything is flushed;
  a JVM shutdown hook performs a budgeted emergency flush + forced vanilla
  synchronous drain even after hard crashes.

## Configuration — `config/ChunkSave.json`

| Key                     | Default  | Meaning                                                       |
|-------------------------|----------|---------------------------------------------------------------|
| `ioThreads`             | `0`      | Writer threads (0 = auto: `clamp(cores/4, 2, 6)`; NVMe sweet spot) |
| `stripeQueueCapacity`   | `8192`   | Per-stripe queue bound (hard cap = threads × this)             |
| `softWarningThreshold`  | `24000`  | Pending-write count that triggers aggressive ERROR warnings    |
| `shutdownFlushBudgetMs` | `30000`  | Time budget for the crash-shutdown emergency flush             |
| `capturedWorkerNames`   | `["chunk"]` | Which vanilla workers are hijacked (entity/poi left alone)  |

## Building

Requires JDK 17. Run `gradle build` (or import into IntelliJ / VS Code with the
Java + Gradle plugins). Output jar lands in `build/libs/`.

Requires Fabric Loader ≥ 0.15 (bundles MixinExtras) and Fabric API.
