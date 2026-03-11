package com.example.replay.actors;

import com.example.replay.datalake.DataLakeReader;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Processes a single {@link WorkPacket}: reads all events in its time window
 * from the data lake (batch by batch) and counts them as emitted.
 *
 * <p>Each worker is assigned exactly one packet by {@link WorkerPoolActor} via
 * an {@link Messages.PacketWorkerCommand.Assign} message.  When the packet is
 * fully read it reports {@link Messages.WorkerPoolCommand.PacketDone} (or
 * {@link Messages.WorkerPoolCommand.PacketFailed} on error) to its pool and
 * stops itself.
 *
 * <p>All Iceberg / Parquet I/O is dispatched on a virtual-thread executor via
 * {@code pipeToSelf}, keeping the actor dispatcher non-blocking.
 *
 * <h3>State machine</h3>
 * <pre>
 *   IDLE      ──Assign──────────────→ dispatch read → FETCHING
 *   FETCHING  ──BatchReady (ok)─────→ emit; dispatch next → FETCHING
 *   FETCHING  ──BatchReady (empty)──→ PacketDone → stopped
 *   FETCHING  ──BatchReady (error)──→ PacketFailed → stopped
 *   FETCHING  ──Pause───────────────→ set flag; complete current batch → PAUSED
 *   FETCHING  ──Cancel──────────────→ stopped
 *   PAUSED    ──Resume──────────────→ dispatch read → FETCHING
 *   PAUSED    ──Cancel──────────────→ stopped
 * </pre>
 */
public final class PacketWorkerActor extends AbstractBehavior<Messages.PacketWorkerCommand> {

    /** Reuse the same batch size as DataReaderActor for consistency. */
    private static final int BATCH_SIZE = DataReaderActor.BATCH_SIZE;

    /** Virtual-thread executor — cheap, one per batch call. */
    private static final Executor IO_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

    private static final Logger log = LoggerFactory.getLogger(PacketWorkerActor.class);

    private final DataLakeReader                           reader;
    private final ActorRef<Messages.WorkerPoolCommand>     pool;

    private WorkPacket packet;
    private int        batchIndex     = 0;
    private long       emittedTotal   = 0L;
    private boolean    pauseRequested = false;

    // -----------------------------------------------------------------------
    // Factory
    // -----------------------------------------------------------------------

    public static Behavior<Messages.PacketWorkerCommand> create(
            DataLakeReader reader,
            ActorRef<Messages.WorkerPoolCommand> pool) {
        return Behaviors.setup(ctx -> new PacketWorkerActor(ctx, reader, pool));
    }

    private PacketWorkerActor(ActorContext<Messages.PacketWorkerCommand> ctx,
                               DataLakeReader reader,
                               ActorRef<Messages.WorkerPoolCommand> pool) {
        super(ctx);
        this.reader = reader;
        this.pool   = pool;
    }

    // -----------------------------------------------------------------------
    // Behaviors
    // -----------------------------------------------------------------------

    @Override
    public Receive<Messages.PacketWorkerCommand> createReceive() {
        return idle();
    }

    private Receive<Messages.PacketWorkerCommand> idle() {
        return newReceiveBuilder()
                .onMessage(Messages.PacketWorkerCommand.Assign.class,  this::onAssign)
                .onMessage(Messages.PacketWorkerCommand.Cancel.class,  msg -> Behaviors.stopped())
                .build();
    }

    /** Reading in-flight via pipeToSelf. */
    private Receive<Messages.PacketWorkerCommand> fetching() {
        return newReceiveBuilder()
                .onMessage(Messages.PacketWorkerCommand.BatchReady.class, this::onBatchReady)
                .onMessage(Messages.PacketWorkerCommand.Pause.class,
                        msg -> { pauseRequested = true;  return fetching(); })
                // Resume can arrive before BatchReady (pool sends Pause then Resume quickly).
                // Clearing the flag here means onBatchReady will not enter waitingResume.
                .onMessage(Messages.PacketWorkerCommand.Resume.class,
                        msg -> { pauseRequested = false; return fetching(); })
                .onMessage(Messages.PacketWorkerCommand.Cancel.class,     this::onCancel)
                .build();
    }

    /** Paused — no I/O in-flight; waiting for Resume or Cancel. */
    private Receive<Messages.PacketWorkerCommand> waitingResume() {
        return newReceiveBuilder()
                .onMessage(Messages.PacketWorkerCommand.Resume.class,  this::onResume)
                .onMessage(Messages.PacketWorkerCommand.Cancel.class,  this::onCancel)
                .build();
    }

    // -----------------------------------------------------------------------
    // Handlers
    // -----------------------------------------------------------------------

    private Behavior<Messages.PacketWorkerCommand> onAssign(Messages.PacketWorkerCommand.Assign msg) {
        this.packet = msg.packet();
        log.debug("Worker assigned {} (suggestedWorker={})", packet, packet.suggestedWorker());
        dispatchFetch();
        return fetching();
    }

    private Behavior<Messages.PacketWorkerCommand> onBatchReady(Messages.PacketWorkerCommand.BatchReady msg) {
        if (msg.error() != null) {
            log.error("Batch read failed for packet {}: {}", packet.packetId(), msg.error().getMessage());
            pool.tell(new Messages.WorkerPoolCommand.PacketFailed(
                    packet.packetId(), msg.error().getMessage(), getContext().getSelf()));
            return Behaviors.stopped();
        }

        if (msg.events().isEmpty()) {
            log.debug("Packet {} complete — {} events emitted in {} batches",
                    packet.packetId().substring(0, 8), emittedTotal, batchIndex);
            pool.tell(new Messages.WorkerPoolCommand.PacketDone(
                    packet.packetId(), emittedTotal, getContext().getSelf()));
            return Behaviors.stopped();
        }

        // Count as "emitted" — in production replace with real Kafka publish
        emittedTotal += msg.events().size();
        batchIndex++;

        if (pauseRequested) {
            pauseRequested = false;
            log.debug("Worker paused at batchIndex={} for packet {}", batchIndex, packet.packetId().substring(0, 8));
            return waitingResume();
        }

        dispatchFetch();
        return fetching();
    }

    private Behavior<Messages.PacketWorkerCommand> onResume(Messages.PacketWorkerCommand.Resume msg) {
        log.debug("Worker resuming from batchIndex={} for packet {}", batchIndex, packet.packetId().substring(0, 8));
        dispatchFetch();
        return fetching();
    }

    private Behavior<Messages.PacketWorkerCommand> onCancel(Messages.PacketWorkerCommand.Cancel msg) {
        log.debug("Worker cancelled for packet {}", packet != null ? packet.packetId().substring(0, 8) : "?");
        return Behaviors.stopped();
    }

    // -----------------------------------------------------------------------
    // Helper
    // -----------------------------------------------------------------------

    private void dispatchFetch() {
        int idx = batchIndex;
        getContext().pipeToSelf(
                CompletableFuture.supplyAsync(
                        () -> reader.readBatch(
                                packet.tableLocation(), packet.from(), packet.to(),
                                idx, BATCH_SIZE),
                        IO_EXECUTOR),
                (events, err) -> new Messages.PacketWorkerCommand.BatchReady(events, idx, err));
    }
}
