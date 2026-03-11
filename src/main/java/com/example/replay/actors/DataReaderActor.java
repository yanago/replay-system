package com.example.replay.actors;

import com.example.replay.datalake.DataLakeReader;
import com.example.replay.model.ReplayJob;
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
 * Reads event batches from the data lake and delivers them to the parent
 * {@link ReplayJobActor}.
 *
 * <p>Each batch is fetched asynchronously via
 * {@code CompletableFuture.supplyAsync(reader::readBatch, IO_EXECUTOR)} and
 * piped back to this actor as {@link Messages.DataReaderCommand.BatchReady}.
 * The actor never blocks its dispatcher thread.
 *
 * <p>State machine:
 * <pre>
 *   IDLE     ──Start──────────────→ dispatch fetch → FETCHING
 *   FETCHING ──BatchReady (ok)────→ tell parent BatchRead; dispatch next → FETCHING
 *   FETCHING ──BatchReady (empty)─→ tell parent ReaderDone → stopped
 *   FETCHING ──BatchReady (error)─→ tell parent BatchFailed → stopped
 *   FETCHING ──Pause──────────────→ set pauseRequested=true, stay in FETCHING
 *   FETCHING ──Cancel─────────────→ stopped
 *   PAUSED   ──Resume─────────────→ dispatch fetch → FETCHING
 *   PAUSED   ──Cancel─────────────→ stopped
 * </pre>
 *
 * <p>Note: when Cancel arrives while a fetch is in-flight, the actor stops
 * immediately.  The in-flight {@code CompletableFuture} may still complete, but
 * its {@code BatchReady} result will be dead-lettered (actor already stopped).
 */
public final class DataReaderActor extends AbstractBehavior<Messages.DataReaderCommand> {

    /** Events requested per batch from the data lake. */
    static final int BATCH_SIZE = 500;

    /**
     * Dedicated virtual-thread executor for blocking Iceberg / Parquet I/O.
     * Virtual threads are cheap; one per batch call is fine.
     */
    private static final Executor IO_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

    private static final Logger log = LoggerFactory.getLogger(DataReaderActor.class);

    private final ActorRef<Messages.ReplayJobCommand> parent;
    private final ReplayJob                           job;
    private final DataLakeReader                      reader;

    private int     batchIndex     = 0;
    private long    batchSeq       = 0;
    private boolean pauseRequested = false;

    // -----------------------------------------------------------------------
    // Factory
    // -----------------------------------------------------------------------

    public static Behavior<Messages.DataReaderCommand> create(
            ActorRef<Messages.ReplayJobCommand> parent,
            ReplayJob job,
            DataLakeReader reader) {
        return Behaviors.setup(ctx -> new DataReaderActor(ctx, parent, job, reader));
    }

    private DataReaderActor(ActorContext<Messages.DataReaderCommand> ctx,
                             ActorRef<Messages.ReplayJobCommand> parent,
                             ReplayJob job,
                             DataLakeReader reader) {
        super(ctx);
        this.parent = parent;
        this.job    = job;
        this.reader = reader;
    }

    // -----------------------------------------------------------------------
    // Behaviors
    // -----------------------------------------------------------------------

    @Override
    public Receive<Messages.DataReaderCommand> createReceive() {
        return idle();
    }

    private Receive<Messages.DataReaderCommand> idle() {
        return newReceiveBuilder()
                .onMessage(Messages.DataReaderCommand.Start.class,  this::onStart)
                .onMessage(Messages.DataReaderCommand.Cancel.class, msg -> Behaviors.stopped())
                .build();
    }

    /** In-flight I/O; only BatchReady, Pause, and Cancel are meaningful. */
    private Receive<Messages.DataReaderCommand> fetching() {
        return newReceiveBuilder()
                .onMessage(Messages.DataReaderCommand.BatchReady.class, this::onBatchReady)
                .onMessage(Messages.DataReaderCommand.Pause.class,
                        msg -> { pauseRequested = true; return fetching(); })
                .onMessage(Messages.DataReaderCommand.Cancel.class, this::onCancel)
                .build();
    }

    /** No I/O in-flight; waiting for Resume or Cancel. */
    private Receive<Messages.DataReaderCommand> waitingResume() {
        return newReceiveBuilder()
                .onMessage(Messages.DataReaderCommand.Resume.class, this::onResume)
                .onMessage(Messages.DataReaderCommand.Cancel.class, this::onCancel)
                .build();
    }

    // -----------------------------------------------------------------------
    // Handlers
    // -----------------------------------------------------------------------

    private Behavior<Messages.DataReaderCommand> onStart(Messages.DataReaderCommand.Start msg) {
        log.debug("DataReaderActor started for job {}", job.jobId());
        dispatchFetch();
        return fetching();
    }

    private Behavior<Messages.DataReaderCommand> onBatchReady(Messages.DataReaderCommand.BatchReady msg) {
        if (msg.error() != null) {
            log.error("Batch read failed for job {}: {}", job.jobId(), msg.error().getMessage());
            parent.tell(new Messages.ReplayJobCommand.BatchFailed(msg.seq(), msg.error().getMessage()));
            return Behaviors.stopped();
        }
        if (msg.events().isEmpty()) {
            log.debug("DataReaderActor done — job {} sent {} batches", job.jobId(), batchSeq);
            parent.tell(new Messages.ReplayJobCommand.ReaderDone(batchSeq));
            return Behaviors.stopped();
        }

        parent.tell(new Messages.ReplayJobCommand.BatchRead(msg.events(), msg.seq()));
        batchIndex++;
        batchSeq++;

        if (pauseRequested) {
            pauseRequested = false;
            log.debug("DataReaderActor paused at batchIndex={} for job {}", batchIndex, job.jobId());
            return waitingResume();
        }

        dispatchFetch();
        return fetching();
    }

    private Behavior<Messages.DataReaderCommand> onResume(Messages.DataReaderCommand.Resume msg) {
        log.debug("DataReaderActor resuming from batchIndex={} for job {}", batchIndex, job.jobId());
        dispatchFetch();
        return fetching();
    }

    private Behavior<Messages.DataReaderCommand> onCancel(Messages.DataReaderCommand.Cancel msg) {
        log.debug("DataReaderActor cancelled for job {}", job.jobId());
        return Behaviors.stopped();
    }

    // -----------------------------------------------------------------------
    // Helper
    // -----------------------------------------------------------------------

    private void dispatchFetch() {
        int  idx = batchIndex;
        long seq = batchSeq;
        getContext().pipeToSelf(
                CompletableFuture.supplyAsync(
                        () -> reader.readBatch(
                                job.sourceTable(), job.fromTime(), job.toTime(), idx, BATCH_SIZE),
                        IO_EXECUTOR),
                (events, err) -> new Messages.DataReaderCommand.BatchReady(events, seq, err));
    }
}
