package com.example.replay.actors;

import com.example.replay.datalake.DataLakeReader;
import com.example.replay.downstream.DownstreamClient;
import com.example.replay.kafka.EventPublisher;
import com.example.replay.metrics.MetricsRegistry;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Processes a single {@link WorkPacket}: reads all events in its time window
 * from the data lake (batch by batch), publishes each batch to Kafka and the
 * downstream REST API, then reports completion.
 *
 * <h3>Publishing</h3>
 * Each batch goes through two parallel delivery paths:
 * <ol>
 *   <li><b>Kafka</b> via {@link EventPublisher} — uses {@code cid} as the partition
 *       key so that events from the same customer land in the same partition.</li>
 *   <li><b>Downstream REST</b> via {@link DownstreamClient} — POSTs the batch as
 *       a JSON array to a configurable HTTP endpoint.</li>
 * </ol>
 * Both futures are combined with {@code thenCombine}; if either fails the worker
 * reports {@link Messages.WorkerPoolCommand.PacketFailed} and stops.
 *
 * <h3>Backpressure</h3>
 * The next fetch is only dispatched <em>after</em> the previous batch has been
 * fully published.  This prevents the actor from accumulating unbounded in-flight
 * data when the downstream is slower than the data lake.
 *
 * <h3>Metrics</h3>
 * After each successful read→publish cycle the actor reports to {@link MetricsRegistry}:
 * <ul>
 *   <li>Fetch latency (data-lake read time in ms)</li>
 *   <li>Publish latency (Kafka + HTTP combined time in ms)</li>
 *   <li>Events published</li>
 * </ul>
 * Read and publish errors are also counted.  All registry writes are lock-free.
 *
 * <h3>State machine</h3>
 * <pre>
 *   IDLE       ──Assign──────────────────→ dispatchFetch → FETCHING
 *   FETCHING   ──BatchReady (ok)──────────→ dispatchPublish → PUBLISHING
 *   FETCHING   ──BatchReady (empty)───────→ PacketDone → stopped
 *   FETCHING   ──BatchReady (error)───────→ PacketFailed → stopped
 *   FETCHING   ──Pause────────────────────→ set flag
 *   FETCHING   ──Resume───────────────────→ clear flag
 *   FETCHING   ──Cancel───────────────────→ stopped
 *   PUBLISHING ──BatchPublished (ok)───────→ if paused → WAITING_RESUME; else dispatchFetch → FETCHING
 *   PUBLISHING ──BatchPublished (error)────→ PacketFailed → stopped
 *   PUBLISHING ──Pause─────────────────────→ set flag
 *   PUBLISHING ──Resume────────────────────→ clear flag
 *   PUBLISHING ──Cancel────────────────────→ stopped
 *   WAITING_RESUME ──Resume───────────────→ dispatchFetch → FETCHING
 *   WAITING_RESUME ──Cancel───────────────→ stopped
 * </pre>
 */
public final class PacketWorkerActor extends AbstractBehavior<Messages.PacketWorkerCommand> {

    /** Reuse the same batch size as DataReaderActor for consistency. */
    static final int BATCH_SIZE = DataReaderActor.BATCH_SIZE;

    /** Virtual-thread executor — cheap, one per batch call. */
    private static final Executor IO_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

    private static final Logger log = LoggerFactory.getLogger(PacketWorkerActor.class);

    private final DataLakeReader                           reader;
    private final EventPublisher                           publisher;
    private final String                                   targetTopic;
    private final DownstreamClient                         downstreamClient;
    private final ActorRef<Messages.WorkerPoolCommand>     pool;
    private final String                                   jobId;
    private final MetricsRegistry                          registry;

    private WorkPacket packet;
    private int        batchIndex     = 0;
    private long       emittedTotal   = 0L;
    private boolean    pauseRequested = false;

    /** Nanosecond timestamp set just before each async fetch is dispatched. */
    private long fetchStartNs;
    /** Nanosecond timestamp set just before each async publish is dispatched. */
    private long publishStartNs;

    // -----------------------------------------------------------------------
    // Factory
    // -----------------------------------------------------------------------

    public static Behavior<Messages.PacketWorkerCommand> create(
            DataLakeReader reader,
            EventPublisher publisher,
            String targetTopic,
            DownstreamClient downstreamClient,
            ActorRef<Messages.WorkerPoolCommand> pool,
            String jobId,
            MetricsRegistry registry) {
        return Behaviors.setup(ctx ->
                new PacketWorkerActor(ctx, reader, publisher, targetTopic, downstreamClient, pool, jobId, registry));
    }

    private PacketWorkerActor(ActorContext<Messages.PacketWorkerCommand> ctx,
                               DataLakeReader reader,
                               EventPublisher publisher,
                               String targetTopic,
                               DownstreamClient downstreamClient,
                               ActorRef<Messages.WorkerPoolCommand> pool,
                               String jobId,
                               MetricsRegistry registry) {
        super(ctx);
        this.reader           = reader;
        this.publisher        = publisher;
        this.targetTopic      = targetTopic;
        this.downstreamClient = downstreamClient;
        this.pool             = pool;
        this.jobId            = jobId;
        this.registry         = registry;
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
                .onMessage(Messages.PacketWorkerCommand.Assign.class, this::onAssign)
                .onMessage(Messages.PacketWorkerCommand.Cancel.class, msg -> Behaviors.stopped())
                .build();
    }

    /** Reading in-flight via pipeToSelf. */
    private Receive<Messages.PacketWorkerCommand> fetching() {
        return newReceiveBuilder()
                .onMessage(Messages.PacketWorkerCommand.BatchReady.class,  this::onBatchReady)
                .onMessage(Messages.PacketWorkerCommand.Pause.class,
                        msg -> { pauseRequested = true;  return fetching(); })
                .onMessage(Messages.PacketWorkerCommand.Resume.class,
                        msg -> { pauseRequested = false; return fetching(); })
                .onMessage(Messages.PacketWorkerCommand.Cancel.class, this::onCancel)
                .build();
    }

    /** Kafka publish + HTTP POST in-flight via pipeToSelf. */
    private Receive<Messages.PacketWorkerCommand> publishing() {
        return newReceiveBuilder()
                .onMessage(Messages.PacketWorkerCommand.BatchPublished.class, this::onBatchPublished)
                .onMessage(Messages.PacketWorkerCommand.Pause.class,
                        msg -> { pauseRequested = true;  return publishing(); })
                .onMessage(Messages.PacketWorkerCommand.Resume.class,
                        msg -> { pauseRequested = false; return publishing(); })
                .onMessage(Messages.PacketWorkerCommand.Cancel.class, this::onCancel)
                .build();
    }

    /** Paused — no I/O in-flight; waiting for Resume or Cancel. */
    private Receive<Messages.PacketWorkerCommand> waitingResume() {
        return newReceiveBuilder()
                .onMessage(Messages.PacketWorkerCommand.Resume.class, this::onResume)
                .onMessage(Messages.PacketWorkerCommand.Cancel.class, this::onCancel)
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
        long fetchMs = (System.nanoTime() - fetchStartNs) / 1_000_000L;

        if (msg.error() != null) {
            log.error("Batch read failed for packet {}: {}", packet.packetId(), msg.error().getMessage());
            registry.recordReadError(jobId);
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

        dispatchPublish(msg.events(), msg.batchIndex(), fetchMs);
        return publishing();
    }

    private Behavior<Messages.PacketWorkerCommand> onBatchPublished(Messages.PacketWorkerCommand.BatchPublished msg) {
        long publishMs = (System.nanoTime() - publishStartNs) / 1_000_000L;

        if (msg.error() != null) {
            log.error("Publish failed for packet {} batch {}: {}",
                    packet.packetId().substring(0, 8), msg.batchIndex(), msg.error().getMessage());
            registry.recordPublishError(jobId);
            pool.tell(new Messages.WorkerPoolCommand.PacketFailed(
                    packet.packetId(), "publish error: " + msg.error().getMessage(), getContext().getSelf()));
            return Behaviors.stopped();
        }

        emittedTotal += msg.eventsPublished();
        batchIndex++;

        registry.recordBatch(jobId, msg.eventsPublished(), msg.fetchMs(), publishMs);

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
    // Helpers
    // -----------------------------------------------------------------------

    private void dispatchFetch() {
        fetchStartNs = System.nanoTime();
        int idx = batchIndex;
        getContext().pipeToSelf(
                CompletableFuture.supplyAsync(
                        () -> reader.readBatch(
                                packet.tableLocation(), packet.from(), packet.to(),
                                idx, BATCH_SIZE),
                        IO_EXECUTOR),
                (events, err) -> new Messages.PacketWorkerCommand.BatchReady(events, idx, err));
    }

    /**
     * Concurrently publishes to Kafka and POSTs to the downstream REST endpoint.
     * Both futures are combined: if either fails the {@link Messages.PacketWorkerCommand.BatchPublished}
     * carries the exception, and the worker reports failure to the pool.
     *
     * @param fetchMs fetch latency already measured, forwarded via {@code BatchPublished}
     *                so {@link #onBatchPublished} can pass it to the registry.
     */
    private void dispatchPublish(List<com.example.replay.model.SecurityEvent> events, int idx, long fetchMs) {
        publishStartNs = System.nanoTime();

        var kafkaFuture = publisher.publish(targetTopic, events);
        var httpFuture  = downstreamClient.post(events);

        // Run Kafka and HTTP concurrently; count = Kafka-confirmed events.
        var combined = kafkaFuture.thenCombine(httpFuture, (kafkaCount, httpCount) -> kafkaCount);

        getContext().pipeToSelf(
                combined,
                (count, err) -> new Messages.PacketWorkerCommand.BatchPublished(
                        count != null ? count : 0, idx, fetchMs, err));
    }
}
