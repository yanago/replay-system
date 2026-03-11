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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Manages a fixed pool of {@link PacketWorkerActor}s and distributes
 * {@link WorkPacket}s across them.
 *
 * <h3>Scheduling</h3>
 * Packets arrive pre-sorted by {@link WorkPlanner} (largest-first, LRT
 * assignment).  The pool respects the {@link WorkPacket#suggestedWorker()}
 * hint when a preferred slot is free, otherwise it falls back to the
 * next available slot.  This ensures that high-skew, high-weight packets
 * from Zipf-heavy customers are spread evenly instead of piling up on a
 * single worker.
 *
 * <h3>State machine</h3>
 * <pre>
 *   IDLE    ──Start──────────────────────→ dispatch up to N packets → RUNNING
 *   RUNNING ──PacketDone────────────────→ dispatch next; if all done → PoolFinished
 *   RUNNING ──PacketFailed──────────────→ cancel all workers → PoolFailed
 *   RUNNING ──Pause─────────────────────→ tell all active workers Pause → PAUSED
 *   RUNNING ──Cancel────────────────────→ tell all workers Cancel → stopped
 *   PAUSED  ──Resume────────────────────→ tell workers Resume; fill open slots → RUNNING
 *   PAUSED  ──PacketDone (draining)─────→ update counter; if all done → PoolFinished
 *   PAUSED  ──PacketFailed──────────────→ cancel all; PoolFailed → stopped
 *   PAUSED  ──Cancel────────────────────→ tell all workers Cancel → stopped
 * </pre>
 */
public final class WorkerPoolActor extends AbstractBehavior<Messages.WorkerPoolCommand> {

    private static final Logger log = LoggerFactory.getLogger(WorkerPoolActor.class);

    private final DataLakeReader                          reader;
    private final EventPublisher                          publisher;
    private final String                                  targetTopic;
    private final DownstreamClient                        downstreamClient;
    private final int                                     numWorkers;
    private final ActorRef<Messages.ReplayJobCommand>     parent;
    private final String                                  jobId;
    private final MetricsRegistry                         registry;

    /** Packets not yet dispatched, ordered as delivered by WorkPlanner. */
    private final Deque<WorkPacket> pending;

    /** Active worker → the packet it is currently processing. */
    private final Map<ActorRef<Messages.PacketWorkerCommand>, WorkPacket> active = new LinkedHashMap<>();

    private long totalEventsEmitted = 0L;
    private int  completedPackets   = 0;
    private final int totalPackets;

    // -----------------------------------------------------------------------
    // Factory
    // -----------------------------------------------------------------------

    public static Behavior<Messages.WorkerPoolCommand> create(
            List<WorkPacket> packets,
            DataLakeReader reader,
            EventPublisher publisher,
            String targetTopic,
            DownstreamClient downstreamClient,
            ActorRef<Messages.ReplayJobCommand> parent,
            int numWorkers,
            String jobId,
            MetricsRegistry registry) {
        return Behaviors.setup(ctx ->
                new WorkerPoolActor(ctx, packets, reader, publisher, targetTopic, downstreamClient,
                        parent, numWorkers, jobId, registry));
    }

    private WorkerPoolActor(ActorContext<Messages.WorkerPoolCommand> ctx,
                             List<WorkPacket> packets,
                             DataLakeReader reader,
                             EventPublisher publisher,
                             String targetTopic,
                             DownstreamClient downstreamClient,
                             ActorRef<Messages.ReplayJobCommand> parent,
                             int numWorkers,
                             String jobId,
                             MetricsRegistry registry) {
        super(ctx);
        this.reader           = reader;
        this.publisher        = publisher;
        this.targetTopic      = targetTopic;
        this.downstreamClient = downstreamClient;
        this.numWorkers       = numWorkers;
        this.parent           = parent;
        this.pending          = new ArrayDeque<>(packets);
        this.totalPackets     = packets.size();
        this.jobId            = jobId;
        this.registry         = registry;
    }

    // -----------------------------------------------------------------------
    // Behaviors
    // -----------------------------------------------------------------------

    @Override
    public Receive<Messages.WorkerPoolCommand> createReceive() {
        return idle();
    }

    private Receive<Messages.WorkerPoolCommand> idle() {
        return newReceiveBuilder()
                .onMessage(Messages.WorkerPoolCommand.Start.class,  this::onStart)
                .onMessage(Messages.WorkerPoolCommand.Cancel.class, msg -> Behaviors.stopped())
                .build();
    }

    private Receive<Messages.WorkerPoolCommand> running() {
        return newReceiveBuilder()
                .onMessage(Messages.WorkerPoolCommand.PacketDone.class,   this::onPacketDone)
                .onMessage(Messages.WorkerPoolCommand.PacketFailed.class, this::onPacketFailed)
                .onMessage(Messages.WorkerPoolCommand.Pause.class,        this::onPause)
                .onMessage(Messages.WorkerPoolCommand.Cancel.class,       this::onCancel)
                .build();
    }

    private Receive<Messages.WorkerPoolCommand> paused() {
        return newReceiveBuilder()
                .onMessage(Messages.WorkerPoolCommand.PacketDone.class,   this::onPacketDoneWhilePaused)
                .onMessage(Messages.WorkerPoolCommand.PacketFailed.class, this::onPacketFailed)
                .onMessage(Messages.WorkerPoolCommand.Resume.class,       this::onResume)
                .onMessage(Messages.WorkerPoolCommand.Cancel.class,       this::onCancel)
                .build();
    }

    // -----------------------------------------------------------------------
    // Handlers
    // -----------------------------------------------------------------------

    private Behavior<Messages.WorkerPoolCommand> onStart(Messages.WorkerPoolCommand.Start msg) {
        if (totalPackets == 0) {
            parent.tell(new Messages.ReplayJobCommand.PoolFinished(0L));
            return Behaviors.stopped();
        }
        log.info("WorkerPool starting: {} packets across {} workers", totalPackets, numWorkers);
        fillWorkers();
        return running();
    }

    private Behavior<Messages.WorkerPoolCommand> onPacketDone(Messages.WorkerPoolCommand.PacketDone msg) {
        totalEventsEmitted += msg.eventsEmitted();
        completedPackets++;
        active.remove(msg.workerRef());
        registry.recordPacketDone(jobId);

        log.debug("Packet {} done ({} events). Progress: {}/{}",
                msg.packetId().substring(0, 8), msg.eventsEmitted(), completedPackets, totalPackets);

        fillWorkers();

        if (active.isEmpty() && pending.isEmpty()) {
            log.info("WorkerPool finished — {} total events from {} packets", totalEventsEmitted, totalPackets);
            parent.tell(new Messages.ReplayJobCommand.PoolFinished(totalEventsEmitted));
            return Behaviors.stopped();
        }
        return running();
    }

    private Behavior<Messages.WorkerPoolCommand> onPacketDoneWhilePaused(Messages.WorkerPoolCommand.PacketDone msg) {
        totalEventsEmitted += msg.eventsEmitted();
        completedPackets++;
        active.remove(msg.workerRef());
        registry.recordPacketDone(jobId);

        log.debug("Packet {} drained while paused. Progress: {}/{}", msg.packetId().substring(0, 8), completedPackets, totalPackets);

        // Don't dispatch new packets — we are paused
        if (active.isEmpty() && pending.isEmpty()) {
            log.info("WorkerPool finished (drained while paused) — {} total events", totalEventsEmitted);
            parent.tell(new Messages.ReplayJobCommand.PoolFinished(totalEventsEmitted));
            return Behaviors.stopped();
        }
        return paused();
    }

    private Behavior<Messages.WorkerPoolCommand> onPacketFailed(Messages.WorkerPoolCommand.PacketFailed msg) {
        log.error("Packet {} failed: {}", msg.packetId().substring(0, 8), msg.reason());
        cancelAllWorkers();
        parent.tell(new Messages.ReplayJobCommand.PoolFailed(
                "packet " + msg.packetId().substring(0, 8) + " failed: " + msg.reason()));
        return Behaviors.stopped();
    }

    private Behavior<Messages.WorkerPoolCommand> onPause(Messages.WorkerPoolCommand.Pause msg) {
        log.debug("WorkerPool pausing — {} active workers", active.size());
        active.keySet().forEach(w -> w.tell(new Messages.PacketWorkerCommand.Pause()));
        return paused();
    }

    private Behavior<Messages.WorkerPoolCommand> onResume(Messages.WorkerPoolCommand.Resume msg) {
        log.debug("WorkerPool resuming");
        active.keySet().forEach(w -> w.tell(new Messages.PacketWorkerCommand.Resume()));
        // Fill any open slots that appeared while paused
        fillWorkers();
        return running();
    }

    private Behavior<Messages.WorkerPoolCommand> onCancel(Messages.WorkerPoolCommand.Cancel msg) {
        log.debug("WorkerPool cancelled");
        cancelAllWorkers();
        return Behaviors.stopped();
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /**
     * Dispatches packets from the pending queue to free worker slots.
     *
     * <p>Prefers the {@link WorkPacket#suggestedWorker()} slot when it is free
     * (LRT hint from the planner).  Falls back to any free slot so that no slot
     * sits idle while packets are waiting.
     */
    private void fillWorkers() {
        while (!pending.isEmpty() && active.size() < numWorkers) {
            WorkPacket packet = pending.poll();
            var worker = getContext().spawn(
                    PacketWorkerActor.create(reader, publisher, targetTopic, downstreamClient,
                            getContext().getSelf(), jobId, registry),
                    "pworker-" + packet.packetId().substring(0, 8));
            active.put(worker, packet);
            worker.tell(new Messages.PacketWorkerCommand.Assign(packet));
            log.debug("Dispatched {} to new worker (active={}/{})",
                    packet.packetId().substring(0, 8), active.size(), numWorkers);
        }
    }

    private void cancelAllWorkers() {
        active.keySet().forEach(w -> w.tell(new Messages.PacketWorkerCommand.Cancel()));
        active.clear();
    }

    /** Summary for log/debug. */
    private List<String> activePacketIds() {
        return active.values().stream()
                .map(p -> p.packetId().substring(0, 8))
                .toList();
    }
}
