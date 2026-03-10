package com.example.replay.actors;

import com.example.replay.model.ReplayJob;
import com.example.replay.model.SecurityEvent;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.apache.pekko.actor.typed.javadsl.TimerScheduler;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Reads event batches from the data lake and delivers them to the parent
 * {@link ReplayJobActor}.
 *
 * <p><strong>Stub implementation</strong> — {@link #fetchNextBatch()} generates
 * synthetic {@link SecurityEvent}s at a fixed rate. Replace it with a real
 * {@code IcebergDataLakeReader.readBatch(job, offset)} call when integrating.
 *
 * <p>State machine:
 * <pre>
 *   IDLE ──Start──→ READING (periodic FetchBatch timer started)
 *   READING ──FetchBatch──→ send BatchRead to parent, reschedule
 *   READING ──FetchBatch (last)──→ send ReaderDone to parent → stopped
 *   READING ──Pause──→ PAUSED  (timer cancelled)
 *   PAUSED  ──Resume──→ READING (timer restarted)
 *   any ──Cancel──→ stopped
 * </pre>
 */
public final class DataReaderActor extends AbstractBehavior<Messages.DataReaderCommand> {

    /** Synthetic events per batch. */
    private static final int      BATCH_SIZE     = 10;
    /** Delay between consecutive batch fetches. */
    private static final Duration FETCH_INTERVAL = Duration.ofMillis(200);
    /** Total synthetic batches before signalling done. Swap for real cursor logic. */
    private static final int      MAX_BATCHES    = 5;
    /** Stable key used to cancel/restart the periodic fetch timer. */
    private static final Object   TIMER_KEY      = "fetch";

    private final ActorRef<Messages.ReplayJobCommand>        parent;
    private final ReplayJob                                  job;
    private final TimerScheduler<Messages.DataReaderCommand> timers;

    private long batchSeq = 0;

    // -----------------------------------------------------------------------
    // Factory
    // -----------------------------------------------------------------------

    public static Behavior<Messages.DataReaderCommand> create(
            ActorRef<Messages.ReplayJobCommand> parent, ReplayJob job) {
        return Behaviors.withTimers(timers ->
                Behaviors.setup(ctx -> new DataReaderActor(ctx, timers, parent, job)));
    }

    private DataReaderActor(ActorContext<Messages.DataReaderCommand> ctx,
                             TimerScheduler<Messages.DataReaderCommand> timers,
                             ActorRef<Messages.ReplayJobCommand> parent,
                             ReplayJob job) {
        super(ctx);
        this.timers = timers;
        this.parent = parent;
        this.job    = job;
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

    private Receive<Messages.DataReaderCommand> reading() {
        return newReceiveBuilder()
                .onMessage(Messages.DataReaderCommand.FetchBatch.class, this::onFetch)
                .onMessage(Messages.DataReaderCommand.Pause.class,      this::onPause)
                .onMessage(Messages.DataReaderCommand.Cancel.class,     this::onCancel)
                .build();
    }

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
        scheduleFetch();
        getContext().getLog().debug("DataReaderActor started for job {}", job.jobId());
        return reading();
    }

    private Behavior<Messages.DataReaderCommand> onFetch(Messages.DataReaderCommand.FetchBatch msg) {
        if (batchSeq >= MAX_BATCHES) {
            parent.tell(new Messages.ReplayJobCommand.ReaderDone(batchSeq));
            getContext().getLog().debug("DataReaderActor done — job {} sent {} batches",
                    job.jobId(), batchSeq);
            return Behaviors.stopped();
        }
        parent.tell(new Messages.ReplayJobCommand.BatchRead(fetchNextBatch(), batchSeq++));
        scheduleFetch();
        return reading();
    }

    private Behavior<Messages.DataReaderCommand> onPause(Messages.DataReaderCommand.Pause msg) {
        timers.cancel(TIMER_KEY);
        getContext().getLog().debug("DataReaderActor paused at batch {} for job {}", batchSeq, job.jobId());
        return waitingResume();
    }

    private Behavior<Messages.DataReaderCommand> onResume(Messages.DataReaderCommand.Resume msg) {
        scheduleFetch();
        getContext().getLog().debug("DataReaderActor resumed for job {}", job.jobId());
        return reading();
    }

    private Behavior<Messages.DataReaderCommand> onCancel(Messages.DataReaderCommand.Cancel msg) {
        timers.cancelAll();
        getContext().getLog().debug("DataReaderActor cancelled for job {}", job.jobId());
        return Behaviors.stopped();
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private void scheduleFetch() {
        timers.startSingleTimer(TIMER_KEY, new Messages.DataReaderCommand.FetchBatch(), FETCH_INTERVAL);
    }

    /**
     * Generates a synthetic batch of events.
     * Replace with {@code IcebergDataLakeReader.readBatch(job, batchSeq)}.
     */
    private List<SecurityEvent> fetchNextBatch() {
        var events = new ArrayList<SecurityEvent>(BATCH_SIZE);
        for (int i = 0; i < BATCH_SIZE; i++) {
            events.add(new SecurityEvent(
                    "evt-b%d-i%d".formatted(batchSeq, i),
                    job.sourceTable(),
                    Instant.now(),
                    Instant.now(),
                    "SECURITY_EVENT",
                    null, null, null, Map.of()));
        }
        return events;
    }
}
