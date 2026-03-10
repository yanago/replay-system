package com.example.replay.actors;

import com.example.replay.model.ReplayJob;
import com.example.replay.storage.ReplayJobRepository;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;

/**
 * Per-job actor that owns the full replay lifecycle for a single job.
 *
 * <p>State machine:
 * <pre>
 *   IDLE ──Start──→ RUNNING
 *         spawns DataReaderActor + DataEmitterActor
 *
 *   RUNNING ──BatchRead(events, seq)──→ forward to emitter ──→ RUNNING
 *   RUNNING ──BatchEmitted(seq, n)───→ progress update; if done → stopped
 *   RUNNING ──BatchFailed(seq, msg)──→ notify coordinator → stopped
 *   RUNNING ──ReaderDone─────────────→ mark done; if last batch emitted → stopped
 *   RUNNING ──Pause──────────────────→ PAUSED  (reader paused, emitter drains)
 *   RUNNING ──Cancel─────────────────→ stopped
 *
 *   PAUSED ──Resume─────────────────→ RUNNING
 *   PAUSED ──BatchEmitted (drain)───→ PAUSED  (progress updated)
 *   PAUSED ──BatchRead (late)───────→ PAUSED  (discarded)
 *   PAUSED ──Cancel─────────────────→ stopped
 * </pre>
 *
 * <p>Children:
 * <ul>
 *   <li>{@link DataReaderActor}  — reads event batches from the data lake</li>
 *   <li>{@link DataEmitterActor} — publishes batches to Kafka</li>
 * </ul>
 */
public final class ReplayJobActor extends AbstractBehavior<Messages.ReplayJobCommand> {

    private final ReplayJob                             job;
    private final ReplayJobRepository                   repo;
    private final ActorRef<Messages.CoordinatorCommand> coordinator;

    private ActorRef<Messages.DataReaderCommand>  reader;
    private ActorRef<Messages.DataEmitterCommand> emitter;

    private long    totalEmitted   = 0L;
    private long    pendingBatches = 0L;   // batches sent to emitter but not yet acked
    private boolean readerDone     = false;

    // -----------------------------------------------------------------------
    // Factory
    // -----------------------------------------------------------------------

    public static Behavior<Messages.ReplayJobCommand> create(
            ReplayJob job,
            ReplayJobRepository repo,
            ActorRef<Messages.CoordinatorCommand> coordinator) {
        return Behaviors.setup(ctx -> new ReplayJobActor(ctx, job, repo, coordinator));
    }

    private ReplayJobActor(ActorContext<Messages.ReplayJobCommand> ctx,
                            ReplayJob job,
                            ReplayJobRepository repo,
                            ActorRef<Messages.CoordinatorCommand> coordinator) {
        super(ctx);
        this.job         = job;
        this.repo        = repo;
        this.coordinator = coordinator;
    }

    // -----------------------------------------------------------------------
    // Behaviors (one per state)
    // -----------------------------------------------------------------------

    @Override
    public Receive<Messages.ReplayJobCommand> createReceive() {
        return idle();
    }

    /** IDLE — waiting for Start. */
    private Receive<Messages.ReplayJobCommand> idle() {
        return newReceiveBuilder()
                .onMessage(Messages.ReplayJobCommand.Start.class,  this::onStart)
                .onMessage(Messages.ReplayJobCommand.Cancel.class, msg -> Behaviors.stopped())
                .build();
    }

    /** RUNNING — reader and emitter are active. */
    private Receive<Messages.ReplayJobCommand> running() {
        return newReceiveBuilder()
                .onMessage(Messages.ReplayJobCommand.Pause.class,        this::onPause)
                .onMessage(Messages.ReplayJobCommand.Cancel.class,       this::onCancel)
                .onMessage(Messages.ReplayJobCommand.BatchRead.class,    this::onBatchRead)
                .onMessage(Messages.ReplayJobCommand.BatchEmitted.class, msg -> onBatchEmitted(msg, running()))
                .onMessage(Messages.ReplayJobCommand.BatchFailed.class,  this::onBatchFailed)
                .onMessage(Messages.ReplayJobCommand.ReaderDone.class,   this::onReaderDone)
                .build();
    }

    /** PAUSED — reader is paused; emitter may still drain its current batch. */
    private Receive<Messages.ReplayJobCommand> paused() {
        return newReceiveBuilder()
                .onMessage(Messages.ReplayJobCommand.Resume.class,       this::onResume)
                .onMessage(Messages.ReplayJobCommand.Cancel.class,       this::onCancel)
                // Emitter may still finish the in-flight batch
                .onMessage(Messages.ReplayJobCommand.BatchEmitted.class, msg -> onBatchEmitted(msg, paused()))
                .onMessage(Messages.ReplayJobCommand.BatchFailed.class,  this::onBatchFailed)
                // Discard late reader batches that slipped through before the pause took effect
                .onMessage(Messages.ReplayJobCommand.BatchRead.class,    msg -> paused())
                .onMessage(Messages.ReplayJobCommand.ReaderDone.class,   msg -> { readerDone = true; return paused(); })
                .build();
    }

    // -----------------------------------------------------------------------
    // Handlers
    // -----------------------------------------------------------------------

    private Behavior<Messages.ReplayJobCommand> onStart(Messages.ReplayJobCommand.Start msg) {
        reader  = getContext().spawn(
                DataReaderActor.create(getContext().getSelf(), job), "reader-"  + job.jobId());
        emitter = getContext().spawn(
                DataEmitterActor.create(getContext().getSelf()),     "emitter-" + job.jobId());
        reader.tell(new Messages.DataReaderCommand.Start());
        getContext().getLog().info("ReplayJobActor started for job {}", job.jobId());
        return running();
    }

    private Behavior<Messages.ReplayJobCommand> onPause(Messages.ReplayJobCommand.Pause msg) {
        reader.tell(new Messages.DataReaderCommand.Pause());
        coordinator.tell(new Messages.CoordinatorCommand.WorkerPaused(job.jobId()));
        getContext().getLog().info("Job {} pausing", job.jobId());
        return paused();
    }

    private Behavior<Messages.ReplayJobCommand> onResume(Messages.ReplayJobCommand.Resume msg) {
        reader.tell(new Messages.DataReaderCommand.Resume());
        coordinator.tell(new Messages.CoordinatorCommand.WorkerResumed(job.jobId()));
        getContext().getLog().info("Job {} resuming", job.jobId());
        return running();
    }

    private Behavior<Messages.ReplayJobCommand> onCancel(Messages.ReplayJobCommand.Cancel msg) {
        if (reader  != null) reader.tell(new Messages.DataReaderCommand.Cancel());
        if (emitter != null) emitter.tell(new Messages.DataEmitterCommand.Cancel());
        getContext().getLog().info("Job {} cancelled ({} events emitted)", job.jobId(), totalEmitted);
        return Behaviors.stopped();
    }

    private Behavior<Messages.ReplayJobCommand> onBatchRead(Messages.ReplayJobCommand.BatchRead msg) {
        pendingBatches++;
        emitter.tell(new Messages.DataEmitterCommand.Emit(msg.events(), msg.seq()));
        return running();
    }

    /**
     * Shared BatchEmitted handler for both RUNNING and PAUSED states.
     * {@code continueWith} is the behavior to return if completion criteria are not met.
     */
    private Behavior<Messages.ReplayJobCommand> onBatchEmitted(
            Messages.ReplayJobCommand.BatchEmitted msg,
            Receive<Messages.ReplayJobCommand> continueWith) {
        totalEmitted   += msg.count();
        pendingBatches--;
        repo.update(job.withProgress(totalEmitted));
        getContext().getLog().debug("Job {} batch {} emitted ({} events, total {})",
                job.jobId(), msg.seq(), msg.count(), totalEmitted);
        return checkCompletion(continueWith);
    }

    private Behavior<Messages.ReplayJobCommand> onBatchFailed(Messages.ReplayJobCommand.BatchFailed msg) {
        coordinator.tell(new Messages.CoordinatorCommand.WorkerFailed(job.jobId(), msg.reason()));
        getContext().getLog().error("Job {} batch {} failed: {}", job.jobId(), msg.seq(), msg.reason());
        return Behaviors.stopped();
    }

    private Behavior<Messages.ReplayJobCommand> onReaderDone(Messages.ReplayJobCommand.ReaderDone msg) {
        readerDone = true;
        getContext().getLog().debug("Job {} reader finished ({} batches)", job.jobId(), msg.totalBatches());
        return checkCompletion(running());
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /**
     * Returns {@link Behaviors#stopped()} (notifying coordinator) when all
     * emitted batches have been acknowledged AND the reader has finished.
     * Otherwise returns {@code continueWith} unchanged.
     */
    private Behavior<Messages.ReplayJobCommand> checkCompletion(
            Receive<Messages.ReplayJobCommand> continueWith) {
        if (readerDone && pendingBatches == 0) {
            coordinator.tell(new Messages.CoordinatorCommand.WorkerFinished(job.jobId(), totalEmitted));
            getContext().getLog().info("Job {} completed — {} events emitted total",
                    job.jobId(), totalEmitted);
            return Behaviors.stopped();
        }
        return continueWith;
    }
}
