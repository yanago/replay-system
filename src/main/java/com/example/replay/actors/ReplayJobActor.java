package com.example.replay.actors;

import com.example.replay.datalake.DataLakeReader;
import com.example.replay.model.ReplayJob;
import com.example.replay.storage.ReplayJobRepository;
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
 * Per-job actor that orchestrates the full replay lifecycle.
 *
 * <h3>Execution model (work-distribution)</h3>
 * <ol>
 *   <li>On {@code Start}: the actor asynchronously invokes the
 *       {@link WorkPlannerFn} (on a virtual-thread executor) to analyse Iceberg
 *       partition metadata and produce a list of {@link WorkPacket}s.</li>
 *   <li>When planning completes ({@code PlanReady}): spawns {@link WorkerPoolActor}
 *       and tells it {@code Start}.  The pool distributes packets across
 *       {@code numWorkers} {@link PacketWorkerActor}s using LRT scheduling.</li>
 *   <li>When all packets finish ({@code PoolFinished}): notifies the
 *       {@link JobManager} coordinator and stops itself.</li>
 * </ol>
 *
 * <h3>State machine</h3>
 * <pre>
 *   IDLE      ──Start──────────────────→ async plan → PLANNING
 *   PLANNING  ──PlanReady (ok)──────────→ spawn pool → RUNNING
 *   PLANNING  ──PlanReady (error)───────→ WorkerFailed → stopped
 *   PLANNING  ──Pause───────────────────→ store flag; WorkerPaused → PLANNING
 *   PLANNING  ──Cancel──────────────────→ stopped
 *   RUNNING   ──PoolFinished────────────→ WorkerFinished → stopped
 *   RUNNING   ──PoolFailed──────────────→ WorkerFailed → stopped
 *   RUNNING   ──Pause───────────────────→ pool.Pause; WorkerPaused → PAUSED
 *   RUNNING   ──Cancel──────────────────→ pool.Cancel → stopped
 *   PAUSED    ──Resume──────────────────→ pool.Resume; WorkerResumed → RUNNING
 *   PAUSED    ──PoolFinished────────────→ WorkerFinished → stopped  (drained)
 *   PAUSED    ──Cancel──────────────────→ pool.Cancel → stopped
 * </pre>
 */
public final class ReplayJobActor extends AbstractBehavior<Messages.ReplayJobCommand> {

    private static final Logger log = LoggerFactory.getLogger(ReplayJobActor.class);

    /** Executor for async planning — virtual threads, non-blocking for the dispatcher. */
    private static final Executor PLAN_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

    private final ReplayJob                             job;
    private final ReplayJobRepository                   repo;
    private final ActorRef<Messages.CoordinatorCommand> coordinator;
    private final DataLakeReader                        dataLakeReader;
    private final WorkPlannerFn                         planner;
    private final int                                   numWorkers;

    private ActorRef<Messages.WorkerPoolCommand> workerPool;

    /** True when a Pause arrived during the async PLANNING phase. */
    private boolean pausedDuringPlanning = false;

    // -----------------------------------------------------------------------
    // Factory
    // -----------------------------------------------------------------------

    public static Behavior<Messages.ReplayJobCommand> create(
            ReplayJob job,
            ReplayJobRepository repo,
            ActorRef<Messages.CoordinatorCommand> coordinator,
            DataLakeReader dataLakeReader,
            WorkPlannerFn planner,
            int numWorkers) {
        return Behaviors.setup(ctx ->
                new ReplayJobActor(ctx, job, repo, coordinator, dataLakeReader, planner, numWorkers));
    }

    private ReplayJobActor(ActorContext<Messages.ReplayJobCommand> ctx,
                            ReplayJob job,
                            ReplayJobRepository repo,
                            ActorRef<Messages.CoordinatorCommand> coordinator,
                            DataLakeReader dataLakeReader,
                            WorkPlannerFn planner,
                            int numWorkers) {
        super(ctx);
        this.job           = job;
        this.repo          = repo;
        this.coordinator   = coordinator;
        this.dataLakeReader = dataLakeReader;
        this.planner       = planner;
        this.numWorkers    = numWorkers;
    }

    // -----------------------------------------------------------------------
    // Behaviors
    // -----------------------------------------------------------------------

    @Override
    public Receive<Messages.ReplayJobCommand> createReceive() {
        return idle();
    }

    /** Waiting for the Start command. */
    private Receive<Messages.ReplayJobCommand> idle() {
        return newReceiveBuilder()
                .onMessage(Messages.ReplayJobCommand.Start.class,  this::onStart)
                .onMessage(Messages.ReplayJobCommand.Cancel.class, msg -> Behaviors.stopped())
                .build();
    }

    /** Async planning in-flight; no pool spawned yet. */
    private Receive<Messages.ReplayJobCommand> planning() {
        return newReceiveBuilder()
                .onMessage(Messages.ReplayJobCommand.PlanReady.class, this::onPlanReady)
                .onMessage(Messages.ReplayJobCommand.Pause.class,     this::onPauseDuringPlanning)
                .onMessage(Messages.ReplayJobCommand.Resume.class,    this::onResumeDuringPlanning)
                .onMessage(Messages.ReplayJobCommand.Cancel.class,    msg -> Behaviors.stopped())
                .build();
    }

    /** Pool dispatching work; making progress. */
    private Receive<Messages.ReplayJobCommand> running() {
        return newReceiveBuilder()
                .onMessage(Messages.ReplayJobCommand.PoolFinished.class, this::onPoolFinished)
                .onMessage(Messages.ReplayJobCommand.PoolFailed.class,   this::onPoolFailed)
                .onMessage(Messages.ReplayJobCommand.Pause.class,        this::onPause)
                .onMessage(Messages.ReplayJobCommand.Cancel.class,       this::onCancel)
                .build();
    }

    /** Pool workers are paused; waiting for Resume or drain completion. */
    private Receive<Messages.ReplayJobCommand> paused() {
        return newReceiveBuilder()
                .onMessage(Messages.ReplayJobCommand.Resume.class,       this::onResume)
                .onMessage(Messages.ReplayJobCommand.Cancel.class,       this::onCancel)
                // Pool may finish while draining in-flight batches
                .onMessage(Messages.ReplayJobCommand.PoolFinished.class, this::onPoolFinished)
                .onMessage(Messages.ReplayJobCommand.PoolFailed.class,   this::onPoolFailed)
                .build();
    }

    // -----------------------------------------------------------------------
    // Handlers
    // -----------------------------------------------------------------------

    private Behavior<Messages.ReplayJobCommand> onStart(Messages.ReplayJobCommand.Start msg) {
        log.info("ReplayJobActor planning job {} [{}, {})", job.jobId(), job.fromTime(), job.toTime());
        getContext().pipeToSelf(
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return planner.plan(job.sourceTable(), job.fromTime(), job.toTime());
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        },
                        PLAN_EXECUTOR),
                (packets, err) -> new Messages.ReplayJobCommand.PlanReady(packets, err));
        return planning();
    }

    private Behavior<Messages.ReplayJobCommand> onPauseDuringPlanning(Messages.ReplayJobCommand.Pause msg) {
        if (!pausedDuringPlanning) {
            pausedDuringPlanning = true;
            coordinator.tell(new Messages.CoordinatorCommand.WorkerPaused(job.jobId()));
            log.debug("Job {} paused during planning", job.jobId());
        }
        return planning();
    }

    private Behavior<Messages.ReplayJobCommand> onResumeDuringPlanning(Messages.ReplayJobCommand.Resume msg) {
        if (pausedDuringPlanning) {
            pausedDuringPlanning = false;
            coordinator.tell(new Messages.CoordinatorCommand.WorkerResumed(job.jobId()));
            log.debug("Job {} resumed during planning (cancels deferred pause)", job.jobId());
        }
        return planning();
    }

    private Behavior<Messages.ReplayJobCommand> onPlanReady(Messages.ReplayJobCommand.PlanReady msg) {
        if (msg.error() != null) {
            log.error("Planning failed for job {}", job.jobId(), msg.error());
            coordinator.tell(new Messages.CoordinatorCommand.WorkerFailed(
                    job.jobId(), "planning failed: " + msg.error().getMessage()));
            return Behaviors.stopped();
        }

        var packets = msg.packets();
        log.info("Job {} plan ready: {} work packets (numWorkers={})", job.jobId(), packets.size(), numWorkers);

        if (packets.isEmpty()) {
            coordinator.tell(new Messages.CoordinatorCommand.WorkerFinished(job.jobId(), 0L));
            return Behaviors.stopped();
        }

        workerPool = getContext().spawn(
                WorkerPoolActor.create(packets, dataLakeReader, getContext().getSelf(), numWorkers),
                "pool-" + job.jobId());
        workerPool.tell(new Messages.WorkerPoolCommand.Start());

        if (pausedDuringPlanning) {
            pausedDuringPlanning = false;
            workerPool.tell(new Messages.WorkerPoolCommand.Pause());
            return paused();
        }
        return running();
    }

    private Behavior<Messages.ReplayJobCommand> onPoolFinished(Messages.ReplayJobCommand.PoolFinished msg) {
        log.info("Job {} completed — {} events emitted total", job.jobId(), msg.totalEvents());
        repo.update(job.withProgress(msg.totalEvents()));
        coordinator.tell(new Messages.CoordinatorCommand.WorkerFinished(job.jobId(), msg.totalEvents()));
        return Behaviors.stopped();
    }

    private Behavior<Messages.ReplayJobCommand> onPoolFailed(Messages.ReplayJobCommand.PoolFailed msg) {
        log.error("Job {} pool failed: {}", job.jobId(), msg.reason());
        coordinator.tell(new Messages.CoordinatorCommand.WorkerFailed(job.jobId(), msg.reason()));
        return Behaviors.stopped();
    }

    private Behavior<Messages.ReplayJobCommand> onPause(Messages.ReplayJobCommand.Pause msg) {
        if (workerPool != null) workerPool.tell(new Messages.WorkerPoolCommand.Pause());
        coordinator.tell(new Messages.CoordinatorCommand.WorkerPaused(job.jobId()));
        log.info("Job {} pausing", job.jobId());
        return paused();
    }

    private Behavior<Messages.ReplayJobCommand> onResume(Messages.ReplayJobCommand.Resume msg) {
        if (workerPool != null) workerPool.tell(new Messages.WorkerPoolCommand.Resume());
        coordinator.tell(new Messages.CoordinatorCommand.WorkerResumed(job.jobId()));
        log.info("Job {} resuming", job.jobId());
        return running();
    }

    private Behavior<Messages.ReplayJobCommand> onCancel(Messages.ReplayJobCommand.Cancel msg) {
        if (workerPool != null) workerPool.tell(new Messages.WorkerPoolCommand.Cancel());
        log.info("Job {} cancelled", job.jobId());
        return Behaviors.stopped();
    }
}
