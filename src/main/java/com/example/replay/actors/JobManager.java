package com.example.replay.actors;

import com.example.replay.actors.Messages.CoordinatorCommand;
import com.example.replay.actors.Messages.CoordinatorResponse;
import com.example.replay.model.ReplayJob;
import com.example.replay.model.ReplayStatus;
import com.example.replay.storage.ReplayJobRepository;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Root supervisor for the replay actor hierarchy.
 *
 * <p>Owns one {@link ReplayJobActor} child per active job. All durable state is
 * delegated to {@link ReplayJobRepository}; actor-internal state (live worker refs)
 * is rebuilt on restart.
 *
 * <p>Supported job lifecycle transitions via {@link CoordinatorCommand}:
 * <pre>
 *   SubmitJob  → PENDING → RUNNING  (spawns ReplayJobActor)
 *   PauseJob   → RUNNING → PAUSED   (tells ReplayJobActor.Pause)
 *   ResumeJob  → PAUSED  → RUNNING  (tells ReplayJobActor.Resume)
 *   CancelJob  → any     → CANCELLED
 * </pre>
 *
 * <p>Drop-in replacement for {@link ReplayCoordinator} — accepts the same
 * {@code ActorSystem<CoordinatorCommand>} type used by {@code JobsHandler}.
 */
public final class JobManager extends AbstractBehavior<CoordinatorCommand> {

    private final ReplayJobRepository                           repo;
    private final Map<String, ActorRef<Messages.ReplayJobCommand>> workers = new HashMap<>();

    // -----------------------------------------------------------------------
    // Factory
    // -----------------------------------------------------------------------

    public static Behavior<CoordinatorCommand> create(ReplayJobRepository repo) {
        return Behaviors.setup(ctx -> new JobManager(ctx, repo));
    }

    private JobManager(ActorContext<CoordinatorCommand> ctx, ReplayJobRepository repo) {
        super(ctx);
        this.repo = repo;
    }

    // -----------------------------------------------------------------------
    // Message dispatch
    // -----------------------------------------------------------------------

    @Override
    public Receive<CoordinatorCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(CoordinatorCommand.SubmitJob.class,     this::onSubmit)
                .onMessage(CoordinatorCommand.PauseJob.class,      this::onPause)
                .onMessage(CoordinatorCommand.ResumeJob.class,     this::onResume)
                .onMessage(CoordinatorCommand.CancelJob.class,     this::onCancel)
                .onMessage(CoordinatorCommand.GetJob.class,        this::onGet)
                .onMessage(CoordinatorCommand.ListJobs.class,      this::onList)
                .onMessage(CoordinatorCommand.WorkerFinished.class, this::onFinished)
                .onMessage(CoordinatorCommand.WorkerFailed.class,  this::onFailed)
                .onMessage(CoordinatorCommand.WorkerPaused.class,  this::onWorkerPaused)
                .onMessage(CoordinatorCommand.WorkerResumed.class, this::onWorkerResumed)
                .build();
    }

    // -----------------------------------------------------------------------
    // Handlers
    // -----------------------------------------------------------------------

    private Behavior<CoordinatorCommand> onSubmit(CoordinatorCommand.SubmitJob msg) {
        var job = msg.job().withStatus(ReplayStatus.RUNNING);
        repo.save(job);

        var workerRef = getContext().spawn(
                ReplayJobActor.create(job, repo, getContext().getSelf()),
                "replay-job-" + job.jobId());
        workers.put(job.jobId(), workerRef);
        workerRef.tell(new Messages.ReplayJobCommand.Start());

        getContext().getLog().info("Job {} submitted — ReplayJobActor spawned", job.jobId());
        msg.replyTo().tell(new CoordinatorResponse.JobAccepted(job));
        return this;
    }

    private Behavior<CoordinatorCommand> onPause(CoordinatorCommand.PauseJob msg) {
        var job = repo.findById(msg.jobId()).orElse(null);
        if (job == null) {
            msg.replyTo().tell(new CoordinatorResponse.JobNotFound(msg.jobId()));
            return this;
        }
        if (job.status() != ReplayStatus.RUNNING) {
            msg.replyTo().tell(new CoordinatorResponse.Rejected(
                    "job is not RUNNING (current: " + job.status() + ")"));
            return this;
        }
        var workerRef = workers.get(msg.jobId());
        if (workerRef != null) workerRef.tell(new Messages.ReplayJobCommand.Pause());

        var paused = job.withStatus(ReplayStatus.PAUSED);
        repo.update(paused);
        getContext().getLog().info("Job {} paused", msg.jobId());
        msg.replyTo().tell(new CoordinatorResponse.JobPaused(paused));
        return this;
    }

    private Behavior<CoordinatorCommand> onResume(CoordinatorCommand.ResumeJob msg) {
        var job = repo.findById(msg.jobId()).orElse(null);
        if (job == null) {
            msg.replyTo().tell(new CoordinatorResponse.JobNotFound(msg.jobId()));
            return this;
        }
        if (job.status() != ReplayStatus.PAUSED) {
            msg.replyTo().tell(new CoordinatorResponse.Rejected(
                    "job is not PAUSED (current: " + job.status() + ")"));
            return this;
        }
        var workerRef = workers.get(msg.jobId());
        if (workerRef != null) workerRef.tell(new Messages.ReplayJobCommand.Resume());

        var running = job.withStatus(ReplayStatus.RUNNING);
        repo.update(running);
        getContext().getLog().info("Job {} resumed", msg.jobId());
        msg.replyTo().tell(new CoordinatorResponse.JobResumed(running));
        return this;
    }

    private Behavior<CoordinatorCommand> onCancel(CoordinatorCommand.CancelJob msg) {
        var job = repo.findById(msg.jobId()).orElse(null);
        if (job == null) {
            msg.replyTo().tell(new CoordinatorResponse.JobNotFound(msg.jobId()));
            return this;
        }
        var workerRef = workers.remove(msg.jobId());
        if (workerRef != null) workerRef.tell(new Messages.ReplayJobCommand.Cancel());

        var cancelled = job.withStatus(ReplayStatus.CANCELLED);
        repo.update(cancelled);
        getContext().getLog().info("Job {} cancelled", msg.jobId());
        msg.replyTo().tell(new CoordinatorResponse.JobSnapshot(cancelled));
        return this;
    }

    private Behavior<CoordinatorCommand> onGet(CoordinatorCommand.GetJob msg) {
        repo.findById(msg.jobId())
                .ifPresentOrElse(
                        job -> msg.replyTo().tell(new CoordinatorResponse.JobSnapshot(job)),
                        ()  -> msg.replyTo().tell(new CoordinatorResponse.JobNotFound(msg.jobId())));
        return this;
    }

    private Behavior<CoordinatorCommand> onList(CoordinatorCommand.ListJobs msg) {
        msg.replyTo().tell(new CoordinatorResponse.JobList(List.copyOf(repo.findAll())));
        return this;
    }

    private Behavior<CoordinatorCommand> onFinished(CoordinatorCommand.WorkerFinished msg) {
        workers.remove(msg.jobId());
        repo.findById(msg.jobId()).ifPresent(job ->
                repo.update(job.withProgress(msg.eventsPublished())
                               .withStatus(ReplayStatus.COMPLETED)));
        getContext().getLog().info("Job {} completed — {} events published",
                msg.jobId(), msg.eventsPublished());
        return this;
    }

    private Behavior<CoordinatorCommand> onFailed(CoordinatorCommand.WorkerFailed msg) {
        workers.remove(msg.jobId());
        repo.findById(msg.jobId()).ifPresent(job -> repo.update(job.failed(msg.reason())));
        getContext().getLog().error("Job {} failed: {}", msg.jobId(), msg.reason());
        return this;
    }

    private Behavior<CoordinatorCommand> onWorkerPaused(CoordinatorCommand.WorkerPaused msg) {
        getContext().getLog().debug("Worker confirmed pause for job {}", msg.jobId());
        return this;
    }

    private Behavior<CoordinatorCommand> onWorkerResumed(CoordinatorCommand.WorkerResumed msg) {
        getContext().getLog().debug("Worker confirmed resume for job {}", msg.jobId());
        return this;
    }
}
