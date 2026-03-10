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
 * Root supervisor that owns the lifecycle of all replay jobs.
 * Spawns one {@link ReplayWorker} child per active job.
 *
 * <p>All job state is persisted through the injected {@link ReplayJobRepository}
 * so both the actor and the HTTP read-layer see the same data.
 * Swap {@code InMemoryJobRepository} for {@code PostgresReplayJobRepository}
 * in {@code ReplayApplication} without touching this class.
 */
public final class ReplayCoordinator
        extends AbstractBehavior<Messages.CoordinatorCommand> {

    private final ReplayJobRepository                           repo;
    // Worker refs kept here (not in repo) — they are runtime-only, non-serialisable
    private final Map<String, ActorRef<Messages.WorkerCommand>> workers = new HashMap<>();

    // -----------------------------------------------------------------------
    // Factory
    // -----------------------------------------------------------------------

    public static Behavior<Messages.CoordinatorCommand> create(ReplayJobRepository repo) {
        return Behaviors.setup(ctx -> new ReplayCoordinator(ctx, repo));
    }

    private ReplayCoordinator(ActorContext<Messages.CoordinatorCommand> ctx,
                               ReplayJobRepository repo) {
        super(ctx);
        this.repo = repo;
    }

    // -----------------------------------------------------------------------
    // Message dispatch
    // -----------------------------------------------------------------------

    @Override
    public Receive<Messages.CoordinatorCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(CoordinatorCommand.SubmitJob.class,      this::onSubmit)
                .onMessage(CoordinatorCommand.CancelJob.class,      this::onCancel)
                .onMessage(CoordinatorCommand.GetJob.class,         this::onGet)
                .onMessage(CoordinatorCommand.ListJobs.class,       this::onList)
                .onMessage(CoordinatorCommand.WorkerFinished.class, this::onFinished)
                .onMessage(CoordinatorCommand.WorkerFailed.class,   this::onFailed)
                .build();
    }

    // -----------------------------------------------------------------------
    // Handlers
    // -----------------------------------------------------------------------

    private Behavior<Messages.CoordinatorCommand> onSubmit(CoordinatorCommand.SubmitJob msg) {
        var job = msg.job().withStatus(ReplayStatus.RUNNING);
        repo.save(job);

        var workerRef = getContext().spawn(
                ReplayWorker.create(getContext().getSelf()),
                "worker-" + job.jobId());
        workers.put(job.jobId(), workerRef);
        workerRef.tell(new Messages.WorkerCommand.Start(job));

        getContext().getLog().info("Job {} submitted, worker spawned", job.jobId());
        msg.replyTo().tell(new CoordinatorResponse.JobAccepted(job));
        return this;
    }

    private Behavior<Messages.CoordinatorCommand> onCancel(CoordinatorCommand.CancelJob msg) {
        var job = repo.findById(msg.jobId()).orElse(null);
        if (job == null) {
            msg.replyTo().tell(new CoordinatorResponse.JobNotFound(msg.jobId()));
            return this;
        }
        var workerRef = workers.remove(msg.jobId());
        if (workerRef != null) workerRef.tell(new Messages.WorkerCommand.Cancel());

        var cancelled = job.withStatus(ReplayStatus.CANCELLED);
        repo.update(cancelled);
        msg.replyTo().tell(new CoordinatorResponse.JobSnapshot(cancelled));
        return this;
    }

    private Behavior<Messages.CoordinatorCommand> onGet(CoordinatorCommand.GetJob msg) {
        repo.findById(msg.jobId())
                .ifPresentOrElse(
                        job -> msg.replyTo().tell(new CoordinatorResponse.JobSnapshot(job)),
                        ()  -> msg.replyTo().tell(new CoordinatorResponse.JobNotFound(msg.jobId())));
        return this;
    }

    private Behavior<Messages.CoordinatorCommand> onList(CoordinatorCommand.ListJobs msg) {
        msg.replyTo().tell(new CoordinatorResponse.JobList(List.copyOf(repo.findAll())));
        return this;
    }

    private Behavior<Messages.CoordinatorCommand> onFinished(CoordinatorCommand.WorkerFinished msg) {
        repo.findById(msg.jobId()).ifPresent(job ->
                repo.update(job.withProgress(msg.eventsPublished())
                               .withStatus(ReplayStatus.COMPLETED)));
        getContext().getLog().info("Job {} completed, {} events published",
                msg.jobId(), msg.eventsPublished());
        return this;
    }

    private Behavior<Messages.CoordinatorCommand> onFailed(CoordinatorCommand.WorkerFailed msg) {
        repo.findById(msg.jobId()).ifPresent(job -> repo.update(job.failed(msg.reason())));
        getContext().getLog().error("Job {} failed: {}", msg.jobId(), msg.reason());
        return this;
    }
}
