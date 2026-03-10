package com.example.replay.actors;

import com.example.replay.actors.Messages.CoordinatorCommand;
import com.example.replay.actors.Messages.CoordinatorResponse;
import com.example.replay.model.ReplayJob;
import com.example.replay.model.ReplayStatus;
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
 */
public final class ReplayCoordinator
        extends AbstractBehavior<Messages.CoordinatorCommand> {

    // In-memory job registry; persistence is delegated to the repository layer
    private final Map<String, ReplayJob>                        jobs    = new HashMap<>();
    // Worker refs kept separately so cancel can send directly without a getChild() cast
    private final Map<String, ActorRef<Messages.WorkerCommand>> workers = new HashMap<>();

    // -----------------------------------------------------------------------
    // Factory
    // -----------------------------------------------------------------------

    public static Behavior<Messages.CoordinatorCommand> create() {
        return Behaviors.setup(ReplayCoordinator::new);
    }

    private ReplayCoordinator(ActorContext<Messages.CoordinatorCommand> ctx) {
        super(ctx);
    }

    // -----------------------------------------------------------------------
    // Message dispatch
    // -----------------------------------------------------------------------

    @Override
    public Receive<Messages.CoordinatorCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(CoordinatorCommand.SubmitJob.class,  this::onSubmit)
                .onMessage(CoordinatorCommand.CancelJob.class,  this::onCancel)
                .onMessage(CoordinatorCommand.GetJob.class,     this::onGet)
                .onMessage(CoordinatorCommand.ListJobs.class,   this::onList)
                .onMessage(CoordinatorCommand.WorkerFinished.class, this::onFinished)
                .onMessage(CoordinatorCommand.WorkerFailed.class,   this::onFailed)
                .build();
    }

    // -----------------------------------------------------------------------
    // Handlers
    // -----------------------------------------------------------------------

    private Behavior<Messages.CoordinatorCommand> onSubmit(CoordinatorCommand.SubmitJob msg) {
        var job = msg.job().withStatus(ReplayStatus.RUNNING);
        jobs.put(job.jobId(), job);

        // Spawn a dedicated worker actor for this job; keep the typed ref for later cancellation
        var workerRef = getContext().spawn(
                ReplayWorker.create(getContext().getSelf()),
                "worker-" + job.jobId()
        );
        workers.put(job.jobId(), workerRef);
        workerRef.tell(new Messages.WorkerCommand.Start(job));

        getContext().getLog().info("Job {} submitted, worker spawned", job.jobId());
        msg.replyTo().tell(new CoordinatorResponse.JobAccepted(job));
        return this;
    }

    private Behavior<Messages.CoordinatorCommand> onCancel(CoordinatorCommand.CancelJob msg) {
        var job = jobs.get(msg.jobId());
        if (job == null) {
            msg.replyTo().tell(new CoordinatorResponse.JobNotFound(msg.jobId()));
            return this;
        }
        // Signal worker to stop via the typed ref we kept at spawn time
        var workerRef = workers.remove(msg.jobId());
        if (workerRef != null) workerRef.tell(new Messages.WorkerCommand.Cancel());
        var cancelled = job.withStatus(ReplayStatus.CANCELLED);
        jobs.put(msg.jobId(), cancelled);
        msg.replyTo().tell(new CoordinatorResponse.JobSnapshot(cancelled));
        return this;
    }

    private Behavior<Messages.CoordinatorCommand> onGet(CoordinatorCommand.GetJob msg) {
        var job = jobs.get(msg.jobId());
        if (job == null) {
            msg.replyTo().tell(new CoordinatorResponse.JobNotFound(msg.jobId()));
        } else {
            msg.replyTo().tell(new CoordinatorResponse.JobSnapshot(job));
        }
        return this;
    }

    private Behavior<Messages.CoordinatorCommand> onList(CoordinatorCommand.ListJobs msg) {
        msg.replyTo().tell(new CoordinatorResponse.JobList(List.copyOf(jobs.values())));
        return this;
    }

    private Behavior<Messages.CoordinatorCommand> onFinished(CoordinatorCommand.WorkerFinished msg) {
        jobs.computeIfPresent(msg.jobId(), (id, job) ->
                job.withProgress(msg.eventsPublished()).withStatus(ReplayStatus.COMPLETED));
        getContext().getLog().info("Job {} completed, {} events published", msg.jobId(), msg.eventsPublished());
        return this;
    }

    private Behavior<Messages.CoordinatorCommand> onFailed(CoordinatorCommand.WorkerFailed msg) {
        jobs.computeIfPresent(msg.jobId(), (id, job) -> job.failed(msg.reason()));
        getContext().getLog().error("Job {} failed: {}", msg.jobId(), msg.reason());
        return this;
    }
}
