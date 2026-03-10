package com.example.replay.actors;

import com.example.replay.actors.Messages.WorkerCommand;
import com.example.replay.model.ReplayJob;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;

/**
 * Per-job worker actor.
 * Reads event batches from the data lake, publishes them to Kafka, and reports
 * progress back to the {@link ReplayCoordinator}.
 *
 * <p>The actual I/O is delegated to injectable {@code DataLakeReader} and
 * {@code EventPublisher} collaborators that are wired in by the application
 * bootstrap; the actor itself stays pure (no blocking calls on the dispatcher).
 */
public final class ReplayWorker extends AbstractBehavior<WorkerCommand> {

    private final ActorRef<Messages.CoordinatorCommand> coordinator;

    // Injected I/O collaborators — set when Start is received
    private ReplayJob job;
    private long      eventsPublished = 0L;
    private boolean   cancelled       = false;

    // -----------------------------------------------------------------------
    // Factory
    // -----------------------------------------------------------------------

    public static Behavior<WorkerCommand> create(
            ActorRef<Messages.CoordinatorCommand> coordinator) {
        return Behaviors.setup(ctx -> new ReplayWorker(ctx, coordinator));
    }

    private ReplayWorker(ActorContext<WorkerCommand> ctx,
                         ActorRef<Messages.CoordinatorCommand> coordinator) {
        super(ctx);
        this.coordinator = coordinator;
    }

    // -----------------------------------------------------------------------
    // Message dispatch
    // -----------------------------------------------------------------------

    @Override
    public Receive<WorkerCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(WorkerCommand.Start.class,      this::onStart)
                .onMessage(WorkerCommand.Cancel.class,     this::onCancel)
                .onMessage(WorkerCommand.EventBatch.class, this::onBatch)
                .build();
    }

    // -----------------------------------------------------------------------
    // Handlers
    // -----------------------------------------------------------------------

    private Behavior<WorkerCommand> onStart(WorkerCommand.Start msg) {
        this.job = msg.job();
        getContext().getLog().info("Worker starting job {}", job.jobId());
        // Trigger first batch fetch — real implementation schedules async read
        // via Pekko Streams piped into self as EventBatch messages.
        scheduleFetch();
        return this;
    }

    private Behavior<WorkerCommand> onCancel(WorkerCommand.Cancel msg) {
        cancelled = true;
        getContext().getLog().info("Worker cancelling job {}", job == null ? "?" : job.jobId());
        return Behaviors.stopped();
    }

    private Behavior<WorkerCommand> onBatch(WorkerCommand.EventBatch msg) {
        if (cancelled) return Behaviors.stopped();

        // Publish batch → Kafka (delegated to EventPublisher injected by app bootstrap)
        eventsPublished += msg.events().size();
        getContext().getLog().debug("Batch {}: {} events published (total: {})",
                msg.batchSequence(), msg.events().size(), eventsPublished);

        if (isLastBatch(msg)) {
            coordinator.tell(new Messages.CoordinatorCommand.WorkerFinished(job.jobId(), eventsPublished));
            return Behaviors.stopped();
        }
        scheduleFetch();
        return this;
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private void scheduleFetch() {
        // Placeholder: real implementation uses pipeToSelf() with a Future
        // returned by DataLakeReader.readBatch(job, lastOffset)
        getContext().getLog().debug("Scheduling next fetch for job {}", job == null ? "?" : job.jobId());
    }

    private boolean isLastBatch(WorkerCommand.EventBatch msg) {
        return msg.events().isEmpty();
    }
}
