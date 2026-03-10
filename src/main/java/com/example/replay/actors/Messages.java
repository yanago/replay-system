package com.example.replay.actors;

import com.example.replay.model.ReplayJob;
import com.example.replay.model.SecurityEvent;
import org.apache.pekko.actor.typed.ActorRef;

import java.util.List;

/**
 * All actor protocol messages, grouped as sealed sub-interfaces per actor.
 * Using sealed interfaces + records gives exhaustive pattern matching for free.
 */
public final class Messages {

    private Messages() {}

    // -----------------------------------------------------------------------
    // ReplayCoordinator protocol
    // -----------------------------------------------------------------------

    public sealed interface CoordinatorCommand permits
            CoordinatorCommand.SubmitJob,
            CoordinatorCommand.CancelJob,
            CoordinatorCommand.GetJob,
            CoordinatorCommand.ListJobs,
            CoordinatorCommand.WorkerFinished,
            CoordinatorCommand.WorkerFailed {}

    public static final class CoordinatorCommand {
        public record SubmitJob(ReplayJob job,
                                ActorRef<CoordinatorResponse> replyTo) implements CoordinatorCommand {}

        public record CancelJob(String jobId,
                                ActorRef<CoordinatorResponse> replyTo) implements CoordinatorCommand {}

        public record GetJob(String jobId,
                             ActorRef<CoordinatorResponse> replyTo) implements CoordinatorCommand {}

        public record ListJobs(ActorRef<CoordinatorResponse> replyTo) implements CoordinatorCommand {}

        /** Internal: worker signals successful completion. */
        public record WorkerFinished(String jobId, long eventsPublished) implements CoordinatorCommand {}

        /** Internal: worker signals failure. */
        public record WorkerFailed(String jobId, String reason) implements CoordinatorCommand {}
    }

    public sealed interface CoordinatorResponse permits
            CoordinatorResponse.JobAccepted,
            CoordinatorResponse.JobNotFound,
            CoordinatorResponse.JobSnapshot,
            CoordinatorResponse.JobList,
            CoordinatorResponse.Rejected {}

    public static final class CoordinatorResponse {
        public record JobAccepted(ReplayJob job) implements CoordinatorResponse {}
        public record JobNotFound(String jobId) implements CoordinatorResponse {}
        public record JobSnapshot(ReplayJob job) implements CoordinatorResponse {}
        public record JobList(List<ReplayJob> jobs) implements CoordinatorResponse {}
        public record Rejected(String reason) implements CoordinatorResponse {}
    }

    // -----------------------------------------------------------------------
    // ReplayWorker protocol
    // -----------------------------------------------------------------------

    public sealed interface WorkerCommand permits
            WorkerCommand.Start,
            WorkerCommand.Cancel,
            WorkerCommand.EventBatch {}

    public static final class WorkerCommand {
        public record Start(ReplayJob job) implements WorkerCommand {}
        public record Cancel() implements WorkerCommand {}
        public record EventBatch(List<SecurityEvent> events, long batchSequence) implements WorkerCommand {}
    }
}
