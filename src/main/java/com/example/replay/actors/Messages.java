package com.example.replay.actors;

import com.example.replay.model.ReplayJob;
import com.example.replay.model.SecurityEvent;
import org.apache.pekko.actor.typed.ActorRef;

import java.util.List;

/**
 * All actor protocol messages, grouped as nested types inside each sealed
 * interface — the standard Java 21 sealed-hierarchy pattern.
 *
 * <p>Usage: {@code CoordinatorCommand.SubmitJob}, {@code CoordinatorResponse.JobAccepted}, …
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
            CoordinatorCommand.WorkerFailed {

        record SubmitJob(ReplayJob job,
                         ActorRef<CoordinatorResponse> replyTo) implements CoordinatorCommand {}

        record CancelJob(String jobId,
                         ActorRef<CoordinatorResponse> replyTo) implements CoordinatorCommand {}

        record GetJob(String jobId,
                      ActorRef<CoordinatorResponse> replyTo) implements CoordinatorCommand {}

        record ListJobs(ActorRef<CoordinatorResponse> replyTo) implements CoordinatorCommand {}

        /** Internal: worker signals successful completion. */
        record WorkerFinished(String jobId, long eventsPublished) implements CoordinatorCommand {}

        /** Internal: worker signals failure. */
        record WorkerFailed(String jobId, String reason) implements CoordinatorCommand {}
    }

    public sealed interface CoordinatorResponse permits
            CoordinatorResponse.JobAccepted,
            CoordinatorResponse.JobNotFound,
            CoordinatorResponse.JobSnapshot,
            CoordinatorResponse.JobList,
            CoordinatorResponse.Rejected {

        record JobAccepted(ReplayJob job)         implements CoordinatorResponse {}
        record JobNotFound(String jobId)          implements CoordinatorResponse {}
        record JobSnapshot(ReplayJob job)         implements CoordinatorResponse {}
        record JobList(List<ReplayJob> jobs)      implements CoordinatorResponse {}
        record Rejected(String reason)            implements CoordinatorResponse {}
    }

    // -----------------------------------------------------------------------
    // ReplayWorker protocol
    // -----------------------------------------------------------------------

    public sealed interface WorkerCommand permits
            WorkerCommand.Start,
            WorkerCommand.Cancel,
            WorkerCommand.EventBatch {

        record Start(ReplayJob job) implements WorkerCommand {}
        record Cancel()             implements WorkerCommand {}
        record EventBatch(List<SecurityEvent> events,
                          long batchSequence)  implements WorkerCommand {}
    }
}
