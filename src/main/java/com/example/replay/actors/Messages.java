package com.example.replay.actors;

import com.example.replay.model.ReplayJob;
import com.example.replay.model.SecurityEvent;
import org.apache.pekko.actor.typed.ActorRef;

import java.util.List;

/**
 * All actor protocol messages, grouped as nested records inside sealed interfaces.
 *
 * <p>Hierarchy overview:
 * <pre>
 *   CoordinatorCommand  → JobManager
 *   CoordinatorResponse ← JobManager (replies to HTTP handlers)
 *   ReplayJobCommand    → ReplayJobActor  (per-job lifecycle)
 *   DataReaderCommand   → DataReaderActor (Iceberg batch reads)
 *   DataEmitterCommand  → DataEmitterActor (Kafka publishes)
 *   WorkerCommand       → ReplayWorker (legacy, kept for compatibility)
 * </pre>
 */
public final class Messages {

    private Messages() {}

    // =========================================================================
    // JobManager protocol
    // =========================================================================

    /**
     * Commands accepted by {@link JobManager} (and the legacy {@link ReplayCoordinator}).
     * New {@code PauseJob}, {@code ResumeJob}, {@code WorkerPaused}, {@code WorkerResumed}
     * are added for the full lifecycle; older handlers ignore unknown types.
     */
    public sealed interface CoordinatorCommand permits
            CoordinatorCommand.SubmitJob,
            CoordinatorCommand.PauseJob,
            CoordinatorCommand.ResumeJob,
            CoordinatorCommand.CancelJob,
            CoordinatorCommand.GetJob,
            CoordinatorCommand.ListJobs,
            CoordinatorCommand.WorkerFinished,
            CoordinatorCommand.WorkerFailed,
            CoordinatorCommand.WorkerPaused,
            CoordinatorCommand.WorkerResumed {

        record SubmitJob(ReplayJob job,
                         ActorRef<CoordinatorResponse> replyTo)   implements CoordinatorCommand {}

        record PauseJob(String jobId,
                        ActorRef<CoordinatorResponse> replyTo)    implements CoordinatorCommand {}

        record ResumeJob(String jobId,
                         ActorRef<CoordinatorResponse> replyTo)   implements CoordinatorCommand {}

        record CancelJob(String jobId,
                         ActorRef<CoordinatorResponse> replyTo)   implements CoordinatorCommand {}

        record GetJob(String jobId,
                      ActorRef<CoordinatorResponse> replyTo)      implements CoordinatorCommand {}

        record ListJobs(ActorRef<CoordinatorResponse> replyTo)    implements CoordinatorCommand {}

        /** Internal: {@link ReplayJobActor} / {@link ReplayWorker} signals completion. */
        record WorkerFinished(String jobId, long eventsPublished) implements CoordinatorCommand {}

        /** Internal: signals failure. */
        record WorkerFailed(String jobId, String reason)          implements CoordinatorCommand {}

        /** Internal: {@link ReplayJobActor} confirms it has paused. */
        record WorkerPaused(String jobId)                         implements CoordinatorCommand {}

        /** Internal: {@link ReplayJobActor} confirms it has resumed. */
        record WorkerResumed(String jobId)                        implements CoordinatorCommand {}
    }

    /** Responses sent back to HTTP handlers via {@code AskPattern}. */
    public sealed interface CoordinatorResponse permits
            CoordinatorResponse.JobAccepted,
            CoordinatorResponse.JobNotFound,
            CoordinatorResponse.JobSnapshot,
            CoordinatorResponse.JobList,
            CoordinatorResponse.JobPaused,
            CoordinatorResponse.JobResumed,
            CoordinatorResponse.Rejected {

        record JobAccepted(ReplayJob job)          implements CoordinatorResponse {}
        record JobNotFound(String jobId)           implements CoordinatorResponse {}
        record JobSnapshot(ReplayJob job)          implements CoordinatorResponse {}
        record JobList(List<ReplayJob> jobs)       implements CoordinatorResponse {}
        record JobPaused(ReplayJob job)            implements CoordinatorResponse {}
        record JobResumed(ReplayJob job)           implements CoordinatorResponse {}
        record Rejected(String reason)             implements CoordinatorResponse {}
    }

    // =========================================================================
    // ReplayJobActor protocol  (replaces WorkerCommand for new actor hierarchy)
    // =========================================================================

    /**
     * Commands for {@link ReplayJobActor}.
     *
     * <p>Lifecycle flow:
     * <pre>
     *   Start → (RUNNING) → Pause → (PAUSED) → Resume → (RUNNING) → ...
     *   BatchRead / BatchEmitted / BatchFailed / ReaderDone are internal signals
     *   between DataReaderActor → ReplayJobActor → DataEmitterActor.
     * </pre>
     */
    public sealed interface ReplayJobCommand permits
            ReplayJobCommand.Start,
            ReplayJobCommand.Pause,
            ReplayJobCommand.Resume,
            ReplayJobCommand.Cancel,
            ReplayJobCommand.BatchRead,
            ReplayJobCommand.BatchEmitted,
            ReplayJobCommand.BatchFailed,
            ReplayJobCommand.ReaderDone {

        /** Triggers the IDLE → RUNNING transition; spawns reader and emitter. */
        record Start()                                                   implements ReplayJobCommand {}
        record Pause()                                                   implements ReplayJobCommand {}
        record Resume()                                                  implements ReplayJobCommand {}
        record Cancel()                                                  implements ReplayJobCommand {}

        /** Internal: DataReaderActor delivers a batch. */
        record BatchRead(List<SecurityEvent> events, long seq)           implements ReplayJobCommand {}
        /** Internal: DataEmitterActor confirms a batch was published. */
        record BatchEmitted(long seq, long count)                        implements ReplayJobCommand {}
        /** Internal: DataEmitterActor reports a publish failure. */
        record BatchFailed(long seq, String reason)                      implements ReplayJobCommand {}
        /** Internal: DataReaderActor signals no more data. */
        record ReaderDone(long totalBatches)                             implements ReplayJobCommand {}
    }

    // =========================================================================
    // DataReaderActor protocol
    // =========================================================================

    /**
     * Commands for {@link DataReaderActor}.
     * {@code FetchBatch} is an internal timer tick — do not send from outside the actor.
     */
    public sealed interface DataReaderCommand permits
            DataReaderCommand.Start,
            DataReaderCommand.Pause,
            DataReaderCommand.Resume,
            DataReaderCommand.Cancel,
            DataReaderCommand.FetchBatch {

        record Start()      implements DataReaderCommand {}
        record Pause()      implements DataReaderCommand {}
        record Resume()     implements DataReaderCommand {}
        record Cancel()     implements DataReaderCommand {}
        /** Internal timer message — scheduled by the actor itself. */
        record FetchBatch() implements DataReaderCommand {}
    }

    // =========================================================================
    // DataEmitterActor protocol
    // =========================================================================

    /** Commands for {@link DataEmitterActor}. */
    public sealed interface DataEmitterCommand permits
            DataEmitterCommand.Emit,
            DataEmitterCommand.Cancel {

        record Emit(List<SecurityEvent> events, long seq) implements DataEmitterCommand {}
        record Cancel()                                   implements DataEmitterCommand {}
    }

    // =========================================================================
    // Legacy ReplayWorker protocol  (kept for backward-compatibility)
    // =========================================================================

    /** @deprecated Use {@link ReplayJobCommand} and the new actor hierarchy. */
    @Deprecated
    public sealed interface WorkerCommand permits
            WorkerCommand.Start,
            WorkerCommand.Cancel,
            WorkerCommand.EventBatch {

        record Start(ReplayJob job)                                    implements WorkerCommand {}
        record Cancel()                                                implements WorkerCommand {}
        record EventBatch(List<SecurityEvent> events, long batchSequence) implements WorkerCommand {}
    }
}
