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
            CoordinatorCommand.StartJob,
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

        /** Transitions a PENDING job to RUNNING and spawns its {@link ReplayJobActor}. */
        record StartJob(String jobId,
                        ActorRef<CoordinatorResponse> replyTo)    implements CoordinatorCommand {}

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
            CoordinatorResponse.JobStarted,
            CoordinatorResponse.JobNotFound,
            CoordinatorResponse.JobSnapshot,
            CoordinatorResponse.JobList,
            CoordinatorResponse.JobPaused,
            CoordinatorResponse.JobResumed,
            CoordinatorResponse.Rejected {

        record JobAccepted(ReplayJob job)          implements CoordinatorResponse {}
        record JobStarted(ReplayJob job)           implements CoordinatorResponse {}
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
     * <p>Lifecycle flow (work-distribution path):
     * <pre>
     *   Start → async planning → PlanReady → WorkerPoolActor dispatches packets
     *   PoolFinished / PoolFailed ← WorkerPoolActor (terminal signals)
     *   Pause / Resume → forwarded to WorkerPoolActor
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
            ReplayJobCommand.ReaderDone,
            ReplayJobCommand.PlanReady,
            ReplayJobCommand.PoolFinished,
            ReplayJobCommand.PoolFailed {

        /** Triggers the IDLE → planning transition. */
        record Start()                                                   implements ReplayJobCommand {}
        record Pause()                                                   implements ReplayJobCommand {}
        record Resume()                                                  implements ReplayJobCommand {}
        record Cancel()                                                  implements ReplayJobCommand {}

        /** Internal: DataReaderActor delivers a batch (legacy path). */
        record BatchRead(List<SecurityEvent> events, long seq)           implements ReplayJobCommand {}
        /** Internal: DataEmitterActor confirms a batch was published (legacy). */
        record BatchEmitted(long seq, long count)                        implements ReplayJobCommand {}
        /** Internal: DataEmitterActor reports a publish failure (legacy). */
        record BatchFailed(long seq, String reason)                      implements ReplayJobCommand {}
        /** Internal: DataReaderActor signals no more data (legacy). */
        record ReaderDone(long totalBatches)                             implements ReplayJobCommand {}

        /** Internal: async {@link WorkPlannerFn} result piped back to the actor. */
        record PlanReady(List<WorkPacket> packets, Throwable error)      implements ReplayJobCommand {}
        /** Internal: {@link WorkerPoolActor} signals all packets were processed. */
        record PoolFinished(long totalEvents)                            implements ReplayJobCommand {}
        /** Internal: {@link WorkerPoolActor} signals an unrecoverable failure. */
        record PoolFailed(String reason)                                 implements ReplayJobCommand {}
    }

    // =========================================================================
    // DataReaderActor protocol
    // =========================================================================

    /**
     * Commands for {@link DataReaderActor}.
     * {@code BatchReady} is an internal signal — piped back to the actor via
     * {@code pipeToSelf} after an async {@code DataLakeReader.readBatch} call.
     */
    public sealed interface DataReaderCommand permits
            DataReaderCommand.Start,
            DataReaderCommand.Pause,
            DataReaderCommand.Resume,
            DataReaderCommand.Cancel,
            DataReaderCommand.BatchReady {

        record Start()   implements DataReaderCommand {}
        record Pause()   implements DataReaderCommand {}
        record Resume()  implements DataReaderCommand {}
        record Cancel()  implements DataReaderCommand {}

        /**
         * Internal: result of an async {@code readBatch} call piped back to the actor.
         * Exactly one of {@code events} (non-null, possibly empty) or {@code error}
         * (non-null) will be set.
         */
        record BatchReady(java.util.List<SecurityEvent> events, long seq, Throwable error)
                implements DataReaderCommand {}
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
    // WorkerPoolActor protocol
    // =========================================================================

    /**
     * Commands for {@link WorkerPoolActor}.
     *
     * <p>{@code PacketDone} and {@code PacketFailed} carry the sender's
     * {@code ActorRef} so the pool knows which worker slot is now free.
     */
    public sealed interface WorkerPoolCommand permits
            WorkerPoolCommand.Start,
            WorkerPoolCommand.PacketDone,
            WorkerPoolCommand.PacketFailed,
            WorkerPoolCommand.Pause,
            WorkerPoolCommand.Resume,
            WorkerPoolCommand.Cancel {

        record Start()   implements WorkerPoolCommand {}
        record Pause()   implements WorkerPoolCommand {}
        record Resume()  implements WorkerPoolCommand {}
        record Cancel()  implements WorkerPoolCommand {}

        record PacketDone(String packetId, long eventsEmitted,
                          org.apache.pekko.actor.typed.ActorRef<Messages.PacketWorkerCommand> workerRef)
                implements WorkerPoolCommand {}

        record PacketFailed(String packetId, String reason,
                            org.apache.pekko.actor.typed.ActorRef<Messages.PacketWorkerCommand> workerRef)
                implements WorkerPoolCommand {}
    }

    // =========================================================================
    // PacketWorkerActor protocol
    // =========================================================================

    /**
     * Commands for {@link PacketWorkerActor}.
     *
     * <p>{@code BatchReady} is an internal pipeToSelf signal carrying the result
     * of an async {@code DataLakeReader.readBatch} call.
     */
    public sealed interface PacketWorkerCommand permits
            PacketWorkerCommand.Assign,
            PacketWorkerCommand.BatchReady,
            PacketWorkerCommand.Pause,
            PacketWorkerCommand.Resume,
            PacketWorkerCommand.Cancel {

        /** Assigns a work packet to this worker; triggers reading immediately. */
        record Assign(WorkPacket packet)  implements PacketWorkerCommand {}
        record Pause()                    implements PacketWorkerCommand {}
        record Resume()                   implements PacketWorkerCommand {}
        record Cancel()                   implements PacketWorkerCommand {}

        /** Internal pipeToSelf result. {@code events} is null when {@code error} is set. */
        record BatchReady(List<SecurityEvent> events, int batchIndex, Throwable error)
                implements PacketWorkerCommand {}
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
