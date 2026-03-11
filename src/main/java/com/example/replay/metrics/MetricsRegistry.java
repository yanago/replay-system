package com.example.replay.metrics;

import com.example.replay.model.JobMetrics;
import com.example.replay.model.JobStatus;
import com.example.replay.model.ReplayJob;
import com.example.replay.model.ReplayStatus;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Central, thread-safe store for live per-job metrics.
 *
 * <h3>Design</h3>
 * Actors write to the registry directly (no actor-message roundtrip) using the
 * {@code record*()} methods — all writes are lock-free.  HTTP handlers read
 * snapshots via {@link #buildStatus} / {@link #getMetrics}, also without
 * acquiring any locks.
 *
 * <h3>Lifecycle</h3>
 * <ol>
 *   <li>{@link ReplayJobActor} calls {@link #registerJob} when planning is done
 *       and the worker pool is about to start.</li>
 *   <li>{@link PacketWorkerActor} calls {@link #recordBatch}, {@link #recordReadError},
 *       {@link #recordPublishError} on every batch cycle.</li>
 *   <li>{@link WorkerPoolActor} calls {@link #recordPacketDone} each time a
 *       {@link PacketWorkerActor} finishes its packet.</li>
 *   <li>State entries are kept indefinitely in memory for query purposes (the
 *       set is small — one entry per started job).</li>
 * </ol>
 */
public final class MetricsRegistry {

    /** Width of the EPS sliding window in seconds. */
    static final int EPS_WINDOW_SECONDS = 60;

    private final ConcurrentHashMap<String, JobMetricsState> states = new ConcurrentHashMap<>();

    // -----------------------------------------------------------------------
    // Write side (called from actors)
    // -----------------------------------------------------------------------

    /**
     * Initialises metrics state for a new job and sets the expected packet
     * count so that progress percentage can be computed.
     */
    public void registerJob(String jobId, int packetCount) {
        var state = states.computeIfAbsent(jobId, JobMetricsState::new);
        state.packetsTotal.set(packetCount);
    }

    /**
     * Records one completed read→publish cycle.
     *
     * @param jobId            owning job
     * @param eventsPublished  events successfully sent in this batch
     * @param fetchMs          wall-clock milliseconds spent reading from the data lake
     * @param publishMs        wall-clock milliseconds spent publishing (Kafka + HTTP)
     */
    public void recordBatch(String jobId, int eventsPublished, long fetchMs, long publishMs) {
        var state = states.get(jobId);
        if (state == null) return;

        state.totalEvents.add(eventsPublished);
        state.totalBatches.increment();
        state.totalFetchMs.add(fetchMs);
        state.totalPublishMs.add(publishMs);

        // EPS window
        long now = System.currentTimeMillis();
        state.epsWindow.addLast(new long[]{now, eventsPublished});
        long cutoff = now - (long) EPS_WINDOW_SECONDS * 1_000;
        // Prune from the front (oldest first)
        while (!state.epsWindow.isEmpty() && state.epsWindow.peekFirst()[0] < cutoff) {
            state.epsWindow.pollFirst();
        }

        // P99 latency samples (fetch + publish combined)
        state.recentBatchLatencies.addLast(fetchMs + publishMs);
        if (state.recentBatchLatencies.size() > JobMetricsState.MAX_LATENCY_SAMPLES) {
            state.recentBatchLatencies.pollFirst();
        }
    }

    /** Increments the count of completed work packets for this job. */
    public void recordPacketDone(String jobId) {
        var state = states.get(jobId);
        if (state != null) state.packetsCompleted.incrementAndGet();
    }

    /** Increments the data-lake read error counter. */
    public void recordReadError(String jobId) {
        var state = states.get(jobId);
        if (state != null) state.readErrors.increment();
    }

    /** Increments the Kafka/HTTP publish error counter. */
    public void recordPublishError(String jobId) {
        var state = states.get(jobId);
        if (state != null) state.publishErrors.increment();
    }

    // -----------------------------------------------------------------------
    // Read side (called from HTTP handlers — no locks)
    // -----------------------------------------------------------------------

    /**
     * Builds a {@link JobStatus} by merging live registry state with the
     * persisted {@link ReplayJob} from the repository.
     *
     * <p>If no registry entry exists (PENDING or pre-start state) the status
     * is derived solely from the repository record.
     */
    public JobStatus buildStatus(ReplayJob job) {
        var state = states.get(job.jobId());

        long   totalEvents       = state != null ? state.totalEvents.sum()           : job.eventsPublished();
        int    packetsCompleted  = state != null ? state.packetsCompleted.get()       : 0;
        int    packetsTotal      = state != null ? state.packetsTotal.get()           : 0;
        Instant startedAt        = state != null ? state.startedAt                   : job.createdAt();

        double progressPct;
        if (job.status() == ReplayStatus.COMPLETED) {
            progressPct = 100.0;
        } else if (packetsTotal > 0) {
            progressPct = Math.min(100.0, (double) packetsCompleted / packetsTotal * 100.0);
        } else {
            progressPct = 0.0;
        }

        double elapsedSeconds = Duration.between(startedAt, Instant.now()).toMillis() / 1_000.0;

        Double etaSeconds = null;
        if (progressPct > 0 && progressPct < 100 && job.status() == ReplayStatus.RUNNING) {
            etaSeconds = elapsedSeconds * (100.0 - progressPct) / progressPct;
        }

        return new JobStatus(
                job.jobId(),
                job.status(),
                totalEvents,
                packetsCompleted,
                packetsTotal,
                progressPct,
                elapsedSeconds,
                etaSeconds,
                startedAt,
                job.updatedAt());
    }

    /**
     * Returns a live {@link JobMetrics} snapshot for the given job, or
     * {@link Optional#empty()} if no registry entry exists (e.g. PENDING).
     */
    public Optional<JobMetrics> getMetrics(String jobId) {
        var state = states.get(jobId);
        if (state == null) return Optional.empty();
        return Optional.of(snapshot(state));
    }

    // -----------------------------------------------------------------------
    // Snapshot helpers
    // -----------------------------------------------------------------------

    private static JobMetrics snapshot(JobMetricsState state) {
        long batches = state.totalBatches.sum();

        double avgBatchMs = batches > 0
                ? (double)(state.totalFetchMs.sum() + state.totalPublishMs.sum()) / batches
                : 0.0;

        // P99: collect sorted copy of recent latencies
        List<Long> sorted = state.recentBatchLatencies.stream().sorted().toList();
        double p99 = sorted.isEmpty() ? 0.0
                : sorted.get(Math.max(0, (int)(sorted.size() * 0.99) - 1)).doubleValue();

        // EPS: sum events in the last EPS_WINDOW_SECONDS
        long cutoff = System.currentTimeMillis() - (long) EPS_WINDOW_SECONDS * 1_000;
        long recentEvents = state.epsWindow.stream()
                .filter(e -> e[0] >= cutoff)
                .mapToLong(e -> e[1])
                .sum();
        double eps = (double) recentEvents / EPS_WINDOW_SECONDS;

        return new JobMetrics(
                state.jobId,
                eps,
                avgBatchMs,
                p99,
                state.totalEvents.sum(),
                batches,
                state.readErrors.sum(),
                state.publishErrors.sum(),
                EPS_WINDOW_SECONDS,
                Instant.now());
    }
}
