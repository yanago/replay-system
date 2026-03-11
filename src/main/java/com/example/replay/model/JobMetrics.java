package com.example.replay.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

/**
 * Read-only snapshot returned by {@code GET /api/v1/replay/jobs/{id}/metrics}.
 *
 * <h3>Throughput</h3>
 * {@code events_per_second} is computed over a {@code window_seconds}-wide
 * sliding window (default 60 s).  It is 0 when the job has not yet published
 * any events within that window.
 *
 * <h3>Latency</h3>
 * {@code avg_batch_latency_ms} and {@code p99_batch_latency_ms} measure the
 * total wall-clock time from the start of a data-lake fetch to the completion
 * of the Kafka + HTTP publish for the same batch.  P99 is computed over the
 * most recent 1 000 completed batches.
 *
 * <h3>Errors</h3>
 * {@code total_read_errors} counts failed {@code DataLakeReader.readBatch()}
 * calls; {@code total_publish_errors} counts failed Kafka/HTTP publish futures.
 * Either error type causes the affected packet worker to stop and report
 * {@link Messages.WorkerPoolCommand.PacketFailed} to the pool.
 */
public record JobMetrics(
        @JsonProperty("job_id")                  String  jobId,
        @JsonProperty("events_per_second")        double  eventsPerSecond,
        @JsonProperty("avg_batch_latency_ms")     double  avgBatchLatencyMs,
        @JsonProperty("p99_batch_latency_ms")     double  p99BatchLatencyMs,
        @JsonProperty("total_events_published")   long    totalEventsPublished,
        @JsonProperty("total_batches_processed")  long    totalBatchesProcessed,
        @JsonProperty("total_read_errors")        long    totalReadErrors,
        @JsonProperty("total_publish_errors")     long    totalPublishErrors,
        @JsonProperty("window_seconds")           int     windowSeconds,
        @JsonProperty("sampled_at")               Instant sampledAt
) {
    /** Empty snapshot for jobs with no live metrics (e.g. PENDING). */
    public static JobMetrics empty(String jobId) {
        return new JobMetrics(jobId, 0.0, 0.0, 0.0, 0L, 0L, 0L, 0L, 60, Instant.now());
    }
}
