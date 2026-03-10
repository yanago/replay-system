package com.example.replay.model;

import java.time.Instant;

/**
 * Represents a replay request: a time-bounded slice of events from the data
 * lake to be published to a Kafka topic at a configurable speed multiplier.
 */
public record ReplayJob(
        String       jobId,
        String       sourceTable,     // Iceberg table name in the data lake
        String       targetTopic,     // Kafka topic to publish to
        Instant      fromTime,
        Instant      toTime,
        double       speedMultiplier, // 1.0 = real-time, 10.0 = 10× faster
        ReplayStatus status,
        Instant      createdAt,
        Instant      updatedAt,
        long         eventsPublished,
        String       errorMessage     // null when no error
) {
    /** Convenience factory for a new job in PENDING state. */
    public static ReplayJob create(
            String jobId,
            String sourceTable,
            String targetTopic,
            Instant fromTime,
            Instant toTime,
            double speedMultiplier) {
        var now = Instant.now();
        return new ReplayJob(jobId, sourceTable, targetTopic,
                fromTime, toTime, speedMultiplier,
                ReplayStatus.PENDING, now, now, 0L, null);
    }

    public ReplayJob withStatus(ReplayStatus s) {
        return new ReplayJob(jobId, sourceTable, targetTopic,
                fromTime, toTime, speedMultiplier,
                s, createdAt, Instant.now(), eventsPublished, errorMessage);
    }

    public ReplayJob withProgress(long published) {
        return new ReplayJob(jobId, sourceTable, targetTopic,
                fromTime, toTime, speedMultiplier,
                status, createdAt, Instant.now(), published, errorMessage);
    }

    public ReplayJob failed(String reason) {
        return new ReplayJob(jobId, sourceTable, targetTopic,
                fromTime, toTime, speedMultiplier,
                ReplayStatus.FAILED, createdAt, Instant.now(), eventsPublished, reason);
    }
}
