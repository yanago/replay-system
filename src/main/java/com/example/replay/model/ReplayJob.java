package com.example.replay.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

/**
 * Tracks parameters and lifecycle state of a single replay operation.
 *
 * <p>A job selects a time-bounded slice of {@link SecurityEvent}s from an
 * Iceberg table and publishes them to a Kafka topic at a configurable
 * speed multiplier (1.0 = real-time, 10.0 = 10× faster).
 *
 * <p>JSON field names are snake_case; Java accessors are camelCase.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ReplayJob(
        @JsonProperty("job_id")           String       jobId,
        @JsonProperty("source_table")     String       sourceTable,
        @JsonProperty("target_topic")     String       targetTopic,
        @JsonProperty("from_time")        Instant      fromTime,
        @JsonProperty("to_time")          Instant      toTime,
        @JsonProperty("speed_multiplier") double       speedMultiplier,
        @JsonProperty("status")           ReplayStatus status,
        @JsonProperty("created_at")       Instant      createdAt,
        @JsonProperty("updated_at")       Instant      updatedAt,
        @JsonProperty("events_published") long         eventsPublished,
        @JsonProperty("error_message")    String       errorMessage
) {
    /** Factory — creates a new job in {@link ReplayStatus#PENDING} state. */
    public static ReplayJob create(
            String  jobId,
            String  sourceTable,
            String  targetTopic,
            Instant fromTime,
            Instant toTime,
            double  speedMultiplier) {
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
