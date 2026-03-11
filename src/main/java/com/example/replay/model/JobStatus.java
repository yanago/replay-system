package com.example.replay.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

/**
 * Read-only snapshot returned by {@code GET /api/v1/replay/jobs/{id}/status}.
 *
 * <p>Combines persisted state from {@link ReplayJob} (status enum, timestamps)
 * with live in-memory metrics (packets progress, ETA).
 *
 * <h3>Fields</h3>
 * <ul>
 *   <li>{@code progress_pct}     – percentage of work packets completed (0–100)</li>
 *   <li>{@code eta_seconds}      – estimated remaining time; {@code null} when
 *       the job is not RUNNING or progress is 0 %</li>
 *   <li>{@code elapsed_seconds}  – wall-clock seconds since the job started</li>
 * </ul>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record JobStatus(
        @JsonProperty("job_id")             String       jobId,
        @JsonProperty("status")             ReplayStatus status,
        @JsonProperty("events_published")   long         eventsPublished,
        @JsonProperty("packets_completed")  int          packetsCompleted,
        @JsonProperty("packets_total")      int          packetsTotal,
        @JsonProperty("progress_pct")       double       progressPct,
        @JsonProperty("elapsed_seconds")    double       elapsedSeconds,
        @JsonProperty("eta_seconds")        Double       etaSeconds,
        @JsonProperty("started_at")         Instant      startedAt,
        @JsonProperty("updated_at")         Instant      updatedAt
) {}
