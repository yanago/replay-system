package com.example.replay.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

/**
 * Deserialised body for {@code POST /api/v1/replay/jobs}.
 *
 * <p>All fields are nullable — validation is handled separately by
 * {@link JobValidator} so error messages are detailed and field-specific.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record JobRequest(
        @JsonProperty("source_table")     String  sourceTable,
        @JsonProperty("target_topic")     String  targetTopic,
        @JsonProperty("from_time")        Instant fromTime,
        @JsonProperty("to_time")          Instant toTime,
        @JsonProperty("speed_multiplier") Double  speedMultiplier  // nullable → defaults to 1.0
) {
    /** Returns {@code speed_multiplier} or 1.0 if absent / non-positive. */
    public double effectiveSpeed() {
        return (speedMultiplier != null && speedMultiplier > 0) ? speedMultiplier : 1.0;
    }
}
