package com.example.replay.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;

/**
 * HTTP request body used to submit a new replay job via the REST API.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ReplayRequest(
        String  sourceTable,
        String  targetTopic,
        Instant fromTime,
        Instant toTime,
        double  speedMultiplier   // defaults to 1.0 if omitted
) {
    /** Apply defaults for optional fields. */
    public ReplayRequest {
        if (speedMultiplier <= 0) speedMultiplier = 1.0;
    }
}
