package com.example.replay.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable domain model for a single security event read from the data lake.
 *
 * <h3>Required fields</h3>
 * <ul>
 *   <li>{@code event_id}        – unique identifier for this event</li>
 *   <li>{@code cid}             – customer / tenant identifier</li>
 *   <li>{@code event_timestamp} – wall-clock time the event was ingested / recorded</li>
 *   <li>{@code event_time}      – time the event actually occurred (may precede ingestion)</li>
 *   <li>{@code event_type}      – classification label, e.g. {@code LOGIN_FAILURE}</li>
 * </ul>
 *
 * <p>JSON field names are snake_case; Java accessors are camelCase (record convention).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record SecurityEvent(
        @JsonProperty("event_id")        String              eventId,
        @JsonProperty("cid")             String              cid,
        @JsonProperty("event_timestamp") Instant             eventTimestamp,
        @JsonProperty("event_time")      Instant             eventTime,
        @JsonProperty("event_type")      String              eventType,
        @JsonProperty("source_ip")       String              sourceIp,
        @JsonProperty("target_host")     String              targetHost,
        @JsonProperty("severity")        String              severity,
        @JsonProperty("attributes")      Map<String, String> attributes
) {
    /** Compact constructor — validates required fields and applies defaults for optional ones. */
    public SecurityEvent {
        Objects.requireNonNull(eventId,        "event_id is required");
        Objects.requireNonNull(cid,            "cid is required");
        Objects.requireNonNull(eventTimestamp, "event_timestamp is required");
        Objects.requireNonNull(eventTime,      "event_time is required");
        Objects.requireNonNull(eventType,      "event_type is required");
        if (sourceIp   == null) sourceIp   = "";
        if (targetHost == null) targetHost = "";
        if (severity   == null) severity   = "UNKNOWN";
        if (attributes == null) attributes = Map.of();
    }

    /** Compact one-line description for logging. */
    public String summary() {
        return "[cid=%s id=%s] %s @ %s".formatted(cid, eventId, eventType, eventTime);
    }
}
