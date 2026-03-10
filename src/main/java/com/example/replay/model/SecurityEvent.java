package com.example.replay.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.util.Map;

/**
 * Immutable domain model for a single security event read from the data lake.
 * Jackson-serialisable via public record accessors.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record SecurityEvent(
        String  eventId,
        String  eventType,       // e.g. "LOGIN_FAILURE", "PORT_SCAN", …
        String  sourceIp,
        String  targetHost,
        String  severity,        // LOW | MEDIUM | HIGH | CRITICAL
        Instant occurredAt,
        Map<String, String> attributes   // arbitrary key/value bag
) {
    /** Compact description for logging. */
    public String summary() {
        return "[%s] %s @ %s -> %s (%s)".formatted(eventId, eventType, sourceIp, targetHost, severity);
    }
}
