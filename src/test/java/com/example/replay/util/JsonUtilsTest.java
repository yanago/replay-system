package com.example.replay.util;

import com.example.replay.model.SecurityEvent;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JsonUtilsTest {

    @Test
    void roundTripSecurityEvent() {
        var event = new SecurityEvent(
                "evt-1", "LOGIN_FAILURE", "1.2.3.4", "host-a", "HIGH",
                Instant.parse("2024-06-01T12:00:00Z"),
                Map.of("user", "alice"));

        var json      = JsonUtils.toJson(event);
        var roundTrip = JsonUtils.fromJson(json, SecurityEvent.class);

        assertEquals(event.eventId(),   roundTrip.eventId());
        assertEquals(event.eventType(), roundTrip.eventType());
        assertEquals(event.severity(),  roundTrip.severity());
        assertEquals(event.occurredAt(),roundTrip.occurredAt());
    }
}
