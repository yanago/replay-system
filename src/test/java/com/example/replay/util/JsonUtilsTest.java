package com.example.replay.util;

import com.example.replay.model.ReplayJob;
import com.example.replay.model.ReplayStatus;
import com.example.replay.model.SecurityEvent;
import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JsonUtilsTest {

    private static final Instant TS  = Instant.parse("2024-06-01T12:00:00Z");
    private static final Instant EVT = Instant.parse("2024-06-01T11:59:55Z");

    private static SecurityEvent sampleEvent() {
        return new SecurityEvent(
                "evt-001",                   // event_id
                "tenant-42",                 // cid
                TS,                          // event_timestamp (ingestion time)
                EVT,                         // event_time      (occurrence time)
                "LOGIN_FAILURE",             // event_type
                "192.168.1.10",              // source_ip
                "auth-server-prod",          // target_host
                "HIGH",                      // severity
                Map.of("user", "alice", "attempts", "5")  // attributes
        );
    }

    // -----------------------------------------------------------------------
    // SecurityEvent round-trip
    // -----------------------------------------------------------------------

    @Test
    void toJson_producesSnakeCaseFieldNames() {
        var json = JsonUtils.toJson(sampleEvent());
        assertTrue(json.contains("\"event_id\""),        json);
        assertTrue(json.contains("\"cid\""),             json);
        assertTrue(json.contains("\"event_timestamp\""), json);
        assertTrue(json.contains("\"event_time\""),      json);
        assertTrue(json.contains("\"event_type\""),      json);
    }

    @Test
    void fromJson_string_roundTripsSecurityEvent() {
        var event     = sampleEvent();
        var json      = JsonUtils.toJson(event);
        var roundTrip = JsonUtils.fromJson(json, SecurityEvent.class);

        assertEquals(event.eventId(),        roundTrip.eventId());
        assertEquals(event.cid(),            roundTrip.cid());
        assertEquals(event.eventTimestamp(), roundTrip.eventTimestamp());
        assertEquals(event.eventTime(),      roundTrip.eventTime());
        assertEquals(event.eventType(),      roundTrip.eventType());
        assertEquals(event.severity(),       roundTrip.severity());
        assertEquals(event.attributes(),     roundTrip.attributes());
    }

    @Test
    void securityEvent_nullOptionalFieldsGetDefaults() {
        // sourceIp, targetHost, severity, attributes are optional
        var event = new SecurityEvent("e1", "cid1", TS, EVT, "PORT_SCAN",
                null, null, null, null);
        assertEquals("",        event.sourceIp());
        assertEquals("",        event.targetHost());
        assertEquals("UNKNOWN", event.severity());
        assertEquals(Map.of(),  event.attributes());
    }

    @Test
    void securityEvent_missingRequiredField_throwsNPE() {
        assertThrows(NullPointerException.class,
                () -> new SecurityEvent(null, "cid1", TS, EVT, "TYPE", "", "", "LOW", Map.of()));
        assertThrows(NullPointerException.class,
                () -> new SecurityEvent("e1", null, TS, EVT, "TYPE", "", "", "LOW", Map.of()));
        assertThrows(NullPointerException.class,
                () -> new SecurityEvent("e1", "cid1", null, EVT, "TYPE", "", "", "LOW", Map.of()));
        assertThrows(NullPointerException.class,
                () -> new SecurityEvent("e1", "cid1", TS, null, "TYPE", "", "", "LOW", Map.of()));
        assertThrows(NullPointerException.class,
                () -> new SecurityEvent("e1", "cid1", TS, EVT, null, "", "", "LOW", Map.of()));
    }

    // -----------------------------------------------------------------------
    // ReplayJob round-trip
    // -----------------------------------------------------------------------

    @Test
    void toJson_producesSnakeCaseReplayJob() {
        var job  = ReplayJob.create("job-1", "db.events", "replay-topic", EVT, TS, 2.0);
        var json = JsonUtils.toJson(job);
        assertTrue(json.contains("\"job_id\""),           json);
        assertTrue(json.contains("\"source_table\""),     json);
        assertTrue(json.contains("\"target_topic\""),     json);
        assertTrue(json.contains("\"speed_multiplier\""), json);
        assertTrue(json.contains("\"events_published\""), json);
    }

    @Test
    void fromJson_string_roundTripsReplayJob() {
        var job      = ReplayJob.create("job-2", "db.events", "out-topic", EVT, TS, 1.0);
        var json     = JsonUtils.toJson(job);
        var restored = JsonUtils.fromJson(json, ReplayJob.class);

        assertEquals(job.jobId(),          restored.jobId());
        assertEquals(job.sourceTable(),    restored.sourceTable());
        assertEquals(job.targetTopic(),    restored.targetTopic());
        assertEquals(job.speedMultiplier(),restored.speedMultiplier(), 1e-9);
        assertEquals(ReplayStatus.PENDING, restored.status());
    }

    // -----------------------------------------------------------------------
    // byte[] overloads
    // -----------------------------------------------------------------------

    @Test
    void toBytes_fromBytes_roundTrip() {
        var event = sampleEvent();
        var bytes = JsonUtils.toBytes(event);
        var back  = JsonUtils.fromJson(bytes, SecurityEvent.class);
        assertEquals(event.eventId(), back.eventId());
        assertEquals(event.cid(),     back.cid());
    }

    @Test
    void fromJson_bytes_withTypeRef() {
        var events = List.of(sampleEvent());
        var bytes  = JsonUtils.toBytes(events);
        List<SecurityEvent> back = JsonUtils.fromJson(bytes, new TypeReference<>() {});
        assertEquals(1,                     back.size());
        assertEquals("evt-001",             back.get(0).eventId());
    }

    // -----------------------------------------------------------------------
    // InputStream overload
    // -----------------------------------------------------------------------

    @Test
    void fromJson_inputStream_deserialises() {
        var json   = JsonUtils.toJson(sampleEvent());
        var stream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
        var back   = JsonUtils.fromJson(stream, SecurityEvent.class);
        assertEquals("evt-001",      back.eventId());
        assertEquals("tenant-42",    back.cid());
    }

    // -----------------------------------------------------------------------
    // JsonNode / dynamic access
    // -----------------------------------------------------------------------

    @Test
    void toJsonNode_fromString_givesExpectedFields() {
        var node = JsonUtils.toJsonNode(JsonUtils.toJson(sampleEvent()));
        assertEquals("evt-001",       node.get("event_id").asText());
        assertEquals("tenant-42",     node.get("cid").asText());
        assertEquals("LOGIN_FAILURE", node.get("event_type").asText());
    }

    @Test
    void toJsonNode_fromObject_givesExpectedFields() {
        var node = JsonUtils.toJsonNode(sampleEvent());
        assertEquals("HIGH", node.get("severity").asText());
    }

    // -----------------------------------------------------------------------
    // List helper
    // -----------------------------------------------------------------------

    @Test
    void fromJsonList_roundTrips() {
        var events = List.of(sampleEvent(),
                new SecurityEvent("evt-002", "tenant-42", TS, EVT, "PORT_SCAN",
                        "10.0.0.1", "firewall", "MEDIUM", Map.of()));
        var json   = JsonUtils.toJson(events);
        var back   = JsonUtils.fromJsonList(json, SecurityEvent.class);
        assertEquals(2,         back.size());
        assertEquals("evt-001", back.get(0).eventId());
        assertEquals("evt-002", back.get(1).eventId());
    }

    // -----------------------------------------------------------------------
    // Pretty-print
    // -----------------------------------------------------------------------

    @Test
    void prettyPrint_containsNewlines() {
        var pretty = JsonUtils.prettyPrint(sampleEvent());
        assertTrue(pretty.contains("\n"), "Expected indented output");
        assertTrue(pretty.contains("event_id"));
    }

    // -----------------------------------------------------------------------
    // TypeReference overload (String)
    // -----------------------------------------------------------------------

    @Test
    void fromJson_typeRef_deserialisesList() {
        var events = List.of(sampleEvent());
        var json   = JsonUtils.toJson(events);
        List<SecurityEvent> back = JsonUtils.fromJson(json, new TypeReference<>() {});
        assertEquals(1, back.size());
        assertEquals("tenant-42", back.get(0).cid());
    }
}
