package com.example.replay.api;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class JobValidatorTest {

    private static final Instant FROM = Instant.parse("2024-01-01T00:00:00Z");
    private static final Instant TO   = Instant.parse("2024-01-02T00:00:00Z");

    // -----------------------------------------------------------------------
    // Valid request
    // -----------------------------------------------------------------------

    @Test
    void valid_allFields_noErrors() {
        var req = new JobRequest("db.events", "replay-out", FROM, TO, 2.0);
        assertEquals(List.of(), JobValidator.validate(req));
    }

    @Test
    void valid_speedMultiplierAbsent_noErrors() {
        var req = new JobRequest("db.events", "replay-out", FROM, TO, null);
        assertEquals(List.of(), JobValidator.validate(req));
    }

    @Test
    void valid_topicWithDotsAndDashes_noErrors() {
        var req = new JobRequest("db.events", "replay.out-v1_2", FROM, TO, 1.0);
        assertEquals(List.of(), JobValidator.validate(req));
    }

    // -----------------------------------------------------------------------
    // Null request
    // -----------------------------------------------------------------------

    @Test
    void nullRequest_returnsBodyError() {
        var errors = JobValidator.validate(null);
        assertEquals(1, errors.size());
        assertEquals("request_body", errors.get(0).field());
    }

    // -----------------------------------------------------------------------
    // source_table
    // -----------------------------------------------------------------------

    @Test
    void missingSourceTable_returnsError() {
        assertFieldError(new JobRequest(null, "topic", FROM, TO, 1.0), "source_table");
    }

    @Test
    void blankSourceTable_returnsError() {
        assertFieldError(new JobRequest("   ", "topic", FROM, TO, 1.0), "source_table");
    }

    // -----------------------------------------------------------------------
    // target_topic
    // -----------------------------------------------------------------------

    @Test
    void missingTargetTopic_returnsError() {
        assertFieldError(new JobRequest("db.events", null, FROM, TO, 1.0), "target_topic");
    }

    @Test
    void blankTargetTopic_returnsError() {
        assertFieldError(new JobRequest("db.events", "", FROM, TO, 1.0), "target_topic");
    }

    @Test
    void invalidTopicChars_returnsError() {
        assertFieldError(new JobRequest("db.events", "bad topic!", FROM, TO, 1.0), "target_topic");
    }

    @Test
    void topicTooLong_returnsError() {
        var longTopic = "a".repeat(250);
        assertFieldError(new JobRequest("db.events", longTopic, FROM, TO, 1.0), "target_topic");
    }

    // -----------------------------------------------------------------------
    // from_time / to_time
    // -----------------------------------------------------------------------

    @Test
    void missingFromTime_returnsError() {
        assertFieldError(new JobRequest("db.events", "topic", null, TO, 1.0), "from_time");
    }

    @Test
    void missingToTime_returnsError() {
        assertFieldError(new JobRequest("db.events", "topic", FROM, null, 1.0), "to_time");
    }

    @Test
    void fromTimeEqualsToTime_returnsError() {
        assertFieldError(new JobRequest("db.events", "topic", FROM, FROM, 1.0), "from_time");
    }

    @Test
    void fromTimeAfterToTime_returnsError() {
        assertFieldError(new JobRequest("db.events", "topic", TO, FROM, 1.0), "from_time");
    }

    @Test
    void rangeExceeds366Days_returnsError() {
        var bigTo = FROM.plusSeconds(366L * 24 * 3600 + 1);
        assertFieldError(new JobRequest("db.events", "topic", FROM, bigTo, 1.0), "time_range");
    }

    @Test
    void rangeExactly366Days_isValid() {
        var to = FROM.plusSeconds(366L * 24 * 3600);
        assertEquals(List.of(), JobValidator.validate(new JobRequest("db.events", "topic", FROM, to, 1.0)));
    }

    // -----------------------------------------------------------------------
    // speed_multiplier
    // -----------------------------------------------------------------------

    @Test
    void speedZero_returnsError() {
        assertFieldError(new JobRequest("db.events", "topic", FROM, TO, 0.0), "speed_multiplier");
    }

    @Test
    void speedNegative_returnsError() {
        assertFieldError(new JobRequest("db.events", "topic", FROM, TO, -1.0), "speed_multiplier");
    }

    @Test
    void speedExceedsMax_returnsError() {
        assertFieldError(new JobRequest("db.events", "topic", FROM, TO, 1_001.0), "speed_multiplier");
    }

    @Test
    void speedAtMax_isValid() {
        assertEquals(List.of(),
                JobValidator.validate(new JobRequest("db.events", "topic", FROM, TO, 1_000.0)));
    }

    // -----------------------------------------------------------------------
    // Multiple errors returned at once
    // -----------------------------------------------------------------------

    @Test
    void multipleInvalidFields_allErrorsReturned() {
        var req    = new JobRequest(null, null, null, null, -1.0);
        var errors = JobValidator.validate(req);
        var fields = errors.stream().map(ValidationError::field).toList();
        assertTrue(fields.contains("source_table"),    fields.toString());
        assertTrue(fields.contains("target_topic"),    fields.toString());
        assertTrue(fields.contains("from_time"),       fields.toString());
        assertTrue(fields.contains("to_time"),         fields.toString());
        assertTrue(fields.contains("speed_multiplier"),fields.toString());
    }

    // -----------------------------------------------------------------------
    // Helper
    // -----------------------------------------------------------------------

    private static void assertFieldError(JobRequest req, String expectedField) {
        var errors = JobValidator.validate(req);
        assertFalse(errors.isEmpty(), "Expected validation error for field: " + expectedField);
        var fields = errors.stream().map(ValidationError::field).toList();
        assertTrue(fields.contains(expectedField),
                "Expected error on field '%s', got: %s".formatted(expectedField, fields));
    }
}
