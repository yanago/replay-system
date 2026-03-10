package com.example.replay.api;

import com.example.replay.actors.Messages;
import com.example.replay.actors.ReplayCoordinator;
import com.example.replay.rest.MinimalHttpServer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.pekko.actor.typed.ActorSystem;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class JobsHandlerTest {

    static ActorSystem<Messages.CoordinatorCommand> system;
    static MinimalHttpServer                        server;
    static HttpClient                               client;
    static int                                      port;
    static ObjectMapper                             mapper;

    @BeforeAll
    static void setUp() throws Exception {
        system = ActorSystem.create(ReplayCoordinator.create(), "test-replay");
        var handler = new JobsHandler(system);

        server = new MinimalHttpServer(0)           // OS picks a free port
                .post("/api/v1/replay/jobs", handler::create);
        server.start();
        port = server.boundPort();

        client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .build();

        mapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    @AfterAll
    static void tearDown() {
        server.stop();
        system.terminate();
    }

    // -----------------------------------------------------------------------
    // Happy path
    // -----------------------------------------------------------------------

    @Test
    void create_validRequest_returns201WithJobId() throws Exception {
        var resp = post("""
                {
                  "source_table":     "db.security_events",
                  "target_topic":     "replay-output",
                  "from_time":        "2024-01-01T00:00:00Z",
                  "to_time":          "2024-01-02T00:00:00Z",
                  "speed_multiplier": 2.0
                }""");

        assertEquals(201, resp.statusCode());
        var body = json(resp);
        assertNotNull(body.get("job_id"),       "missing job_id");
        assertEquals("RUNNING", body.get("status").asText());
        assertEquals("db.security_events", body.get("source_table").asText());
        assertEquals("replay-output",      body.get("target_topic").asText());
        assertEquals(2.0, body.get("speed_multiplier").asDouble(), 1e-9);
        assertEquals(0,   body.get("events_published").asLong());
    }

    @Test
    void create_speedMultiplierOmitted_defaults1() throws Exception {
        var resp = post("""
                {
                  "source_table": "db.events",
                  "target_topic": "out",
                  "from_time":    "2024-02-01T00:00:00Z",
                  "to_time":      "2024-02-02T00:00:00Z"
                }""");

        assertEquals(201, resp.statusCode());
        assertEquals(1.0, json(resp).get("speed_multiplier").asDouble(), 1e-9);
    }

    @Test
    void create_responseContainsSnakeCaseFields() throws Exception {
        var resp = post(validBody());
        assertEquals(201, resp.statusCode());
        var body = resp.body();
        assertTrue(body.contains("\"job_id\""),           body);
        assertTrue(body.contains("\"source_table\""),     body);
        assertTrue(body.contains("\"target_topic\""),     body);
        assertTrue(body.contains("\"speed_multiplier\""), body);
        assertTrue(body.contains("\"events_published\""), body);
        assertTrue(body.contains("\"created_at\""),       body);
    }

    // -----------------------------------------------------------------------
    // Validation errors → 422
    // -----------------------------------------------------------------------

    @Test
    void create_emptyBody_returns422() throws Exception {
        var resp = post("");
        assertEquals(422, resp.statusCode());
        assertTrue(resp.body().contains("\"errors\""), resp.body());
    }

    @Test
    void create_malformedJson_returns400() throws Exception {
        var resp = post("not-json{{{");
        assertEquals(400, resp.statusCode());
    }

    @Test
    void create_missingSourceTable_returns422WithFieldError() throws Exception {
        var resp = post("""
                {
                  "target_topic": "out",
                  "from_time": "2024-01-01T00:00:00Z",
                  "to_time":   "2024-01-02T00:00:00Z"
                }""");

        assertEquals(422, resp.statusCode());
        assertErrorField(resp, "source_table");
    }

    @Test
    void create_missingTargetTopic_returns422WithFieldError() throws Exception {
        var resp = post("""
                {
                  "source_table": "db.events",
                  "from_time": "2024-01-01T00:00:00Z",
                  "to_time":   "2024-01-02T00:00:00Z"
                }""");

        assertEquals(422, resp.statusCode());
        assertErrorField(resp, "target_topic");
    }

    @Test
    void create_missingFromTime_returns422WithFieldError() throws Exception {
        var resp = post("""
                {
                  "source_table": "db.events",
                  "target_topic": "out",
                  "to_time": "2024-01-02T00:00:00Z"
                }""");

        assertEquals(422, resp.statusCode());
        assertErrorField(resp, "from_time");
    }

    @Test
    void create_fromTimeAfterToTime_returns422WithFieldError() throws Exception {
        var resp = post("""
                {
                  "source_table": "db.events",
                  "target_topic": "out",
                  "from_time": "2024-01-02T00:00:00Z",
                  "to_time":   "2024-01-01T00:00:00Z"
                }""");

        assertEquals(422, resp.statusCode());
        assertErrorField(resp, "from_time");
    }

    @Test
    void create_invalidTopicName_returns422WithFieldError() throws Exception {
        var resp = post("""
                {
                  "source_table": "db.events",
                  "target_topic": "bad topic name!",
                  "from_time": "2024-01-01T00:00:00Z",
                  "to_time":   "2024-01-02T00:00:00Z"
                }""");

        assertEquals(422, resp.statusCode());
        assertErrorField(resp, "target_topic");
    }

    @Test
    void create_negativeSpeed_returns422WithFieldError() throws Exception {
        var resp = post("""
                {
                  "source_table":     "db.events",
                  "target_topic":     "out",
                  "from_time":        "2024-01-01T00:00:00Z",
                  "to_time":          "2024-01-02T00:00:00Z",
                  "speed_multiplier": -1.0
                }""");

        assertEquals(422, resp.statusCode());
        assertErrorField(resp, "speed_multiplier");
    }

    @Test
    void create_multipleInvalidFields_allErrorsInResponse() throws Exception {
        var resp = post("{}");   // all fields missing
        assertEquals(422, resp.statusCode());
        var errors = json(resp).get("errors");
        assertNotNull(errors, "expected 'errors' array");
        assertTrue(errors.isArray());
        assertTrue(errors.size() >= 4, "expected ≥4 errors, got " + errors.size());
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private HttpResponse<String> post(String body) throws Exception {
        return client.send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/v1/replay/jobs"))
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .header("Content-Type", "application/json")
                        .build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private JsonNode json(HttpResponse<String> resp) throws Exception {
        return mapper.readTree(resp.body());
    }

    private void assertErrorField(HttpResponse<String> resp, String field) throws Exception {
        var errors = json(resp).get("errors");
        assertNotNull(errors, "response has no 'errors' key: " + resp.body());
        assertTrue(errors.isArray(), "'errors' is not an array");
        var fields = new java.util.ArrayList<String>();
        errors.forEach(e -> fields.add(e.get("field").asText()));
        assertTrue(fields.contains(field),
                "Expected error on field '%s', got: %s".formatted(field, fields));
    }

    private static String validBody() {
        return """
                {
                  "source_table":     "db.events",
                  "target_topic":     "out",
                  "from_time":        "2024-03-01T00:00:00Z",
                  "to_time":          "2024-03-02T00:00:00Z",
                  "speed_multiplier": 1.0
                }""";
    }
}
