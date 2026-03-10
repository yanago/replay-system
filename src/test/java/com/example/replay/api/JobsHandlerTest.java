package com.example.replay.api;

import com.example.replay.actors.JobManager;
import com.example.replay.actors.Messages;
import com.example.replay.rest.MinimalHttpServer;
import com.example.replay.storage.InMemoryJobRepository;
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
        var repo = new InMemoryJobRepository();
        system = ActorSystem.create(JobManager.create(repo), "test-replay");
        var handler = new JobsHandler(system, repo);

        server = new MinimalHttpServer(0)           // OS picks a free port
                .post("/api/v1/replay/jobs",               handler::create)
                .get("/api/v1/replay/jobs",                handler::list)
                .get("/api/v1/replay/jobs/{id}",           handler::getById)
                .post("/api/v1/replay/jobs/{id}/start",    handler::start)
                .post("/api/v1/replay/jobs/{id}/pause",    handler::pause)
                .post("/api/v1/replay/jobs/{id}/resume",   handler::resume)
                .post("/api/v1/replay/jobs/{id}/cancel",   handler::cancel)
                .delete("/api/v1/replay/jobs/{id}",        handler::cancel);
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
        assertEquals("PENDING", body.get("status").asText());
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
        assertEquals("PENDING", json(resp).get("status").asText());
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
    // GET /api/v1/replay/jobs
    // -----------------------------------------------------------------------

    @Test
    void list_returnsJsonArray() throws Exception {
        var resp = get("/api/v1/replay/jobs");
        assertEquals(200, resp.statusCode());
        var body = mapper.readTree(resp.body());
        assertTrue(body.isArray(), "expected JSON array, got: " + resp.body());
    }

    @Test
    void list_afterCreate_containsJob() throws Exception {
        post("""
                {
                  "source_table": "db.events",
                  "target_topic": "list-test-topic",
                  "from_time":    "2024-05-01T00:00:00Z",
                  "to_time":      "2024-05-02T00:00:00Z"
                }""");

        var resp = get("/api/v1/replay/jobs");
        assertEquals(200, resp.statusCode());
        var array = mapper.readTree(resp.body());
        assertTrue(array.isArray());
        // At least one job with our topic exists
        boolean found = false;
        for (var node : array) {
            if ("list-test-topic".equals(node.path("target_topic").asText())) {
                found = true;
                break;
            }
        }
        assertTrue(found, "Created job not found in list response");
    }

    // -----------------------------------------------------------------------
    // GET /api/v1/replay/jobs/{id}
    // -----------------------------------------------------------------------

    @Test
    void getById_returnsJob() throws Exception {
        var createResp = post("""
                {
                  "source_table": "db.events",
                  "target_topic": "get-by-id-topic",
                  "from_time":    "2024-06-01T00:00:00Z",
                  "to_time":      "2024-06-02T00:00:00Z"
                }""");
        assertEquals(201, createResp.statusCode());
        var jobId = json(createResp).get("job_id").asText();

        var getResp = get("/api/v1/replay/jobs/" + jobId);
        assertEquals(200, getResp.statusCode());
        var body = json(getResp);
        assertEquals(jobId, body.get("job_id").asText());
        assertEquals("get-by-id-topic", body.get("target_topic").asText());
    }

    @Test
    void getById_unknownId_returns404() throws Exception {
        var resp = get("/api/v1/replay/jobs/no-such-id");
        assertEquals(404, resp.statusCode());
    }

    // -----------------------------------------------------------------------
    // POST /api/v1/replay/jobs/{id}/start
    // -----------------------------------------------------------------------

    @Test
    void start_pendingJob_returns200WithRunningStatus() throws Exception {
        var jobId = createJobId();   // PENDING

        var resp = post("/api/v1/replay/jobs/" + jobId + "/start", "");
        assertEquals(200, resp.statusCode(), resp.body());
        assertEquals("RUNNING", json(resp).get("status").asText());
    }

    @Test
    void start_unknownJob_returns404() throws Exception {
        var resp = post("/api/v1/replay/jobs/ghost/start", "");
        assertEquals(404, resp.statusCode());
    }

    @Test
    void start_alreadyRunningJob_returns422() throws Exception {
        var jobId = createAndGetId();   // PENDING → RUNNING

        var resp = post("/api/v1/replay/jobs/" + jobId + "/start", "");
        assertEquals(422, resp.statusCode());
    }

    // -----------------------------------------------------------------------
    // POST /api/v1/replay/jobs/{id}/pause
    // -----------------------------------------------------------------------

    @Test
    void pause_runningJob_returns200WithPausedStatus() throws Exception {
        var jobId = createAndGetId();

        var resp = post("/api/v1/replay/jobs/" + jobId + "/pause", "");
        assertEquals(200, resp.statusCode(), resp.body());
        assertEquals("PAUSED", json(resp).get("status").asText());
    }

    @Test
    void pause_unknownJob_returns404() throws Exception {
        var resp = post("/api/v1/replay/jobs/ghost/pause", "");
        assertEquals(404, resp.statusCode());
    }

    @Test
    void pause_alreadyPaused_returns422() throws Exception {
        var jobId = createAndGetId();
        post("/api/v1/replay/jobs/" + jobId + "/pause", "");  // first pause

        var resp = post("/api/v1/replay/jobs/" + jobId + "/pause", "");
        assertEquals(422, resp.statusCode());
    }

    // -----------------------------------------------------------------------
    // POST /api/v1/replay/jobs/{id}/resume
    // -----------------------------------------------------------------------

    @Test
    void resume_pausedJob_returns200WithRunningStatus() throws Exception {
        var jobId = createAndGetId();
        post("/api/v1/replay/jobs/" + jobId + "/pause", "");

        var resp = post("/api/v1/replay/jobs/" + jobId + "/resume", "");
        assertEquals(200, resp.statusCode(), resp.body());
        assertEquals("RUNNING", json(resp).get("status").asText());
    }

    @Test
    void resume_runningJob_returns422() throws Exception {
        var jobId = createAndGetId();  // already RUNNING

        var resp = post("/api/v1/replay/jobs/" + jobId + "/resume", "");
        assertEquals(422, resp.statusCode());
    }

    // -----------------------------------------------------------------------
    // DELETE /api/v1/replay/jobs/{id}
    // -----------------------------------------------------------------------

    @Test
    void cancel_runningJob_returns200WithCancelledStatus() throws Exception {
        var jobId = createAndGetId();

        var resp = delete("/api/v1/replay/jobs/" + jobId);
        assertEquals(200, resp.statusCode(), resp.body());
        assertEquals("CANCELLED", json(resp).get("status").asText());
    }

    @Test
    void cancel_pausedJob_returns200() throws Exception {
        var jobId = createAndGetId();
        post("/api/v1/replay/jobs/" + jobId + "/pause", "");

        var resp = delete("/api/v1/replay/jobs/" + jobId);
        assertEquals(200, resp.statusCode());
        assertEquals("CANCELLED", json(resp).get("status").asText());
    }

    @Test
    void cancel_unknownJob_returns404() throws Exception {
        var resp = delete("/api/v1/replay/jobs/nobody");
        assertEquals(404, resp.statusCode());
    }

    @Test
    void cancel_viaPostEndpoint_returns200() throws Exception {
        var jobId = createAndGetId();

        var resp = post("/api/v1/replay/jobs/" + jobId + "/cancel", "");
        assertEquals(200, resp.statusCode(), resp.body());
        assertEquals("CANCELLED", json(resp).get("status").asText());
    }

    @Test
    void cancel_pendingJob_returns200() throws Exception {
        var jobId = createJobId();   // PENDING, never started

        var resp = post("/api/v1/replay/jobs/" + jobId + "/cancel", "");
        assertEquals(200, resp.statusCode(), resp.body());
        assertEquals("CANCELLED", json(resp).get("status").asText());
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /** Creates a PENDING job and returns its job_id. */
    private String createJobId() throws Exception {
        var resp = post("""
                {
                  "source_table": "db.events",
                  "target_topic": "lc-topic-%d",
                  "from_time":    "2024-07-01T00:00:00Z",
                  "to_time":      "2024-07-02T00:00:00Z"
                }""".formatted(System.nanoTime()));
        assertEquals(201, resp.statusCode(), resp.body());
        return json(resp).get("job_id").asText();
    }

    /** Creates a job, starts it (PENDING → RUNNING), and returns its job_id. */
    private String createAndGetId() throws Exception {
        var jobId = createJobId();
        var startResp = post("/api/v1/replay/jobs/" + jobId + "/start", "");
        assertEquals(200, startResp.statusCode(), startResp.body());
        return jobId;
    }

    private HttpResponse<String> delete(String path) throws Exception {
        return client.send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                        .DELETE()
                        .build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> get(String path) throws Exception {
        return client.send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> post(String body) throws Exception {
        return post("/api/v1/replay/jobs", body);
    }

    private HttpResponse<String> post(String path, String body) throws Exception {
        return client.send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
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
