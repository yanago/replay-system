package com.example.replay.api;

import com.example.replay.actors.JobManager;
import com.example.replay.actors.Messages;
import com.example.replay.actors.StubWorkPlanner;
import com.example.replay.datalake.StubDataLakeReader;
import com.example.replay.downstream.StubDownstreamClient;
import com.example.replay.kafka.StubEventPublisher;
import com.example.replay.metrics.MetricsRegistry;
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

/**
 * Tests for {@code GET .../status} and {@code GET .../metrics} endpoints.
 */
class MetricsEndpointTest {

    static ActorSystem<Messages.CoordinatorCommand> system;
    static MinimalHttpServer                        server;
    static HttpClient                               client;
    static int                                      port;
    static ObjectMapper                             mapper;

    @BeforeAll
    static void setUp() throws Exception {
        var repo     = new InMemoryJobRepository();
        var registry = new MetricsRegistry();
        // Use slow reader so the job stays RUNNING while we query status/metrics.
        system = ActorSystem.create(
                JobManager.create(repo, new StubDataLakeReader(100, 5, 30L), new StubWorkPlanner(1), 1,
                        new StubEventPublisher(), new StubDownstreamClient(), registry),
                "metrics-test-replay");

        var handler = new JobsHandler(system, repo, registry);

        server = new MinimalHttpServer(0)
                .post("/api/v1/replay/jobs",               handler::create)
                .get("/api/v1/replay/jobs/{id}",           handler::getById)
                .post("/api/v1/replay/jobs/{id}/start",    handler::start)
                .get("/api/v1/replay/jobs/{id}/status",    handler::status)
                .get("/api/v1/replay/jobs/{id}/metrics",   handler::metrics);
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
    // GET /status — unknown job
    // -----------------------------------------------------------------------

    @Test
    void status_unknownJob_returns404() throws Exception {
        var resp = get("/api/v1/replay/jobs/no-such/status");
        assertEquals(404, resp.statusCode());
    }

    // -----------------------------------------------------------------------
    // GET /metrics — unknown job
    // -----------------------------------------------------------------------

    @Test
    void metrics_unknownJob_returns404() throws Exception {
        var resp = get("/api/v1/replay/jobs/no-such/metrics");
        assertEquals(404, resp.statusCode());
    }

    // -----------------------------------------------------------------------
    // GET /metrics — PENDING job returns empty snapshot
    // -----------------------------------------------------------------------

    @Test
    void metrics_pendingJob_returnsEmptySnapshot() throws Exception {
        var jobId = createJob();

        var resp = get("/api/v1/replay/jobs/" + jobId + "/metrics");
        assertEquals(200, resp.statusCode(), resp.body());

        var body = json(resp);
        assertEquals(jobId, body.get("job_id").asText());
        assertEquals(0.0,   body.get("events_per_second").asDouble(),   1e-9);
        assertEquals(0L,    body.get("total_events_published").asLong());
        assertEquals(0L,    body.get("total_batches_processed").asLong());
        assertEquals(0L,    body.get("total_read_errors").asLong());
        assertEquals(0L,    body.get("total_publish_errors").asLong());
        assertEquals(60,    body.get("window_seconds").asInt());
    }

    // -----------------------------------------------------------------------
    // GET /status — PENDING job (no registry entry yet)
    // -----------------------------------------------------------------------

    @Test
    void status_pendingJob_returns200WithPendingStatus() throws Exception {
        var jobId = createJob();

        var resp = get("/api/v1/replay/jobs/" + jobId + "/status");
        assertEquals(200, resp.statusCode(), resp.body());

        var body = json(resp);
        assertEquals(jobId,     body.get("job_id").asText());
        assertEquals("PENDING", body.get("status").asText());
        assertEquals(0,         body.get("packets_total").asInt());
        assertEquals(0,         body.get("packets_completed").asInt());
        assertEquals(0.0,       body.get("progress_pct").asDouble(), 1e-9);
        assertFalse(body.has("eta_seconds"), "eta_seconds should be absent for PENDING");
    }

    // -----------------------------------------------------------------------
    // GET /status — RUNNING job has the expected fields
    // -----------------------------------------------------------------------

    @Test
    void status_runningJob_returns200WithExpectedFields() throws Exception {
        var jobId = createAndStartJob();

        var resp = get("/api/v1/replay/jobs/" + jobId + "/status");
        assertEquals(200, resp.statusCode(), resp.body());

        var body = json(resp);
        assertEquals(jobId,    body.get("job_id").asText());
        assertEquals("RUNNING", body.get("status").asText());
        assertTrue(body.has("packets_total"),     "missing packets_total");
        assertTrue(body.has("packets_completed"), "missing packets_completed");
        assertTrue(body.has("progress_pct"),      "missing progress_pct");
        assertTrue(body.has("elapsed_seconds"),   "missing elapsed_seconds");
        assertTrue(body.has("events_published"),  "missing events_published");
    }

    // -----------------------------------------------------------------------
    // GET /metrics — RUNNING job has the expected fields
    // -----------------------------------------------------------------------

    @Test
    void metrics_runningJob_returns200WithExpectedFields() throws Exception {
        var jobId = createAndStartJob();

        // Give the job a moment to process at least one batch so metrics are non-zero.
        Thread.sleep(100);

        var resp = get("/api/v1/replay/jobs/" + jobId + "/metrics");
        assertEquals(200, resp.statusCode(), resp.body());

        var body = json(resp);
        assertEquals(jobId, body.get("job_id").asText());
        assertTrue(body.has("events_per_second"),       "missing events_per_second");
        assertTrue(body.has("avg_batch_latency_ms"),    "missing avg_batch_latency_ms");
        assertTrue(body.has("p99_batch_latency_ms"),    "missing p99_batch_latency_ms");
        assertTrue(body.has("total_events_published"),  "missing total_events_published");
        assertTrue(body.has("total_batches_processed"), "missing total_batches_processed");
        assertTrue(body.has("total_read_errors"),       "missing total_read_errors");
        assertTrue(body.has("total_publish_errors"),    "missing total_publish_errors");
        assertTrue(body.has("window_seconds"),          "missing window_seconds");
        assertTrue(body.has("sampled_at"),              "missing sampled_at");
        assertEquals(60, body.get("window_seconds").asInt());
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private String createJob() throws Exception {
        var body = """
                {
                  "source_table": "db.events",
                  "target_topic": "metrics-test-%d",
                  "from_time":    "2024-01-01T00:00:00Z",
                  "to_time":      "2024-01-02T00:00:00Z"
                }""".formatted(System.nanoTime());
        var resp = client.send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/v1/replay/jobs"))
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .header("Content-Type", "application/json")
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(201, resp.statusCode(), resp.body());
        return mapper.readTree(resp.body()).get("job_id").asText();
    }

    private String createAndStartJob() throws Exception {
        var jobId = createJob();
        var resp = client.send(
                HttpRequest.newBuilder(URI.create(
                        "http://localhost:" + port + "/api/v1/replay/jobs/" + jobId + "/start"))
                        .POST(HttpRequest.BodyPublishers.noBody())
                        .header("Content-Type", "application/json")
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode(), resp.body());
        return jobId;
    }

    private HttpResponse<String> get(String path) throws Exception {
        return client.send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private JsonNode json(HttpResponse<String> resp) throws Exception {
        return mapper.readTree(resp.body());
    }
}
