package com.example.replay;

import com.example.replay.actors.JobManager;
import com.example.replay.actors.Messages;
import com.example.replay.actors.WorkPlanner;
import com.example.replay.api.JobsHandler;
import com.example.replay.datalake.IcebergDataLakeReader;
import com.example.replay.downstream.DownstreamClient;
import com.example.replay.downstream.HttpDownstreamClient;
import com.example.replay.downstream.SimulatedDownstreamClient;
import com.example.replay.kafka.EventPublisher;
import com.example.replay.kafka.KafkaEventPublisher;
import com.example.replay.kafka.NoOpEventPublisher;
import com.example.replay.metrics.MetricsRegistry;
import org.apache.hadoop.conf.Configuration;
import com.example.replay.rest.HttpResponse;
import com.example.replay.rest.MinimalHttpServer;
import com.example.replay.storage.DatabaseConfig;
import com.example.replay.storage.DataSourceFactory;
import com.example.replay.storage.InMemoryJobRepository;
import com.example.replay.storage.PostgresReplayJobRepository;
import com.example.replay.storage.ReplayJobRepository;
import org.apache.pekko.actor.typed.ActorSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/**
 * Application entry point.
 *
 * <p>Bootstrap order:
 * <ol>
 *   <li>Read config from env / system properties.</li>
 *   <li>Start the {@link MinimalHttpServer} with all API routes registered.</li>
 *   <li>Spin up the Pekko {@link ActorSystem} with {@link ReplayCoordinator} as the guardian.</li>
 * </ol>
 *
 * <p>All wiring (DataSource, Iceberg catalog, Kafka producer) is done here so
 * actors and services stay free of static state.
 */
public final class ReplayApplication {

    private static final Logger log = LoggerFactory.getLogger(ReplayApplication.class);

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(System.getenv().getOrDefault("HTTP_PORT", "8080"));

        log.info("Starting Replay System …");

        // --- Storage -----------------------------------------------------------
        // Use PostgreSQL when POSTGRES_URL is set; fall back to in-memory otherwise.
        ReplayJobRepository repo;
        if (DatabaseConfig.isConfigured()) {
            var ds = DataSourceFactory.create(DatabaseConfig.fromEnv());
            repo = new PostgresReplayJobRepository(ds);
            log.info("Storage backend: PostgreSQL");
        } else {
            repo = new InMemoryJobRepository();
            log.warn("Storage backend: in-memory (data will not survive restart — set POSTGRES_URL for persistence)");
        }

        // --- Data lake reader & work planner -----------------------------------
        var dataLakeReader = new IcebergDataLakeReader();

        // Number of parallel workers per job: cap at 4 to avoid overwhelming
        // the Iceberg reader on a single-node demo deployment.
        int numWorkers = Math.min(4, Runtime.getRuntime().availableProcessors());
        var hadoopConf  = new Configuration();
        var planner     = (com.example.replay.actors.WorkPlannerFn)
                (loc, from, to) -> WorkPlanner.plan(loc, from, to, hadoopConf, numWorkers);

        log.info("Work distribution: {} parallel workers per job", numWorkers);

        // --- Kafka producer ----------------------------------------------------
        // Uses cid (customer ID) as the partition key so all events for the same
        // customer land in one partition.  Falls back to a no-op publisher when
        // KAFKA_BOOTSTRAP_SERVERS is not set (local development / tests).
        var kafkaBootstrap = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        EventPublisher publisher;
        if (kafkaBootstrap != null && !kafkaBootstrap.isBlank()) {
            publisher = new KafkaEventPublisher(kafkaBootstrap, new java.util.Properties());
            log.info("Kafka publisher enabled — brokers: {}", kafkaBootstrap);
        } else {
            publisher = new NoOpEventPublisher();
            log.warn("Kafka publisher disabled — set KAFKA_BOOTSTRAP_SERVERS for real publishing");
        }

        // --- Downstream REST client --------------------------------------------
        // POSTs each batch as a JSON array to DOWNSTREAM_URL when set; otherwise
        // uses a simulated client that logs and discards events.
        var downstreamUrl = System.getenv("DOWNSTREAM_URL");
        DownstreamClient downstreamClient;
        if (downstreamUrl != null && !downstreamUrl.isBlank()) {
            downstreamClient = new HttpDownstreamClient(downstreamUrl);
            log.info("Downstream HTTP client enabled — endpoint: {}", downstreamUrl);
        } else {
            downstreamClient = new SimulatedDownstreamClient();
            log.warn("Downstream HTTP client disabled — set DOWNSTREAM_URL for real forwarding");
        }

        // --- Metrics registry --------------------------------------------------
        var registry = new MetricsRegistry();

        // --- Pekko actor system ------------------------------------------------
        ActorSystem<Messages.CoordinatorCommand> system =
                ActorSystem.create(
                        JobManager.create(repo, dataLakeReader, planner, numWorkers, publisher, downstreamClient, registry),
                        "replay-system");

        // --- Route handlers ----------------------------------------------------
        var jobsHandler = new JobsHandler(system, repo, registry);

        // --- HTTP server -------------------------------------------------------
        var http = new MinimalHttpServer(port)
                .get("/health", req -> HttpResponse.ok(
                        "{\"status\":\"OK\",\"timestamp\":\"%s\"}".formatted(Instant.now())))
                .post("/api/v1/replay/jobs",               jobsHandler::create)
                .get("/api/v1/replay/jobs",                jobsHandler::list)
                .get("/api/v1/replay/jobs/{id}",           jobsHandler::getById)
                .post("/api/v1/replay/jobs/{id}/start",    jobsHandler::start)
                .post("/api/v1/replay/jobs/{id}/pause",    jobsHandler::pause)
                .post("/api/v1/replay/jobs/{id}/resume",   jobsHandler::resume)
                .post("/api/v1/replay/jobs/{id}/cancel",   jobsHandler::cancel)
                .delete("/api/v1/replay/jobs/{id}",        jobsHandler::cancel)
                .get("/api/v1/replay/jobs/{id}/status",    jobsHandler::status)
                .get("/api/v1/replay/jobs/{id}/metrics",   jobsHandler::metrics);

        http.start();

        // --- Shutdown hook -----------------------------------------------------
        final var pub        = publisher;
        final var downstream = downstreamClient;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received");
            http.stop();
            system.terminate();
            pub.close();
            downstream.close();
        }, "shutdown-hook"));

        log.info("Replay System online — http://0.0.0.0:{}/", port);
    }
}
