package com.example.replay;

import com.example.replay.actors.JobManager;
import com.example.replay.actors.Messages;
import com.example.replay.actors.WorkPlanner;
import com.example.replay.api.JobsHandler;
import com.example.replay.datalake.IcebergDataLakeReader;
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

        // --- Pekko actor system ------------------------------------------------
        ActorSystem<Messages.CoordinatorCommand> system =
                ActorSystem.create(JobManager.create(repo, dataLakeReader, planner, numWorkers), "replay-system");

        // --- Route handlers ----------------------------------------------------
        var jobsHandler = new JobsHandler(system, repo);

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
                .delete("/api/v1/replay/jobs/{id}",        jobsHandler::cancel);

        http.start();

        // --- Shutdown hook -----------------------------------------------------
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received");
            http.stop();
            system.terminate();
        }, "shutdown-hook"));

        log.info("Replay System online — http://0.0.0.0:{}/", port);
    }
}
