package com.example.replay;

import com.example.replay.actors.Messages;
import com.example.replay.actors.ReplayCoordinator;
import com.example.replay.api.JobsHandler;
import com.example.replay.rest.HttpResponse;
import com.example.replay.rest.MinimalHttpServer;
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

        // --- Pekko actor system ------------------------------------------------
        ActorSystem<Messages.CoordinatorCommand> system =
                ActorSystem.create(ReplayCoordinator.create(), "replay-system");

        // --- Route handlers ----------------------------------------------------
        var jobsHandler = new JobsHandler(system);

        // --- HTTP server -------------------------------------------------------
        var http = new MinimalHttpServer(port)
                .get("/health", req -> HttpResponse.ok(
                        "{\"status\":\"OK\",\"timestamp\":\"%s\"}".formatted(Instant.now())))
                .post("/api/v1/replay/jobs", jobsHandler::create);

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
