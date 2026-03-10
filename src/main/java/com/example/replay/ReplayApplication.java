package com.example.replay;

import com.example.replay.actors.ReplayCoordinator;
import com.example.replay.rest.HttpServer;
import org.apache.pekko.actor.typed.ActorSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Application entry point.
 *
 * <p>Bootstrap order:
 * <ol>
 *   <li>Read config from env / system properties.</li>
 *   <li>Spin up the Pekko {@link ActorSystem} with {@link ReplayCoordinator} as the guardian.</li>
 *   <li>Start the Pekko HTTP REST server.</li>
 * </ol>
 *
 * <p>All wiring (DataSource, Iceberg catalog, Kafka producer) is done here so
 * actors and services stay free of static state.
 */
public final class ReplayApplication {

    private static final Logger log = LoggerFactory.getLogger(ReplayApplication.class);

    public static void main(String[] args) {
        var host = System.getenv().getOrDefault("HTTP_HOST", "0.0.0.0");
        var port = Integer.parseInt(System.getenv().getOrDefault("HTTP_PORT", "8080"));

        log.info("Starting Replay System …");

        // Guardian actor == the coordinator; the actor system name is used in
        // Pekko remoting and logging — keep it stable.
        var system = ActorSystem.create(ReplayCoordinator.create(), "replay-system");

        var coordinator = system; // ActorSystem<CoordinatorCommand> IS the guardian ref

        var httpServer = new HttpServer(host, port, system, coordinator);
        httpServer.start();

        // JVM shutdown hook — terminate the actor system cleanly
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received — terminating actor system");
            system.terminate();
        }, "shutdown-hook"));

        log.info("Replay System started on {}:{}", host, port);
    }
}
