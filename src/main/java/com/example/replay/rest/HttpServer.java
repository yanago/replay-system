package com.example.replay.rest;

import com.example.replay.actors.Messages.CoordinatorCommand;
import com.example.replay.api.ReplayRoutes;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.http.javadsl.Http;
import org.apache.pekko.http.javadsl.ServerBinding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletionStage;

/**
 * Bootstraps the Pekko HTTP server and binds it to the configured host/port.
 */
public final class HttpServer {

    private static final Logger log = LoggerFactory.getLogger(HttpServer.class);

    private final String  host;
    private final int     port;
    private final ActorSystem<?>              system;
    private final ActorRef<CoordinatorCommand> coordinator;

    private CompletionStage<ServerBinding> binding;

    public HttpServer(String host,
                      int port,
                      ActorSystem<?> system,
                      ActorRef<CoordinatorCommand> coordinator) {
        this.host        = host;
        this.port        = port;
        this.system      = system;
        this.coordinator = coordinator;
    }

    /** Starts the HTTP server asynchronously; returns the binding future. */
    public CompletionStage<ServerBinding> start() {
        var routes = new ReplayRoutes(coordinator, system).routes();
        binding = Http.get(system).newServerAt(host, port).bind(routes);
        binding.whenComplete((b, ex) -> {
            if (ex != null) {
                log.error("HTTP server failed to bind on {}:{}", host, port, ex);
            } else {
                log.info("HTTP server online at http://{}:{}/", host, port);
                system.classicSystem().registerOnTermination(b::unbind);
            }
        });
        return binding;
    }
}
