package com.example.replay.api;

import com.example.replay.actors.Messages.CoordinatorCommand;
import com.example.replay.actors.Messages.CoordinatorResponse;
import com.example.replay.model.ReplayJob;
import com.example.replay.model.ReplayRequest;
import com.example.replay.util.JsonUtils;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.AskPattern;
import org.apache.pekko.http.javadsl.marshallers.jackson.Jackson;
import org.apache.pekko.http.javadsl.model.StatusCodes;
import org.apache.pekko.http.javadsl.server.AllDirectives;
import org.apache.pekko.http.javadsl.server.PathMatchers;
import org.apache.pekko.http.javadsl.server.Route;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

/**
 * Pekko HTTP route definitions for the replay REST API.
 *
 * <pre>
 *   POST   /replay              – submit a new replay job
 *   GET    /replay              – list all jobs
 *   GET    /replay/{jobId}      – get job status
 *   DELETE /replay/{jobId}      – cancel a job
 * </pre>
 *
 * Variable path segments are matched with {@code remaining()} — the standard
 * Pekko HTTP Java DSL method for capturing the rest of the matched path.
 */
public final class ReplayRoutes extends AllDirectives {

    private static final Duration ASK_TIMEOUT = Duration.ofSeconds(5);

    private final ActorRef<CoordinatorCommand> coordinator;
    private final ActorSystem<?>               system;

    public ReplayRoutes(ActorRef<CoordinatorCommand> coordinator, ActorSystem<?> system) {
        this.coordinator = coordinator;
        this.system      = system;
    }

    public Route routes() {
        return pathPrefix("replay", () ->
                concat(
                        pathEndOrSingleSlash(() -> concat(
                                post(this::submitJob),
                                get(this::listJobs)
                        )),
                        // PathMatchers.segment() captures a single URL path segment as String
                        path(PathMatchers.segment(), jobId -> concat(
                                get(()    -> getJob(jobId)),
                                delete(() -> cancelJob(jobId))
                        ))
                )
        );
    }

    // -----------------------------------------------------------------------
    // Route handlers
    // -----------------------------------------------------------------------

    private Route submitJob() {
        return entity(Jackson.unmarshaller(JsonUtils.MAPPER, ReplayRequest.class), req -> {
            var jobId = UUID.randomUUID().toString();
            var job   = ReplayJob.create(
                    jobId,
                    req.sourceTable(),
                    req.targetTopic(),
                    req.fromTime(),
                    req.toTime(),
                    req.speedMultiplier());

            CompletionStage<CoordinatorResponse> reply =
                    AskPattern.ask(coordinator,
                            replyTo -> new CoordinatorCommand.SubmitJob(job, replyTo),
                            ASK_TIMEOUT,
                            system.scheduler());

            return onSuccess(reply, response -> switch (response) {
                case CoordinatorResponse.JobAccepted a ->
                        complete(StatusCodes.CREATED, a.job(), Jackson.marshaller(JsonUtils.MAPPER));
                case CoordinatorResponse.Rejected r ->
                        complete(StatusCodes.BAD_REQUEST, r.reason());
                default -> complete(StatusCodes.INTERNAL_SERVER_ERROR, "Unexpected response");
            });
        });
    }

    private Route listJobs() {
        CompletionStage<CoordinatorResponse> reply =
                AskPattern.ask(coordinator,
                        CoordinatorCommand.ListJobs::new,
                        ASK_TIMEOUT,
                        system.scheduler());

        return onSuccess(reply, response -> switch (response) {
            case CoordinatorResponse.JobList list ->
                    complete(StatusCodes.OK, list.jobs(), Jackson.marshaller(JsonUtils.MAPPER));
            default -> complete(StatusCodes.INTERNAL_SERVER_ERROR, "Unexpected response");
        });
    }

    private Route getJob(String jobId) {
        CompletionStage<CoordinatorResponse> reply =
                AskPattern.ask(coordinator,
                        replyTo -> new CoordinatorCommand.GetJob(jobId, replyTo),
                        ASK_TIMEOUT,
                        system.scheduler());

        return onSuccess(reply, response -> switch (response) {
            case CoordinatorResponse.JobSnapshot snap ->
                    complete(StatusCodes.OK, snap.job(), Jackson.marshaller(JsonUtils.MAPPER));
            case CoordinatorResponse.JobNotFound notFound ->
                    complete(StatusCodes.NOT_FOUND, "Job not found: " + jobId);
            default -> complete(StatusCodes.INTERNAL_SERVER_ERROR, "Unexpected response");
        });
    }

    private Route cancelJob(String jobId) {
        CompletionStage<CoordinatorResponse> reply =
                AskPattern.ask(coordinator,
                        replyTo -> new CoordinatorCommand.CancelJob(jobId, replyTo),
                        ASK_TIMEOUT,
                        system.scheduler());

        return onSuccess(reply, response -> switch (response) {
            case CoordinatorResponse.JobSnapshot snap ->
                    complete(StatusCodes.OK, snap.job(), Jackson.marshaller(JsonUtils.MAPPER));
            case CoordinatorResponse.JobNotFound notFound ->
                    complete(StatusCodes.NOT_FOUND, "Job not found: " + jobId);
            default -> complete(StatusCodes.INTERNAL_SERVER_ERROR, "Unexpected response");
        });
    }
}
