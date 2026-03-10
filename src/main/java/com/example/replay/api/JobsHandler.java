package com.example.replay.api;

import com.example.replay.actors.Messages;
import com.example.replay.model.ReplayJob;
import com.example.replay.rest.HttpRequest;
import com.example.replay.rest.HttpResponse;
import com.example.replay.storage.ReplayJobRepository;
import com.example.replay.util.JsonUtils;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.AskPattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Handler for the replay-jobs REST resource.
 *
 * <pre>
 *   POST    /api/v1/replay/jobs             → {@link #create}   (creates PENDING)
 *   GET     /api/v1/replay/jobs             → {@link #list}
 *   GET     /api/v1/replay/jobs/{id}        → {@link #getById}
 *   POST    /api/v1/replay/jobs/{id}/start  → {@link #start}    (PENDING → RUNNING)
 *   POST    /api/v1/replay/jobs/{id}/pause  → {@link #pause}    (RUNNING → PAUSED)
 *   POST    /api/v1/replay/jobs/{id}/resume → {@link #resume}   (PAUSED  → RUNNING)
 *   POST    /api/v1/replay/jobs/{id}/cancel → {@link #cancel}   (any     → CANCELLED)
 *   DELETE  /api/v1/replay/jobs/{id}        → {@link #cancel}   (alias)
 * </pre>
 *
 * Handlers are plain {@code Function<HttpRequest, HttpResponse>} so they plug
 * directly into {@link com.example.replay.rest.MinimalHttpServer}.
 * Blocking on the actor ask-future is intentional — callers run on Java 21
 * virtual threads where blocking is cheap.
 */
public final class JobsHandler {

    private static final Logger   log     = LoggerFactory.getLogger(JobsHandler.class);
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private final ActorSystem<Messages.CoordinatorCommand> system;
    private final ReplayJobRepository                      repo;

    public JobsHandler(ActorSystem<Messages.CoordinatorCommand> system,
                       ReplayJobRepository repo) {
        this.system = system;
        this.repo   = repo;
    }

    // -----------------------------------------------------------------------
    // POST /api/v1/replay/jobs
    // -----------------------------------------------------------------------

    /**
     * Creates a new replay job.
     *
     * <p>Request body (JSON):
     * <pre>
     * {
     *   "source_table":     "db.security_events",   // required
     *   "target_topic":     "replay-output",         // required
     *   "from_time":        "2024-01-01T00:00:00Z",  // required
     *   "to_time":          "2024-01-02T00:00:00Z",  // required
     *   "speed_multiplier": 2.0                      // optional, default 1.0
     * }
     * </pre>
     *
     * <p>Responses:
     * <ul>
     *   <li>{@code 201 Created} — {@link ReplayJob} as JSON</li>
     *   <li>{@code 400 Bad Request} — malformed JSON</li>
     *   <li>{@code 422 Unprocessable Entity} — validation errors array</li>
     *   <li>{@code 500 Internal Server Error} — actor/runtime failure</li>
     * </ul>
     */
    public HttpResponse create(HttpRequest req) {

        // 1. Parse -------------------------------------------------------
        if (req.body() == null || req.body().isBlank()) {
            return unprocessable(List.of(
                    new ValidationError("request_body", "must not be empty")));
        }

        JobRequest jobReq;
        try {
            jobReq = JsonUtils.fromJson(req.body(), JobRequest.class);
        } catch (JsonUtils.JsonException e) {
            log.debug("Malformed request body: {}", e.getMessage());
            return HttpResponse.badRequest("invalid JSON: " + e.getCause().getMessage());
        }

        // 2. Validate ----------------------------------------------------
        var errors = JobValidator.validate(jobReq);
        if (!errors.isEmpty()) return unprocessable(errors);

        // 3. Build domain object ----------------------------------------
        var job = ReplayJob.create(
                UUID.randomUUID().toString(),
                jobReq.sourceTable(),
                jobReq.targetTopic(),
                jobReq.fromTime(),
                jobReq.toTime(),
                jobReq.effectiveSpeed());

        // 4. Submit to coordinator (blocks VT) --------------------------
        try {
            // Explicit lambda parameter type required — AskPattern can't infer
            // CoordinatorResponse when the result is captured with 'var'.
            Messages.CoordinatorResponse response = AskPattern
                    .<Messages.CoordinatorCommand, Messages.CoordinatorResponse>ask(
                            system,
                            replyTo -> new Messages.CoordinatorCommand.SubmitJob(job, replyTo),
                            TIMEOUT,
                            system.scheduler())
                    .toCompletableFuture()
                    .get(TIMEOUT.toSeconds() + 1, TimeUnit.SECONDS);

            return switch (response) {
                case Messages.CoordinatorResponse.JobAccepted a ->
                        HttpResponse.of(201, "Created", JsonUtils.toJson(a.job()));
                case Messages.CoordinatorResponse.Rejected r ->
                        unprocessable(List.of(new ValidationError("job", r.reason())));
                default ->
                        HttpResponse.internalError("unexpected coordinator response");
            };

        } catch (Exception e) {
            log.error("Failed to submit job to coordinator", e);
            return HttpResponse.internalError("failed to submit job: " + e.getMessage());
        }
    }

    // -----------------------------------------------------------------------
    // GET /api/v1/replay/jobs
    // -----------------------------------------------------------------------

    /**
     * Returns all replay jobs, ordered newest-first.
     *
     * <p>Response: {@code 200 OK} with a JSON array of {@link ReplayJob}.
     */
    public HttpResponse list(HttpRequest req) {
        return HttpResponse.ok(JsonUtils.toJson(repo.findAll()));
    }

    // -----------------------------------------------------------------------
    // GET /api/v1/replay/jobs/{id}
    // -----------------------------------------------------------------------

    /**
     * Returns a single replay job by ID.
     *
     * <p>Responses:
     * <ul>
     *   <li>{@code 200 OK} — {@link ReplayJob} as JSON</li>
     *   <li>{@code 404 Not Found} — unknown job ID</li>
     * </ul>
     */
    public HttpResponse getById(HttpRequest req) {
        var id = req.pathParam("id");
        return repo.findById(id)
                .map(job -> HttpResponse.ok(JsonUtils.toJson(job)))
                .orElse(HttpResponse.notFound("/api/v1/replay/jobs/" + id));
    }

    // -----------------------------------------------------------------------
    // POST /api/v1/replay/jobs/{id}/start
    // -----------------------------------------------------------------------

    /**
     * Starts a pending replay job (PENDING → RUNNING).
     *
     * <p>Responses:
     * <ul>
     *   <li>{@code 200 OK} — {@link ReplayJob} in RUNNING state</li>
     *   <li>{@code 404 Not Found} — unknown job ID</li>
     *   <li>{@code 422 Unprocessable Entity} — job not in PENDING state</li>
     * </ul>
     */
    public HttpResponse start(HttpRequest req) {
        return lifecycle(req, replyTo ->
                new Messages.CoordinatorCommand.StartJob(req.pathParam("id"), replyTo));
    }

    // -----------------------------------------------------------------------
    // POST /api/v1/replay/jobs/{id}/pause
    // -----------------------------------------------------------------------

    /**
     * Pauses a running replay job.
     *
     * <p>Responses:
     * <ul>
     *   <li>{@code 200 OK} — {@link ReplayJob} in PAUSED state</li>
     *   <li>{@code 404 Not Found} — unknown job ID</li>
     *   <li>{@code 422 Unprocessable Entity} — job not in RUNNING state</li>
     * </ul>
     */
    public HttpResponse pause(HttpRequest req) {
        return lifecycle(req, replyTo ->
                new Messages.CoordinatorCommand.PauseJob(req.pathParam("id"), replyTo));
    }

    // -----------------------------------------------------------------------
    // POST /api/v1/replay/jobs/{id}/resume
    // -----------------------------------------------------------------------

    /**
     * Resumes a paused replay job.
     *
     * <p>Responses:
     * <ul>
     *   <li>{@code 200 OK} — {@link ReplayJob} in RUNNING state</li>
     *   <li>{@code 404 Not Found} — unknown job ID</li>
     *   <li>{@code 422 Unprocessable Entity} — job not in PAUSED state</li>
     * </ul>
     */
    public HttpResponse resume(HttpRequest req) {
        return lifecycle(req, replyTo ->
                new Messages.CoordinatorCommand.ResumeJob(req.pathParam("id"), replyTo));
    }

    // -----------------------------------------------------------------------
    // POST /api/v1/replay/jobs/{id}/cancel  and  DELETE /api/v1/replay/jobs/{id}
    // -----------------------------------------------------------------------

    /**
     * Cancels a replay job (any non-terminal status).
     *
     * <p>Responses:
     * <ul>
     *   <li>{@code 200 OK} — {@link ReplayJob} in CANCELLED state</li>
     *   <li>{@code 404 Not Found} — unknown job ID</li>
     * </ul>
     */
    public HttpResponse cancel(HttpRequest req) {
        return lifecycle(req, replyTo ->
                new Messages.CoordinatorCommand.CancelJob(req.pathParam("id"), replyTo));
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /**
     * Generic lifecycle-command dispatcher: builds the command with the
     * provided factory, asks the coordinator, maps the response to HTTP.
     */
    private HttpResponse lifecycle(
            HttpRequest req,
            java.util.function.Function<
                    org.apache.pekko.actor.typed.ActorRef<Messages.CoordinatorResponse>,
                    Messages.CoordinatorCommand> cmdFactory) {
        try {
            Messages.CoordinatorResponse response = AskPattern
                    .<Messages.CoordinatorCommand, Messages.CoordinatorResponse>ask(
                            system,
                            cmdFactory::apply,
                            TIMEOUT,
                            system.scheduler())
                    .toCompletableFuture()
                    .get(TIMEOUT.toSeconds() + 1, TimeUnit.SECONDS);

            return switch (response) {
                case Messages.CoordinatorResponse.JobAccepted  a -> HttpResponse.ok(JsonUtils.toJson(a.job()));
                case Messages.CoordinatorResponse.JobStarted   s -> HttpResponse.ok(JsonUtils.toJson(s.job()));
                case Messages.CoordinatorResponse.JobSnapshot  s -> HttpResponse.ok(JsonUtils.toJson(s.job()));
                case Messages.CoordinatorResponse.JobPaused    p -> HttpResponse.ok(JsonUtils.toJson(p.job()));
                case Messages.CoordinatorResponse.JobResumed   r -> HttpResponse.ok(JsonUtils.toJson(r.job()));
                case Messages.CoordinatorResponse.JobNotFound  n ->
                        HttpResponse.notFound("/api/v1/replay/jobs/" + n.jobId());
                case Messages.CoordinatorResponse.Rejected     r ->
                        unprocessable(List.of(new ValidationError("job", r.reason())));
                default -> HttpResponse.internalError("unexpected coordinator response");
            };
        } catch (Exception e) {
            log.error("Lifecycle command failed", e);
            return HttpResponse.internalError(e.getMessage());
        }
    }

    private static HttpResponse unprocessable(List<ValidationError> errors) {
        var body = "{\"errors\":" + JsonUtils.toJson(errors) + "}";
        return HttpResponse.of(422, "Unprocessable Entity", body);
    }
}
