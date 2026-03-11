package com.example.replay.actors;

import com.example.replay.actors.Messages.CoordinatorCommand;
import com.example.replay.datalake.StubDataLakeReader;
import com.example.replay.model.ReplayJob;
import com.example.replay.model.ReplayStatus;
import com.example.replay.storage.InMemoryJobRepository;
import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit;
import org.apache.pekko.actor.testkit.typed.javadsl.FishingOutcomes;
import org.apache.pekko.actor.testkit.typed.javadsl.TestProbe;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link ReplayJobActor} state machine transitions.
 *
 * <p>Strategy: spawn a real {@code ReplayJobActor} backed by a
 * {@link InMemoryJobRepository} and a {@link StubDataLakeReader};
 * observe coordinator messages via a probe; assert repo state matches
 * expected lifecycle.
 *
 * <p>The stub reader returns 5 batches × 10 events synchronously on a
 * virtual thread, so tests complete quickly.
 */
class ReplayJobActorTest {

    static final ActorTestKit testKit = ActorTestKit.create();

    @AfterAll
    static void teardown() { testKit.shutdownTestKit(); }

    private static final Duration LONG_WAIT = Duration.ofSeconds(5);

    private static ReplayJob job(String id) {
        return ReplayJob.create(id, "db.events", "test-topic",
                Instant.parse("2024-01-01T00:00:00Z"),
                Instant.parse("2024-01-02T00:00:00Z"), 1.0);
    }

    // -----------------------------------------------------------------------
    // Start → runs to completion
    // -----------------------------------------------------------------------

    @Test
    void start_runsToCompletion_notifiesCoordinator() {
        var repo        = new InMemoryJobRepository();
        var coordProbe  = testKit.<CoordinatorCommand>createTestProbe();
        var job         = job("complete-1");
        repo.save(job.withStatus(ReplayStatus.RUNNING));

        var actor = testKit.spawn(
                ReplayJobActor.create(job, repo, coordProbe.getRef(), new StubDataLakeReader(5, 10), new StubWorkPlanner(1), 1), "actor-complete-1");
        actor.tell(new Messages.ReplayJobCommand.Start());

        // Wait for WorkerFinished (5 batches × 200ms = ~1s)
        var msg = coordProbe.receiveMessage(LONG_WAIT);
        assertInstanceOf(CoordinatorCommand.WorkerFinished.class, msg);
        var finished = (CoordinatorCommand.WorkerFinished) msg;
        assertEquals("complete-1", finished.jobId());
        assertEquals(50L, finished.eventsPublished());  // 5 batches × 10 events
    }

    // -----------------------------------------------------------------------
    // Pause → PAUSED state
    // -----------------------------------------------------------------------

    @Test
    void pause_whileRunning_sendsWorkerPaused() {
        var repo       = new InMemoryJobRepository();
        var coordProbe = testKit.<CoordinatorCommand>createTestProbe();
        var job        = job("pause-1");
        repo.save(job.withStatus(ReplayStatus.RUNNING));

        var actor = testKit.spawn(
                ReplayJobActor.create(job, repo, coordProbe.getRef(), new StubDataLakeReader(5, 10), new StubWorkPlanner(1), 1), "actor-pause-1");
        actor.tell(new Messages.ReplayJobCommand.Start());
        actor.tell(new Messages.ReplayJobCommand.Pause());

        // Expect WorkerPaused confirmation (may arrive before any WorkerFinished)
        coordProbe.fishForMessage(LONG_WAIT, m ->
                m instanceof CoordinatorCommand.WorkerPaused
                        ? FishingOutcomes.complete()
                        : FishingOutcomes.continueAndIgnore());
    }

    // -----------------------------------------------------------------------
    // Pause → Resume → continues to completion
    // -----------------------------------------------------------------------

    @Test
    void pauseThenResume_completesNormally() {
        var repo       = new InMemoryJobRepository();
        var coordProbe = testKit.<CoordinatorCommand>createTestProbe();
        var job        = job("pause-resume-1");
        repo.save(job.withStatus(ReplayStatus.RUNNING));

        var actor = testKit.spawn(
                ReplayJobActor.create(job, repo, coordProbe.getRef(), new StubDataLakeReader(5, 10), new StubWorkPlanner(1), 1), "actor-pr-1");
        actor.tell(new Messages.ReplayJobCommand.Start());
        actor.tell(new Messages.ReplayJobCommand.Pause());
        actor.tell(new Messages.ReplayJobCommand.Resume());

        // Must eventually complete
        coordProbe.fishForMessage(LONG_WAIT, m ->
                m instanceof CoordinatorCommand.WorkerFinished
                        ? FishingOutcomes.complete()
                        : FishingOutcomes.continueAndIgnore());
    }

    // -----------------------------------------------------------------------
    // Cancel → stopped, no WorkerFinished
    // -----------------------------------------------------------------------

    @Test
    void cancel_beforeStart_stopsActor() {
        var repo       = new InMemoryJobRepository();
        var coordProbe = testKit.<CoordinatorCommand>createTestProbe();
        var job        = job("cancel-before-start");

        var actor = testKit.spawn(
                ReplayJobActor.create(job, repo, coordProbe.getRef(), new StubDataLakeReader(5, 10), new StubWorkPlanner(1), 1), "actor-cancel-bs");
        actor.tell(new Messages.ReplayJobCommand.Cancel());

        // No WorkerFinished or WorkerFailed expected
        coordProbe.expectNoMessage(Duration.ofMillis(300));
    }

    @Test
    void cancel_whileRunning_neverSendsWorkerFailed() {
        var repo       = new InMemoryJobRepository();
        var coordProbe = testKit.<CoordinatorCommand>createTestProbe();
        var job        = job("cancel-running-1");
        repo.save(job.withStatus(ReplayStatus.RUNNING));

        var actor = testKit.spawn(
                ReplayJobActor.create(job, repo, coordProbe.getRef(), new StubDataLakeReader(5, 10), new StubWorkPlanner(1), 1), "actor-cancel-r-1");
        // Start dispatches an async readBatch, but Cancel is enqueued immediately
        // after Start. The actor processes Cancel before the BatchReady result
        // arrives (actor message ordering), stops cleanly, and the coordinator
        // never receives WorkerFinished or WorkerFailed.
        actor.tell(new Messages.ReplayJobCommand.Start());
        actor.tell(new Messages.ReplayJobCommand.Cancel());

        coordProbe.expectNoMessage(Duration.ofMillis(300));
    }

    // -----------------------------------------------------------------------
    // Progress updates stored in repo
    // -----------------------------------------------------------------------

    @Test
    void completedJob_progressPersistedInRepo() throws InterruptedException {
        var repo       = new InMemoryJobRepository();
        var coordProbe = testKit.<CoordinatorCommand>createTestProbe();
        var job        = job("progress-1");
        repo.save(job.withStatus(ReplayStatus.RUNNING));

        var actor = testKit.spawn(
                ReplayJobActor.create(job, repo, coordProbe.getRef(), new StubDataLakeReader(5, 10), new StubWorkPlanner(1), 1), "actor-progress-1");
        actor.tell(new Messages.ReplayJobCommand.Start());

        // Wait for completion
        coordProbe.receiveMessage(LONG_WAIT);

        var stored = repo.findById("progress-1").orElseThrow();
        assertEquals(50L, stored.eventsPublished());
    }
}
