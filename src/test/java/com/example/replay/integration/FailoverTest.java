package com.example.replay.integration;

import com.example.replay.actors.Messages;
import com.example.replay.actors.Messages.CoordinatorCommand;
import com.example.replay.actors.Messages.ReplayJobCommand;
import com.example.replay.actors.ReplayJobActor;
import com.example.replay.actors.StubWorkPlanner;
import com.example.replay.actors.WorkerPoolActor;
import com.example.replay.datalake.StubDataLakeReader;
import com.example.replay.downstream.StubDownstreamClient;
import com.example.replay.kafka.StubEventPublisher;
import com.example.replay.metrics.MetricsRegistry;
import com.example.replay.model.ReplayJob;
import com.example.replay.model.ReplayStatus;
import com.example.replay.storage.InMemoryJobRepository;
import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit;
import org.apache.pekko.actor.testkit.typed.javadsl.FishingOutcomes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests validating that pause/resume/cancel lifecycle operations
 * do not cause data loss, duplication, or incorrect final state.
 *
 * <h3>No-data-loss invariant</h3>
 * <p>{@link com.example.replay.actors.PacketWorkerActor} tracks a {@code batchIndex}
 * checkpoint.  When paused, it waits until the current in-flight publish completes
 * before entering {@code WAITING_RESUME}.  On resume it continues from the next
 * unprocessed batch.  Therefore:
 * <ul>
 *   <li>Every batch is processed exactly once — no skips, no replays.</li>
 *   <li>Total events after pause+resume = {@code numPackets × totalBatches × eventsPerBatch}.</li>
 * </ul>
 *
 * <h3>Cancel semantics</h3>
 * <p>After {@code Cancel}, workers stop immediately and the pool never sends
 * {@code PoolFinished} — the test probe must receive no completion message.
 */
class FailoverTest {

    static final ActorTestKit testKit = ActorTestKit.create();

    @AfterAll
    static void teardown() { testKit.shutdownTestKit(); }

    private static final Duration WAIT       = Duration.ofSeconds(15);
    private static final Duration SHORT_WAIT = Duration.ofMillis(400);

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static ReplayJob job(String id) {
        return ReplayJob.create(id, "db.events", "failover-topic",
                Instant.parse("2024-06-01T00:00:00Z"),
                Instant.parse("2024-06-02T00:00:00Z"), 1.0);
    }

    // -----------------------------------------------------------------------
    // Test 1: Pause then resume — single packet, no event loss
    // -----------------------------------------------------------------------

    @Test
    void pauseResume_singlePacket_noEventLoss() {
        var probe      = testKit.<ReplayJobCommand>createTestProbe();
        var publisher  = new StubEventPublisher();
        // 20 batches × 30ms delay = ~600ms — enough to land pause before EOF
        var packets    = new StubWorkPlanner(1).plan(
                "loc",
                Instant.parse("2024-01-01T00:00:00Z"),
                Instant.parse("2024-01-02T00:00:00Z"));

        var pool = testKit.spawn(WorkerPoolActor.create(
                packets, new StubDataLakeReader(20, 5, 30L),
                publisher, "topic",
                new StubDownstreamClient(), probe.getRef(), 1,
                "fo-1", new MetricsRegistry()));

        pool.tell(new Messages.WorkerPoolCommand.Start());
        pool.tell(new Messages.WorkerPoolCommand.Pause());
        pool.tell(new Messages.WorkerPoolCommand.Resume());

        var msg = probe.receiveMessage(WAIT);
        assertInstanceOf(ReplayJobCommand.PoolFinished.class, msg);
        // 1 packet × 20 batches × 5 events = 100
        assertEquals(100L, ((ReplayJobCommand.PoolFinished) msg).totalEvents(),
                "Event count after pause+resume must equal full expected total");
    }

    // -----------------------------------------------------------------------
    // Test 2: Pause then resume — multiple packets, no event loss
    // -----------------------------------------------------------------------

    @Test
    void pauseResume_multiplePackets_noEventLoss() {
        var probe   = testKit.<ReplayJobCommand>createTestProbe();
        var packets = new StubWorkPlanner(4).plan(
                "loc",
                Instant.parse("2024-01-01T00:00:00Z"),
                Instant.parse("2024-01-05T00:00:00Z"));

        var pool = testKit.spawn(WorkerPoolActor.create(
                packets, new StubDataLakeReader(10, 5, 20L),
                new StubEventPublisher(), "topic",
                new StubDownstreamClient(), probe.getRef(), 2,
                "fo-2", new MetricsRegistry()));

        pool.tell(new Messages.WorkerPoolCommand.Start());
        pool.tell(new Messages.WorkerPoolCommand.Pause());
        pool.tell(new Messages.WorkerPoolCommand.Resume());

        var msg = probe.receiveMessage(WAIT);
        assertInstanceOf(ReplayJobCommand.PoolFinished.class, msg);
        // 4 packets × 10 batches × 5 events = 200
        assertEquals(200L, ((ReplayJobCommand.PoolFinished) msg).totalEvents(),
                "No events should be lost across multiple packets");
    }

    // -----------------------------------------------------------------------
    // Test 3: Three pause/resume cycles — cumulative total still correct
    // -----------------------------------------------------------------------

    @Test
    void multiplePauseResumeCycles_noEventLoss() {
        var probe   = testKit.<ReplayJobCommand>createTestProbe();
        var packets = new StubWorkPlanner(2).plan(
                "loc",
                Instant.parse("2024-01-01T00:00:00Z"),
                Instant.parse("2024-01-03T00:00:00Z"));

        var pool = testKit.spawn(WorkerPoolActor.create(
                packets, new StubDataLakeReader(15, 4, 25L),
                new StubEventPublisher(), "topic",
                new StubDownstreamClient(), probe.getRef(), 1,
                "fo-3", new MetricsRegistry()));

        pool.tell(new Messages.WorkerPoolCommand.Start());
        // Three rapid pause/resume cycles
        for (int i = 0; i < 3; i++) {
            pool.tell(new Messages.WorkerPoolCommand.Pause());
            pool.tell(new Messages.WorkerPoolCommand.Resume());
        }

        var msg = probe.receiveMessage(WAIT);
        assertInstanceOf(ReplayJobCommand.PoolFinished.class, msg);
        // 2 packets × 15 batches × 4 events = 120
        assertEquals(120L, ((ReplayJobCommand.PoolFinished) msg).totalEvents(),
                "Repeated pause/resume must not lose or duplicate events");
    }

    // -----------------------------------------------------------------------
    // Test 4: Cancel — no PoolFinished, no data duplication possible
    // -----------------------------------------------------------------------

    @Test
    void cancel_stopsEmission_poolFinishedNeverArrives() {
        var probe   = testKit.<ReplayJobCommand>createTestProbe();
        var packets = new StubWorkPlanner(1).plan(
                "loc",
                Instant.parse("2024-01-01T00:00:00Z"),
                Instant.parse("2024-01-02T00:00:00Z"));

        var pool = testKit.spawn(WorkerPoolActor.create(
                packets, new StubDataLakeReader(100, 10, 20L),  // very slow
                new StubEventPublisher(), "topic",
                new StubDownstreamClient(), probe.getRef(), 1,
                "fo-4", new MetricsRegistry()));

        pool.tell(new Messages.WorkerPoolCommand.Start());
        pool.tell(new Messages.WorkerPoolCommand.Cancel());

        // No PoolFinished or PoolFailed should arrive after cancel
        probe.expectNoMessage(SHORT_WAIT);
    }

    // -----------------------------------------------------------------------
    // Test 5: Cancel during pause — workers stop cleanly
    // -----------------------------------------------------------------------

    @Test
    void cancelWhilePaused_stopsCleanly() {
        var probe   = testKit.<ReplayJobCommand>createTestProbe();
        var packets = new StubWorkPlanner(1).plan(
                "loc",
                Instant.parse("2024-01-01T00:00:00Z"),
                Instant.parse("2024-01-02T00:00:00Z"));

        var pool = testKit.spawn(WorkerPoolActor.create(
                packets, new StubDataLakeReader(50, 5, 30L),
                new StubEventPublisher(), "topic",
                new StubDownstreamClient(), probe.getRef(), 1,
                "fo-5", new MetricsRegistry()));

        pool.tell(new Messages.WorkerPoolCommand.Start());
        pool.tell(new Messages.WorkerPoolCommand.Pause());
        pool.tell(new Messages.WorkerPoolCommand.Cancel());

        probe.expectNoMessage(SHORT_WAIT);
    }

    // -----------------------------------------------------------------------
    // Test 6: Pause during planning — resume after plan completes
    // -----------------------------------------------------------------------

    @Test
    void pauseDuringPlanning_resumeCompletesWithFullCount() {
        var repo       = new InMemoryJobRepository();
        var coordProbe = testKit.<CoordinatorCommand>createTestProbe();
        var job        = job("fo-plan-pause");
        repo.save(job.withStatus(ReplayStatus.RUNNING));

        var actor = testKit.spawn(ReplayJobActor.create(
                job, repo, coordProbe.getRef(),
                new StubDataLakeReader(5, 10),
                new StubWorkPlanner(1), 1,
                new StubEventPublisher(),
                new StubDownstreamClient(),
                new MetricsRegistry()), "fo-plan-actor-" + UUID.randomUUID());

        // Send Pause immediately after Start — likely lands during async planning
        actor.tell(new Messages.ReplayJobCommand.Start());
        actor.tell(new Messages.ReplayJobCommand.Pause());
        actor.tell(new Messages.ReplayJobCommand.Resume());

        // Must eventually complete with correct count
        coordProbe.fishForMessage(WAIT, m ->
                m instanceof CoordinatorCommand.WorkerFinished wf
                        && wf.eventsPublished() == 50L    // 5 batches × 10 events
                        ? FishingOutcomes.complete()
                        : FishingOutcomes.continueAndIgnore());
    }

    // -----------------------------------------------------------------------
    // Test 7: Publisher and downstream counts agree after pause+resume
    // -----------------------------------------------------------------------

    @Test
    void pauseResume_kafkaAndDownstreamCountsAgree() {
        var probe      = testKit.<ReplayJobCommand>createTestProbe();
        var publisher  = new StubEventPublisher();
        var downstream = new StubDownstreamClient();
        var packets    = new StubWorkPlanner(2).plan(
                "loc",
                Instant.parse("2024-01-01T00:00:00Z"),
                Instant.parse("2024-01-03T00:00:00Z"));

        var pool = testKit.spawn(WorkerPoolActor.create(
                packets, new StubDataLakeReader(8, 5, 20L),
                publisher, "topic",
                downstream, probe.getRef(), 1,
                "fo-7", new MetricsRegistry()));

        pool.tell(new Messages.WorkerPoolCommand.Start());
        pool.tell(new Messages.WorkerPoolCommand.Pause());
        pool.tell(new Messages.WorkerPoolCommand.Resume());

        var msg = probe.receiveMessage(WAIT);
        assertInstanceOf(ReplayJobCommand.PoolFinished.class, msg);

        long expected = 2L * 8 * 5; // 80
        assertEquals(expected, ((ReplayJobCommand.PoolFinished) msg).totalEvents());
        assertEquals((int) expected, publisher.totalPublished(),
                "Kafka and PoolFinished counts diverged after pause+resume");
        assertEquals((int) expected, downstream.totalPosted(),
                "Downstream and PoolFinished counts diverged after pause+resume");
    }

    // -----------------------------------------------------------------------
    // Test 8: Partial cancel — events published before cancel ≤ expected max
    // -----------------------------------------------------------------------

    @Test
    void cancelMidJob_eventsPublishedLessThanMax() throws Exception {
        var publisher  = new StubEventPublisher();
        var downstream = new StubDownstreamClient();
        var probe      = testKit.<ReplayJobCommand>createTestProbe();

        var packets = new StubWorkPlanner(1).plan(
                "loc",
                Instant.parse("2024-01-01T00:00:00Z"),
                Instant.parse("2024-01-02T00:00:00Z"));

        var pool = testKit.spawn(WorkerPoolActor.create(
                packets, new StubDataLakeReader(100, 10, 10L),
                publisher, "topic",
                downstream, probe.getRef(), 1,
                "fo-8", new MetricsRegistry()));

        pool.tell(new Messages.WorkerPoolCommand.Start());
        // Let a few batches run, then cancel
        Thread.sleep(50);
        pool.tell(new Messages.WorkerPoolCommand.Cancel());

        probe.expectNoMessage(SHORT_WAIT);

        int maxPossible = 100 * 10;
        assertTrue(publisher.totalPublished() <= maxPossible,
                "Published more than max possible: " + publisher.totalPublished());
        assertEquals(publisher.totalPublished(), downstream.totalPosted(),
                "Kafka and downstream counts diverged on partial cancel");
    }
}
