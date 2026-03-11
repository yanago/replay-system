package com.example.replay.integration;

import com.example.replay.actors.JobManager;
import com.example.replay.actors.Messages;
import com.example.replay.actors.Messages.CoordinatorCommand;
import com.example.replay.actors.Messages.ReplayJobCommand;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Large-scale integration tests for replay job creation and control.
 *
 * <p>Validates:
 * <ul>
 *   <li>Many work packets distributed across multiple workers produce the exact
 *       expected total event count with zero loss.</li>
 *   <li>Multiple jobs submitted concurrently all complete independently.</li>
 *   <li>The metrics registry accurately reflects completion state.</li>
 *   <li>High-throughput (no delay) scenarios do not corrupt event counts.</li>
 * </ul>
 */
class LargeScaleReplayTest {

    static final ActorTestKit testKit = ActorTestKit.create();

    @AfterAll
    static void teardown() { testKit.shutdownTestKit(); }

    private static final Duration LONG_WAIT = Duration.ofSeconds(30);

    // -----------------------------------------------------------------------
    // Helper
    // -----------------------------------------------------------------------

    private static ReplayJob job(String id) {
        return ReplayJob.create(id, "db.events", "replay-topic",
                Instant.parse("2024-01-01T00:00:00Z"),
                Instant.parse("2024-02-01T00:00:00Z"), 1.0);
    }

    // -----------------------------------------------------------------------
    // Test 1: 15 packets across 4 workers — exact event count
    // -----------------------------------------------------------------------

    @Test
    void fifteenPackets_fourWorkers_exactEventCount() {
        var probe   = testKit.<ReplayJobCommand>createTestProbe();
        var reader  = new StubDataLakeReader(3, 20);   // 3 batches × 20 events = 60 per packet
        var planner = new StubWorkPlanner(15);         // 15 packets

        var packets = planner.plan(
                "loc",
                Instant.parse("2024-01-01T00:00:00Z"),
                Instant.parse("2024-01-16T00:00:00Z"));
        assertEquals(15, packets.size());

        var pool = testKit.spawn(WorkerPoolActor.create(
                packets, reader, new StubEventPublisher(), "topic",
                new StubDownstreamClient(), probe.getRef(), 4,
                "large-1", new MetricsRegistry()));
        pool.tell(new Messages.WorkerPoolCommand.Start());

        var msg = probe.receiveMessage(LONG_WAIT);
        assertInstanceOf(ReplayJobCommand.PoolFinished.class, msg);
        assertEquals(15L * 3 * 20, ((ReplayJobCommand.PoolFinished) msg).totalEvents());
    }

    // -----------------------------------------------------------------------
    // Test 2: 20 packets single worker — sequential, no concurrency issues
    // -----------------------------------------------------------------------

    @Test
    void twentyPackets_singleWorker_correctCount() {
        var probe   = testKit.<ReplayJobCommand>createTestProbe();
        var reader  = new StubDataLakeReader(2, 10);
        var packets = new StubWorkPlanner(20).plan(
                "loc",
                Instant.parse("2024-01-01T00:00:00Z"),
                Instant.parse("2024-01-21T00:00:00Z"));

        var pool = testKit.spawn(WorkerPoolActor.create(
                packets, reader, new StubEventPublisher(), "topic",
                new StubDownstreamClient(), probe.getRef(), 1,
                "large-2", new MetricsRegistry()));
        pool.tell(new Messages.WorkerPoolCommand.Start());

        var msg = probe.receiveMessage(LONG_WAIT);
        assertInstanceOf(ReplayJobCommand.PoolFinished.class, msg);
        assertEquals(20L * 2 * 10, ((ReplayJobCommand.PoolFinished) msg).totalEvents());
    }

    // -----------------------------------------------------------------------
    // Test 3: Three concurrent jobs via JobManager — all complete independently
    // -----------------------------------------------------------------------

    @Test
    void threeConcurrentJobs_allComplete_withCorrectCounts() throws Exception {
        var repo     = new InMemoryJobRepository();
        var registry = new MetricsRegistry();
        // Fast reader: 5 batches × 10 events = 50 per packet, 1 packet per job
        var manager  = testKit.spawn(
                JobManager.create(repo, new StubDataLakeReader(5, 10),
                        new StubWorkPlanner(1), 2,
                        new StubEventPublisher(), new StubDownstreamClient(), registry),
                "jm-concurrent-" + UUID.randomUUID());

        var reply  = testKit.<Messages.CoordinatorResponse>createTestProbe();
        var jobIds = new ArrayList<String>();

        for (int i = 0; i < 3; i++) {
            var id = "concurrent-job-" + UUID.randomUUID();
            manager.tell(new CoordinatorCommand.SubmitJob(job(id), reply.getRef()));
            reply.receiveMessage();
            manager.tell(new CoordinatorCommand.StartJob(id, reply.getRef()));
            reply.receiveMessage();
            jobIds.add(id);
        }

        // JobManager handles WorkerFinished internally and updates the repo.
        // Poll the repo until all 3 jobs reach COMPLETED.
        long deadline = System.currentTimeMillis() + LONG_WAIT.toMillis();
        while (System.currentTimeMillis() < deadline) {
            boolean allDone = jobIds.stream().allMatch(id ->
                    repo.findById(id)
                        .map(j -> j.status() == ReplayStatus.COMPLETED)
                        .orElse(false));
            if (allDone) break;
            Thread.sleep(50);
        }

        for (var id : jobIds) {
            var stored = repo.findById(id).orElseThrow(
                    () -> new AssertionError("Job " + id + " not in repo"));
            assertEquals(ReplayStatus.COMPLETED, stored.status(),
                    "Job " + id + " not COMPLETED");
            assertEquals(50L, stored.eventsPublished(),
                    "Wrong event count for job " + id);
        }
    }

    // -----------------------------------------------------------------------
    // Test 4: High-throughput (no delay) — no event count corruption
    // -----------------------------------------------------------------------

    @Test
    void highThroughput_noDelay_eventCountIsExact() {
        var probe      = testKit.<ReplayJobCommand>createTestProbe();
        var publisher  = new StubEventPublisher();
        var downstream = new StubDownstreamClient();
        // 8 packets, 4 workers, 10 batches × 50 events = 500 per packet
        var packets    = new StubWorkPlanner(8).plan(
                "loc",
                Instant.parse("2024-01-01T00:00:00Z"),
                Instant.parse("2024-01-09T00:00:00Z"));

        var pool = testKit.spawn(WorkerPoolActor.create(
                packets, new StubDataLakeReader(10, 50),
                publisher, "topic",
                downstream, probe.getRef(), 4,
                "large-4", new MetricsRegistry()));
        pool.tell(new Messages.WorkerPoolCommand.Start());

        var msg = probe.receiveMessage(LONG_WAIT);
        assertInstanceOf(ReplayJobCommand.PoolFinished.class, msg);

        long expected = 8L * 10 * 50; // 4 000
        assertEquals(expected, ((ReplayJobCommand.PoolFinished) msg).totalEvents());
        assertEquals((int) expected, publisher.totalPublished(),  "Kafka count mismatch");
        assertEquals((int) expected, downstream.totalPosted(),    "Downstream count mismatch");
    }

    // -----------------------------------------------------------------------
    // Test 5: Metrics registry populated accurately after job completion
    // -----------------------------------------------------------------------

    @Test
    void completedJob_registryReflectsCorrectEventCount() throws Exception {
        var repo     = new InMemoryJobRepository();
        var registry = new MetricsRegistry();
        var reply    = testKit.<Messages.CoordinatorResponse>createTestProbe();

        var manager = testKit.spawn(
                JobManager.create(repo, new StubDataLakeReader(4, 25),
                        new StubWorkPlanner(1), 2,
                        new StubEventPublisher(), new StubDownstreamClient(), registry),
                "jm-metrics-" + UUID.randomUUID());

        var id = "metrics-job-ls";
        var j  = job(id);
        manager.tell(new CoordinatorCommand.SubmitJob(j, reply.getRef()));
        reply.receiveMessage();
        manager.tell(new CoordinatorCommand.StartJob(id, reply.getRef()));
        reply.receiveMessage();

        // Wait for COMPLETED
        long deadline = System.currentTimeMillis() + LONG_WAIT.toMillis();
        while (System.currentTimeMillis() < deadline) {
            var stored = repo.findById(id).orElseThrow();
            if (stored.status() == ReplayStatus.COMPLETED) break;
            Thread.sleep(50);
        }

        assertEquals(ReplayStatus.COMPLETED, repo.findById(id).orElseThrow().status());

        var metrics = registry.getMetrics(id).orElseThrow(
                () -> new AssertionError("No metrics entry for " + id));
        assertEquals(4L * 25, metrics.totalEventsPublished(),
                "Registry event count mismatch");
        assertTrue(metrics.totalBatchesProcessed() >= 4,
                "Expected ≥4 batches, got " + metrics.totalBatchesProcessed());
        assertEquals(0L, metrics.totalReadErrors(),    "Unexpected read errors");
        assertEquals(0L, metrics.totalPublishErrors(), "Unexpected publish errors");
    }

    // -----------------------------------------------------------------------
    // Test 6: Many workers, more packets than workers — LRT scheduling correct
    // -----------------------------------------------------------------------

    @Test
    void morePacketsThanWorkers_lrtScheduling_noPacketsDropped() {
        var probe   = testKit.<ReplayJobCommand>createTestProbe();
        var packets = new StubWorkPlanner(12).plan(
                "loc",
                Instant.parse("2024-01-01T00:00:00Z"),
                Instant.parse("2024-01-13T00:00:00Z"));
        assertEquals(12, packets.size());

        var pool = testKit.spawn(WorkerPoolActor.create(
                packets, new StubDataLakeReader(2, 5),
                new StubEventPublisher(), "topic",
                new StubDownstreamClient(), probe.getRef(), 3,
                "large-6", new MetricsRegistry()));
        pool.tell(new Messages.WorkerPoolCommand.Start());

        var msg = probe.receiveMessage(LONG_WAIT);
        assertInstanceOf(ReplayJobCommand.PoolFinished.class, msg);
        assertEquals(12L * 2 * 5, ((ReplayJobCommand.PoolFinished) msg).totalEvents());
    }
}
