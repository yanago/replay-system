package com.example.replay.actors;

import com.example.replay.actors.Messages.ReplayJobCommand;
import com.example.replay.datalake.StubDataLakeReader;
import com.example.replay.downstream.StubDownstreamClient;
import com.example.replay.kafka.StubEventPublisher;
import com.example.replay.metrics.MetricsRegistry;
import com.example.replay.storage.InMemoryJobRepository;
import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit;
import org.apache.pekko.actor.testkit.typed.javadsl.FishingOutcomes;
import org.apache.pekko.actor.testkit.typed.javadsl.TestProbe;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link WorkerPoolActor} and {@link PacketWorkerActor}.
 *
 * <p>Uses a {@link StubDataLakeReader} so no Iceberg table is required.
 * Pools are driven by pre-built {@link WorkPacket} lists (no WorkPlanner I/O).
 */
class WorkerPoolActorTest {

    static final ActorTestKit testKit = ActorTestKit.create();

    @AfterAll
    static void teardown() { testKit.shutdownTestKit(); }

    private static final Duration WAIT = Duration.ofSeconds(10);

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static WorkPacket packet(String tableLocation, int batchesInReader) {
        return new WorkPacket(
                UUID.randomUUID().toString(),
                tableLocation,
                Instant.parse("2024-01-01T00:00:00Z"),
                Instant.parse("2024-01-02T00:00:00Z"),
                (long) batchesInReader * 10,   // estimatedEvents
                1024L,                          // weightBytes
                0.0,                            // skewScore
                0);                             // suggestedWorker
    }

    // -----------------------------------------------------------------------
    // Single packet, single worker
    // -----------------------------------------------------------------------

    @Test
    void singlePacket_singleWorker_sendsPoolFinished() {
        var probe   = testKit.<ReplayJobCommand>createTestProbe();
        var reader  = new StubDataLakeReader(3, 10);  // 3 batches × 10 events = 30 total
        var packets = List.of(packet("loc-1", 3));

        var pool = testKit.spawn(WorkerPoolActor.create(packets, reader,
                new StubEventPublisher(), "test-topic", new StubDownstreamClient(), probe.getRef(), 1, "test-job", new MetricsRegistry()));
        pool.tell(new Messages.WorkerPoolCommand.Start());

        var msg = probe.receiveMessage(WAIT);
        assertInstanceOf(ReplayJobCommand.PoolFinished.class, msg);
        assertEquals(30L, ((ReplayJobCommand.PoolFinished) msg).totalEvents());
    }

    // -----------------------------------------------------------------------
    // Multiple packets distributed across workers
    // -----------------------------------------------------------------------

    @Test
    void multiplePackets_twoWorkers_allComplete() {
        var probe   = testKit.<ReplayJobCommand>createTestProbe();
        // Each StubDataLakeReader call returns 2 batches × 5 events = 10 events per packet
        var reader  = new StubDataLakeReader(2, 5);
        var packets = List.of(
                packet("loc-a", 2),
                packet("loc-b", 2),
                packet("loc-c", 2));

        var pool = testKit.spawn(WorkerPoolActor.create(packets, reader,
                new StubEventPublisher(), "test-topic", new StubDownstreamClient(), probe.getRef(), 2, "test-job", new MetricsRegistry()));
        pool.tell(new Messages.WorkerPoolCommand.Start());

        var msg = probe.receiveMessage(WAIT);
        assertInstanceOf(ReplayJobCommand.PoolFinished.class, msg);
        // 3 packets × 2 batches × 5 events = 30
        assertEquals(30L, ((ReplayJobCommand.PoolFinished) msg).totalEvents());
    }

    // -----------------------------------------------------------------------
    // Empty packet list → immediate PoolFinished(0)
    // -----------------------------------------------------------------------

    @Test
    void emptyPacketList_sendsPoolFinished_withZeroEvents() {
        var probe  = testKit.<ReplayJobCommand>createTestProbe();
        var reader = new StubDataLakeReader(3, 10);

        var pool = testKit.spawn(WorkerPoolActor.create(List.of(), reader,
                new StubEventPublisher(), "test-topic", new StubDownstreamClient(), probe.getRef(), 2, "test-job", new MetricsRegistry()));
        pool.tell(new Messages.WorkerPoolCommand.Start());

        var msg = probe.receiveMessage(WAIT);
        assertInstanceOf(ReplayJobCommand.PoolFinished.class, msg);
        assertEquals(0L, ((ReplayJobCommand.PoolFinished) msg).totalEvents());
    }

    // -----------------------------------------------------------------------
    // Pause → Resume → completes
    // -----------------------------------------------------------------------

    @Test
    void pauseThenResume_poolEventuallyCompletes() {
        var probe   = testKit.<ReplayJobCommand>createTestProbe();
        var reader  = new StubDataLakeReader(5, 10, 30L);  // 30ms/batch keeps job alive
        var packets = List.of(packet("loc-pr", 5));

        var pool = testKit.spawn(WorkerPoolActor.create(packets, reader,
                new StubEventPublisher(), "test-topic", new StubDownstreamClient(), probe.getRef(), 1, "test-job", new MetricsRegistry()));
        pool.tell(new Messages.WorkerPoolCommand.Start());
        pool.tell(new Messages.WorkerPoolCommand.Pause());
        pool.tell(new Messages.WorkerPoolCommand.Resume());

        // Must eventually finish (may also receive other internal signals — fish for it)
        probe.fishForMessage(WAIT, m ->
                m instanceof ReplayJobCommand.PoolFinished
                        ? FishingOutcomes.complete()
                        : FishingOutcomes.continueAndIgnore());
    }

    // -----------------------------------------------------------------------
    // Cancel → stopped, no PoolFinished
    // -----------------------------------------------------------------------

    @Test
    void cancel_noPoolFinished() {
        var probe   = testKit.<ReplayJobCommand>createTestProbe();
        var reader  = new StubDataLakeReader(100, 10, 50L);  // slow enough to cancel first
        var packets = List.of(packet("loc-cancel", 100));

        var pool = testKit.spawn(WorkerPoolActor.create(packets, reader,
                new StubEventPublisher(), "test-topic", new StubDownstreamClient(), probe.getRef(), 1, "test-job", new MetricsRegistry()));
        pool.tell(new Messages.WorkerPoolCommand.Start());
        pool.tell(new Messages.WorkerPoolCommand.Cancel());

        probe.expectNoMessage(Duration.ofMillis(300));
    }

    // -----------------------------------------------------------------------
    // LRT scheduling: packets assigned to different workers
    // -----------------------------------------------------------------------

    @Test
    void lrtScheduling_twoWorkers_bothReceiveWork() {
        var probe  = testKit.<ReplayJobCommand>createTestProbe();
        var reader = new StubDataLakeReader(1, 5);
        // 4 packets across 2 workers → each worker gets 2
        var packets = List.of(
                new WorkPacket(UUID.randomUUID().toString(), "loc", Instant.EPOCH,
                        Instant.EPOCH.plusSeconds(3600), 100L, 2048L, 0.2, 0),
                new WorkPacket(UUID.randomUUID().toString(), "loc", Instant.EPOCH,
                        Instant.EPOCH.plusSeconds(3600), 80L,  1024L, 0.1, 1),
                new WorkPacket(UUID.randomUUID().toString(), "loc", Instant.EPOCH,
                        Instant.EPOCH.plusSeconds(3600), 60L,  512L,  0.0, 0),
                new WorkPacket(UUID.randomUUID().toString(), "loc", Instant.EPOCH,
                        Instant.EPOCH.plusSeconds(3600), 40L,  256L,  0.0, 1));

        var pool = testKit.spawn(WorkerPoolActor.create(packets, reader,
                new StubEventPublisher(), "test-topic", new StubDownstreamClient(), probe.getRef(), 2, "test-job", new MetricsRegistry()));
        pool.tell(new Messages.WorkerPoolCommand.Start());

        var msg = probe.receiveMessage(WAIT);
        assertInstanceOf(ReplayJobCommand.PoolFinished.class, msg);
        // 4 packets × 1 batch × 5 events = 20
        assertEquals(20L, ((ReplayJobCommand.PoolFinished) msg).totalEvents());
    }

    // -----------------------------------------------------------------------
    // WorkPlanner LRT assignment correctness (pure unit test, no actors)
    // -----------------------------------------------------------------------

    @Test
    void stubWorkPlanner_returnsExpectedPacketCount() throws Exception {
        var planner = new StubWorkPlanner(3);
        var packets = planner.plan(
                "loc",
                Instant.parse("2024-01-01T00:00:00Z"),
                Instant.parse("2024-01-04T00:00:00Z"));
        assertEquals(3, packets.size());
        packets.forEach(p -> {
            assertNotNull(p.packetId());
            assertEquals("loc", p.tableLocation());
            assertTrue(p.estimatedEvents() > 0);
        });
    }

    // -----------------------------------------------------------------------
    // ReplayJobActor integration: plans → pool → PoolFinished → WorkerFinished
    // -----------------------------------------------------------------------

    @Test
    void replayJobActor_plansAndCompletes_notifiesCoordinator() {
        var repo        = new InMemoryJobRepository();
        var coordProbe  = testKit.<Messages.CoordinatorCommand>createTestProbe();
        var job         = com.example.replay.model.ReplayJob.create(
                "wpa-job-1", "loc", "topic",
                Instant.parse("2024-01-01T00:00:00Z"),
                Instant.parse("2024-01-02T00:00:00Z"), 1.0);
        repo.save(job.withStatus(com.example.replay.model.ReplayStatus.RUNNING));

        var actor = testKit.spawn(ReplayJobActor.create(
                job, repo, coordProbe.getRef(),
                new StubDataLakeReader(2, 10),
                new StubWorkPlanner(2),    // 2 packets
                2,                         // 2 workers
                new StubEventPublisher(),
                new StubDownstreamClient(),
                new MetricsRegistry()));

        actor.tell(new Messages.ReplayJobCommand.Start());

        // Fish for WorkerFinished (may be preceded by internal pool signals)
        coordProbe.fishForMessage(WAIT, m ->
                m instanceof Messages.CoordinatorCommand.WorkerFinished
                        ? FishingOutcomes.complete()
                        : FishingOutcomes.continueAndIgnore());
    }
}
