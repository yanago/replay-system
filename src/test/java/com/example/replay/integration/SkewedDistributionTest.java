package com.example.replay.integration;

import com.example.replay.actors.Messages;
import com.example.replay.actors.Messages.ReplayJobCommand;
import com.example.replay.actors.WorkPacket;
import com.example.replay.actors.WorkerPoolActor;
import com.example.replay.datalake.DataLakeReader;
import com.example.replay.downstream.StubDownstreamClient;
import com.example.replay.kafka.StubEventPublisher;
import com.example.replay.metrics.MetricsRegistry;
import com.example.replay.model.SecurityEvent;
import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for skewed customer data distribution.
 *
 * <p>Verifies that when a small number of customers (Zipf "whales") own the
 * vast majority of events, the system:
 * <ul>
 *   <li>Completes all work packets correctly with the right event count.</li>
 *   <li>Uses {@code cid} — not {@code eventId} — as the Kafka partition key.</li>
 *   <li>Delivers the same event count to both Kafka and the downstream REST path.</li>
 *   <li>Handles packets with extreme skewScore values without stalling.</li>
 * </ul>
 */
class SkewedDistributionTest {

    static final ActorTestKit testKit = ActorTestKit.create();

    @AfterAll
    static void teardown() { testKit.shutdownTestKit(); }

    private static final Duration WAIT = Duration.ofSeconds(15);

    // -----------------------------------------------------------------------
    // Skewed data lake stub
    // -----------------------------------------------------------------------

    /**
     * Returns events where ~80 % share the same "whale" customer id and
     * the remaining ~20 % are spread across five light customers.
     * This mirrors a Zipf-heavy real-world distribution.
     */
    static final class SkewedDataLakeReader implements DataLakeReader {

        static final String WHALE_CID = "whale-001";

        private final int  totalBatches;
        private final int  eventsPerBatch;
        private final long delayMs;

        SkewedDataLakeReader(int totalBatches, int eventsPerBatch) {
            this(totalBatches, eventsPerBatch, 0L);
        }

        SkewedDataLakeReader(int totalBatches, int eventsPerBatch, long delayMs) {
            this.totalBatches   = totalBatches;
            this.eventsPerBatch = eventsPerBatch;
            this.delayMs        = delayMs;
        }

        @Override
        public List<SecurityEvent> readBatch(
                String loc, Instant from, Instant to, int idx, int size) {
            if (delayMs > 0) {
                try { Thread.sleep(delayMs); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
            if (idx >= totalBatches) return List.of();

            var events = new ArrayList<SecurityEvent>(eventsPerBatch);
            for (int i = 0; i < eventsPerBatch; i++) {
                // First 80 % of each batch → whale, rest → light-01 … light-05
                String cid = (i < eventsPerBatch * 4 / 5)
                        ? WHALE_CID
                        : "light-%02d".formatted(i % 5 + 1);
                events.add(new SecurityEvent(
                        "evt-b%d-i%d".formatted(idx, i), cid,
                        Instant.now(), from.plusSeconds((long) idx * 60 + i).toEpochMilli(),
                        "SKEW_TEST", "10.0.0.1", "host-0", "HIGH", Map.of()));
            }
            return events;
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static WorkPacket packet(String loc, double skewScore, int worker) {
        return new WorkPacket(
                UUID.randomUUID().toString(), loc,
                Instant.parse("2024-01-01T00:00:00Z"),
                Instant.parse("2024-01-02T00:00:00Z"),
                500L, 2048L, skewScore, worker);
    }

    // -----------------------------------------------------------------------
    // Test 1: High-skew packets — pool completes with correct event count
    // -----------------------------------------------------------------------

    @Test
    void poolWithHighSkewPackets_completesSuccessfully() {
        var probe   = testKit.<ReplayJobCommand>createTestProbe();
        var reader  = new SkewedDataLakeReader(5, 20);
        // 4 packets, all with high skewScore, across 2 workers
        var packets = List.of(
                packet("loc-1", 0.9, 0),
                packet("loc-2", 0.9, 1),
                packet("loc-3", 0.8, 0),
                packet("loc-4", 0.7, 1));

        var pool = testKit.spawn(WorkerPoolActor.create(
                packets, reader, new StubEventPublisher(), "topic",
                new StubDownstreamClient(), probe.getRef(), 2,
                "skew-1", new MetricsRegistry()));
        pool.tell(new Messages.WorkerPoolCommand.Start());

        var msg = probe.receiveMessage(WAIT);
        assertInstanceOf(ReplayJobCommand.PoolFinished.class, msg);
        // 4 packets × 5 batches × 20 events = 400
        assertEquals(400L, ((ReplayJobCommand.PoolFinished) msg).totalEvents());
    }

    // -----------------------------------------------------------------------
    // Test 2: Partition key is cid, never eventId
    // -----------------------------------------------------------------------

    @Test
    void cidIsPartitionKey_notEventId() {
        var probe     = testKit.<ReplayJobCommand>createTestProbe();
        var reader    = new SkewedDataLakeReader(3, 10);
        var publisher = new StubEventPublisher();

        var pool = testKit.spawn(WorkerPoolActor.create(
                List.of(packet("loc-key", 0.8, 0)),
                reader, publisher, "topic",
                new StubDownstreamClient(), probe.getRef(), 1,
                "skew-2", new MetricsRegistry()));
        pool.tell(new Messages.WorkerPoolCommand.Start());
        probe.receiveMessage(WAIT);

        var keys = publisher.publishedKeys();
        assertFalse(keys.isEmpty(), "No keys published");

        // Keys must be cid values — stub cids are "whale-001" or "light-NN"
        keys.forEach(k -> assertFalse(k.startsWith("evt-"),
                "Key looks like an eventId: " + k));

        // Whale must dominate (≥ 75 %)
        long whaleCount = keys.stream()
                .filter(SkewedDataLakeReader.WHALE_CID::equals)
                .count();
        assertTrue(whaleCount >= keys.size() * 0.75,
                "Expected ≥75 %% whale keys, got %d/%d".formatted(whaleCount, keys.size()));
    }

    // -----------------------------------------------------------------------
    // Test 3: Both Kafka and downstream receive the complete event count
    // -----------------------------------------------------------------------

    @Test
    void skewedCustomerLoad_allEventsReachBothEmissionPaths() {
        var probe      = testKit.<ReplayJobCommand>createTestProbe();
        var reader     = new SkewedDataLakeReader(4, 15);
        var publisher  = new StubEventPublisher();
        var downstream = new StubDownstreamClient();
        var packets    = List.of(packet("loc-a", 0.9, 0), packet("loc-b", 0.5, 1));

        var pool = testKit.spawn(WorkerPoolActor.create(
                packets, reader, publisher, "topic",
                downstream, probe.getRef(), 2, "skew-3", new MetricsRegistry()));
        pool.tell(new Messages.WorkerPoolCommand.Start());
        probe.receiveMessage(WAIT);

        int expected = 2 * 4 * 15; // 120
        assertEquals(expected, publisher.totalPublished(),  "Kafka count mismatch");
        assertEquals(expected, downstream.totalPosted(),    "Downstream count mismatch");
    }

    // -----------------------------------------------------------------------
    // Test 4: Mixed-skew packets distributed across workers — correct total
    // -----------------------------------------------------------------------

    @Test
    void mixedSkewPackets_acrossMultipleWorkers_correctTotalEvents() {
        var probe  = testKit.<ReplayJobCommand>createTestProbe();
        var reader = new SkewedDataLakeReader(2, 5);
        // 6 packets with varying skewScore and estimatedEvents, across 3 workers
        var packets = List.of(
                new WorkPacket(UUID.randomUUID().toString(), "loc", Instant.EPOCH,
                        Instant.EPOCH.plusSeconds(3600), 1000L, 102400L, 0.9, 0),
                new WorkPacket(UUID.randomUUID().toString(), "loc", Instant.EPOCH,
                        Instant.EPOCH.plusSeconds(3600),  800L,  81920L, 0.7, 1),
                new WorkPacket(UUID.randomUUID().toString(), "loc", Instant.EPOCH,
                        Instant.EPOCH.plusSeconds(3600),  600L,  61440L, 0.5, 2),
                new WorkPacket(UUID.randomUUID().toString(), "loc", Instant.EPOCH,
                        Instant.EPOCH.plusSeconds(3600),  400L,  40960L, 0.3, 0),
                new WorkPacket(UUID.randomUUID().toString(), "loc", Instant.EPOCH,
                        Instant.EPOCH.plusSeconds(3600),  200L,  20480L, 0.1, 1),
                new WorkPacket(UUID.randomUUID().toString(), "loc", Instant.EPOCH,
                        Instant.EPOCH.plusSeconds(3600),  100L,  10240L, 0.0, 2));

        var pool = testKit.spawn(WorkerPoolActor.create(
                packets, reader, new StubEventPublisher(), "topic",
                new StubDownstreamClient(), probe.getRef(), 3,
                "skew-4", new MetricsRegistry()));
        pool.tell(new Messages.WorkerPoolCommand.Start());

        var msg = probe.receiveMessage(WAIT);
        assertInstanceOf(ReplayJobCommand.PoolFinished.class, msg);
        // 6 packets × 2 batches × 5 events = 60
        assertEquals(60L, ((ReplayJobCommand.PoolFinished) msg).totalEvents());
    }

    // -----------------------------------------------------------------------
    // Test 5: Single extreme-skew packet (skewScore = 1.0) still completes
    // -----------------------------------------------------------------------

    @Test
    void extremeSkewPacket_skewScoreOne_completesWithCorrectCount() {
        var probe  = testKit.<ReplayJobCommand>createTestProbe();
        var reader = new SkewedDataLakeReader(6, 8);

        var pool = testKit.spawn(WorkerPoolActor.create(
                List.of(packet("loc-extreme", 1.0, 0)),
                reader, new StubEventPublisher(), "topic",
                new StubDownstreamClient(), probe.getRef(), 1,
                "skew-5", new MetricsRegistry()));
        pool.tell(new Messages.WorkerPoolCommand.Start());

        var msg = probe.receiveMessage(WAIT);
        assertInstanceOf(ReplayJobCommand.PoolFinished.class, msg);
        assertEquals(48L, ((ReplayJobCommand.PoolFinished) msg).totalEvents()); // 6×8
    }
}
