package com.example.replay.integration;

import com.example.replay.actors.Messages;
import com.example.replay.actors.Messages.ReplayJobCommand;
import com.example.replay.actors.StubWorkPlanner;
import com.example.replay.actors.WorkerPoolActor;
import com.example.replay.datalake.StubDataLakeReader;
import com.example.replay.downstream.StubDownstreamClient;
import com.example.replay.integration.SkewedDistributionTest.SkewedDataLakeReader;
import com.example.replay.kafka.StubEventPublisher;
import com.example.replay.metrics.MetricsRegistry;
import com.example.replay.model.SecurityEvent;
import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests verifying the dual-path event emission model.
 *
 * <p>Each batch of events is published concurrently to two sinks:
 * <ol>
 *   <li><b>Kafka</b> — via {@link com.example.replay.kafka.EventPublisher}, keyed by {@code cid}.</li>
 *   <li><b>Downstream REST</b> — via {@link com.example.replay.downstream.DownstreamClient}.</li>
 * </ol>
 *
 * <p>Tests confirm:
 * <ul>
 *   <li>Both sinks receive exactly the same event count.</li>
 *   <li>The Kafka partition key recorded by {@link StubEventPublisher} is {@code cid}.</li>
 *   <li>A failing downstream causes the pool to report failure, not silent data loss.</li>
 *   <li>Skewed data flows correctly through both emission paths end-to-end.</li>
 * </ul>
 *
 * <p>Low-level {@link com.example.replay.kafka.KafkaEventPublisher} / MockProducer
 * tests (cid-as-key, topic routing, large batches) live in
 * {@code com.example.replay.kafka.KafkaEventPublisherTest}.
 */
class EmissionTest {

    static final ActorTestKit testKit = ActorTestKit.create();

    @AfterAll
    static void teardown() { testKit.shutdownTestKit(); }

    private static final Duration WAIT = Duration.ofSeconds(15);

    // -----------------------------------------------------------------------
    // Test 1: Kafka and downstream receive the same event count — single packet
    // -----------------------------------------------------------------------

    @Test
    void singlePacket_kafkaAndDownstreamReceiveSameCount() {
        var probe      = testKit.<ReplayJobCommand>createTestProbe();
        var publisher  = new StubEventPublisher();
        var downstream = new StubDownstreamClient();
        var packets    = new StubWorkPlanner(1).plan(
                "loc",
                Instant.parse("2024-01-01T00:00:00Z"),
                Instant.parse("2024-01-02T00:00:00Z"));

        var pool = testKit.spawn(WorkerPoolActor.create(
                packets, new StubDataLakeReader(5, 20),
                publisher, "topic",
                downstream, probe.getRef(), 1,
                "emit-1", new MetricsRegistry()));
        pool.tell(new Messages.WorkerPoolCommand.Start());
        probe.receiveMessage(WAIT);

        int expected = 5 * 20; // 100
        assertEquals(expected, publisher.totalPublished(),  "Kafka received wrong count");
        assertEquals(expected, downstream.totalPosted(),    "Downstream received wrong count");
    }

    // -----------------------------------------------------------------------
    // Test 2: Kafka and downstream receive the same event count — multiple packets
    // -----------------------------------------------------------------------

    @Test
    void multiplePackets_kafkaAndDownstreamReceiveSameCount() {
        var probe      = testKit.<ReplayJobCommand>createTestProbe();
        var publisher  = new StubEventPublisher();
        var downstream = new StubDownstreamClient();
        var packets    = new StubWorkPlanner(5).plan(
                "loc",
                Instant.parse("2024-01-01T00:00:00Z"),
                Instant.parse("2024-01-06T00:00:00Z"));

        var pool = testKit.spawn(WorkerPoolActor.create(
                packets, new StubDataLakeReader(4, 10),
                publisher, "topic",
                downstream, probe.getRef(), 2,
                "emit-2", new MetricsRegistry()));
        pool.tell(new Messages.WorkerPoolCommand.Start());
        probe.receiveMessage(WAIT);

        int expected = 5 * 4 * 10; // 200
        assertEquals(expected, publisher.totalPublished(), "Kafka count wrong");
        assertEquals(expected, downstream.totalPosted(),   "Downstream count wrong");
    }

    // -----------------------------------------------------------------------
    // Test 3: Published keys are cid values — not eventId values
    // -----------------------------------------------------------------------

    @Test
    void publishedKeys_areEventCids_notEventIds() {
        var probe     = testKit.<ReplayJobCommand>createTestProbe();
        var publisher = new StubEventPublisher();
        var packets   = new StubWorkPlanner(1).plan(
                "loc",
                Instant.parse("2024-01-01T00:00:00Z"),
                Instant.parse("2024-01-02T00:00:00Z"));

        var pool = testKit.spawn(WorkerPoolActor.create(
                packets, new StubDataLakeReader(3, 10),
                publisher, "topic",
                new StubDownstreamClient(), probe.getRef(), 1,
                "emit-3", new MetricsRegistry()));
        pool.tell(new Messages.WorkerPoolCommand.Start());
        probe.receiveMessage(WAIT);

        var keys = publisher.publishedKeys();
        assertFalse(keys.isEmpty(), "No keys published");

        // StubDataLakeReader generates cids like "cust-001" … "cust-010"
        keys.forEach(k -> {
            assertFalse(k.startsWith("evt-"),  "Key looks like an eventId: " + k);
            assertTrue(k.startsWith("cust-"), "Expected cust-XXX cid, got: " + k);
        });
    }

    // -----------------------------------------------------------------------
    // Test 4: Number of distinct cids matches expected customer spread
    // -----------------------------------------------------------------------

    @Test
    void publishedKeys_distinctCidCount_matchesCustomerSpread() {
        var probe     = testKit.<ReplayJobCommand>createTestProbe();
        var publisher = new StubEventPublisher();
        // StubDataLakeReader cycles through cust-001 … cust-010 (10 distinct)
        var packets   = new StubWorkPlanner(1).plan(
                "loc",
                Instant.parse("2024-01-01T00:00:00Z"),
                Instant.parse("2024-01-02T00:00:00Z"));

        var pool = testKit.spawn(WorkerPoolActor.create(
                packets, new StubDataLakeReader(5, 20),
                publisher, "topic",
                new StubDownstreamClient(), probe.getRef(), 1,
                "emit-4", new MetricsRegistry()));
        pool.tell(new Messages.WorkerPoolCommand.Start());
        probe.receiveMessage(WAIT);

        Set<String> distinctCids = Set.copyOf(publisher.publishedKeys());
        // 5 batches × 20 events; cid cycles mod 10 → 10 distinct customers
        assertEquals(10, distinctCids.size(),
                "Expected 10 distinct customer cids, got: " + distinctCids);
    }

    // -----------------------------------------------------------------------
    // Test 5: Skewed data — whale customer dominates published keys
    // -----------------------------------------------------------------------

    @Test
    void skewedLoad_whaleCidDominatesPublishedKeys() {
        var probe     = testKit.<ReplayJobCommand>createTestProbe();
        var publisher = new StubEventPublisher();
        var reader    = new SkewedDataLakeReader(4, 20); // 80 % whale

        var packets = new StubWorkPlanner(2).plan(
                "loc",
                Instant.parse("2024-01-01T00:00:00Z"),
                Instant.parse("2024-01-03T00:00:00Z"));

        var pool = testKit.spawn(WorkerPoolActor.create(
                packets, reader, publisher, "topic",
                new StubDownstreamClient(), probe.getRef(), 2,
                "emit-5", new MetricsRegistry()));
        pool.tell(new Messages.WorkerPoolCommand.Start());
        probe.receiveMessage(WAIT);

        var keys       = publisher.publishedKeys();
        long whaleKeys = keys.stream()
                .filter(SkewedDataLakeReader.WHALE_CID::equals)
                .count();

        // ~80 % of 2×4×20 = 160 events should be whale
        assertTrue(whaleKeys >= keys.size() * 0.75,
                "Expected ≥75 %% whale keys, got %d / %d".formatted(whaleKeys, keys.size()));
    }

    // -----------------------------------------------------------------------
    // Test 6: Skewed load — both sinks mirror the same count
    // -----------------------------------------------------------------------

    @Test
    void skewedLoad_actorPipeline_downstreamMirrorsKafkaCount() {
        var probe      = testKit.<ReplayJobCommand>createTestProbe();
        var publisher  = new StubEventPublisher();
        var downstream = new StubDownstreamClient();
        var reader     = new SkewedDataLakeReader(5, 20);
        var packets    = new StubWorkPlanner(3).plan(
                "loc",
                Instant.parse("2024-01-01T00:00:00Z"),
                Instant.parse("2024-01-04T00:00:00Z"));

        var pool = testKit.spawn(WorkerPoolActor.create(
                packets, reader, publisher, "topic",
                downstream, probe.getRef(), 2,
                "emit-6", new MetricsRegistry()));
        pool.tell(new Messages.WorkerPoolCommand.Start());
        probe.receiveMessage(WAIT);

        assertEquals(publisher.totalPublished(), downstream.totalPosted(),
                "Kafka and downstream counts diverged");
        assertEquals(3 * 5 * 20, publisher.totalPublished(),
                "Wrong total event count");
    }

    // -----------------------------------------------------------------------
    // Test 7: Failing downstream propagates as PoolFailed — not silent loss
    // -----------------------------------------------------------------------

    @Test
    void failingDownstream_propagatesAsPoolFailed() {
        var failingDownstream = new com.example.replay.downstream.DownstreamClient() {
            @Override
            public CompletableFuture<Integer> post(List<SecurityEvent> events) {
                return CompletableFuture.failedFuture(
                        new RuntimeException("downstream unavailable"));
            }
            @Override public void close() {}
        };

        var probe   = testKit.<ReplayJobCommand>createTestProbe();
        var packets = new StubWorkPlanner(1).plan(
                "loc",
                Instant.parse("2024-01-01T00:00:00Z"),
                Instant.parse("2024-01-02T00:00:00Z"));

        var pool = testKit.spawn(WorkerPoolActor.create(
                packets, new StubDataLakeReader(3, 10),
                new StubEventPublisher(), "topic",
                failingDownstream, probe.getRef(), 1,
                "emit-7", new MetricsRegistry()));
        pool.tell(new Messages.WorkerPoolCommand.Start());

        var msg = probe.receiveMessage(WAIT);
        assertInstanceOf(ReplayJobCommand.PoolFailed.class, msg,
                "Expected PoolFailed when downstream is unavailable, got: " + msg);
    }

    // -----------------------------------------------------------------------
    // Test 8: Failing Kafka propagates as PoolFailed — not silent loss
    // -----------------------------------------------------------------------

    @Test
    void failingKafka_propagatesAsPoolFailed() {
        var failingPublisher = new com.example.replay.kafka.EventPublisher() {
            @Override
            public CompletableFuture<Integer> publish(String topic,
                    List<SecurityEvent> events) {
                return CompletableFuture.failedFuture(
                        new RuntimeException("Kafka broker unreachable"));
            }
            @Override public void close() {}
        };

        var probe   = testKit.<ReplayJobCommand>createTestProbe();
        var packets = new StubWorkPlanner(1).plan(
                "loc",
                Instant.parse("2024-01-01T00:00:00Z"),
                Instant.parse("2024-01-02T00:00:00Z"));

        var pool = testKit.spawn(WorkerPoolActor.create(
                packets, new StubDataLakeReader(3, 10),
                failingPublisher, "topic",
                new StubDownstreamClient(), probe.getRef(), 1,
                "emit-8", new MetricsRegistry()));
        pool.tell(new Messages.WorkerPoolCommand.Start());

        var msg = probe.receiveMessage(WAIT);
        assertInstanceOf(ReplayJobCommand.PoolFailed.class, msg,
                "Expected PoolFailed when Kafka is unavailable, got: " + msg);
    }
}
