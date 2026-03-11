package com.example.replay.kafka;

import com.example.replay.model.SecurityEvent;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link KafkaEventPublisher}.
 *
 * <p>Uses Kafka's built-in {@link MockProducer} — no broker required.
 * Verifies that {@code cid} is used as the partition key, ensuring
 * all events for the same customer land in the same Kafka partition.
 */
class KafkaEventPublisherTest {

    private static SecurityEvent event(String eventId, String cid) {
        return new SecurityEvent(eventId, cid, Instant.now(), Instant.now(),
                "TEST", "1.2.3.4", "host", "LOW", Map.of());
    }

    /** Creates a publisher backed by a MockProducer (which implements Producer). */
    private static KafkaEventPublisher publisherWith(MockProducer<String, String> mock) {
        // MockProducer implements Producer<K,V> — compatible with the package-private ctor
        return new KafkaEventPublisher((org.apache.kafka.clients.producer.Producer<String, String>) mock);
    }

    // -----------------------------------------------------------------------
    // Partition key = cid
    // -----------------------------------------------------------------------

    @Test
    void publish_usesCidAsPartitionKey() throws Exception {
        var mock      = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        var publisher = publisherWith(mock);

        var events = List.of(
                event("evt-1", "cust-007"),
                event("evt-2", "cust-007"),
                event("evt-3", "cust-042"));

        var result = publisher.publish("security-events", events).get();

        assertEquals(3, result, "should return count of all sent events");

        var records = mock.history();
        assertEquals(3, records.size());

        // All records for cust-007 use cid as key
        records.stream()
               .filter(r -> r.key().equals("cust-007"))
               .forEach(r -> assertEquals("cust-007", r.key()));

        // Record for cust-042 uses its own cid
        var rec042 = records.stream().filter(r -> r.key().equals("cust-042")).findFirst();
        assertTrue(rec042.isPresent());
        assertEquals("cust-042", rec042.get().key());
    }

    @Test
    void publish_keyIsNotEventId() throws Exception {
        var mock      = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        var publisher = publisherWith(mock);

        var event = event("unique-event-id-xyz", "cust-001");
        publisher.publish("topic", List.of(event)).get();

        ProducerRecord<String, String> record = mock.history().get(0);
        assertEquals("cust-001", record.key(), "key must be cid, not eventId");
        assertNotEquals("unique-event-id-xyz", record.key(), "eventId must not be used as key");
    }

    @Test
    void publish_sameCustomer_sameKey_acrossMultipleBatches() throws Exception {
        var mock      = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        var publisher = publisherWith(mock);

        // Two separate publish calls for the same customer
        publisher.publish("t", List.of(event("e1", "cust-X"))).get();
        publisher.publish("t", List.of(event("e2", "cust-X"))).get();

        var keys = mock.history().stream().map(ProducerRecord::key).toList();
        assertEquals(List.of("cust-X", "cust-X"), keys,
                "same cid must produce same key across batches");
    }

    // -----------------------------------------------------------------------
    // Topic routing
    // -----------------------------------------------------------------------

    @Test
    void publish_usesSuppliedTopic() throws Exception {
        var mock      = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        var publisher = publisherWith(mock);

        publisher.publish("my-replay-topic", List.of(event("e1", "c1"))).get();

        assertEquals("my-replay-topic", mock.history().get(0).topic());
    }

    // -----------------------------------------------------------------------
    // Empty batch
    // -----------------------------------------------------------------------

    @Test
    void publish_emptyBatch_returnsZero() throws Exception {
        var mock      = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        var publisher = publisherWith(mock);

        var result = publisher.publish("t", List.of()).get();

        assertEquals(0, result);
        assertTrue(mock.history().isEmpty());
    }
}
