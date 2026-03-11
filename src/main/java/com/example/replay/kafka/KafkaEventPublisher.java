package com.example.replay.kafka;

import com.example.replay.model.SecurityEvent;
import com.example.replay.util.JsonUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Kafka-backed {@link EventPublisher}.
 *
 * <p>Uses {@code cid} (customer ID) as the record partition key so that all
 * events for the same customer are ordered within a single partition.  This is
 * critical for Zipf-skewed workloads: heavy customers produce many events, and
 * routing them by {@code cid} ensures that downstream consumers can process a
 * customer's full event stream sequentially without cross-partition merging.
 */
public final class KafkaEventPublisher implements EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventPublisher.class);

    private final Producer<String, String> producer;

    /**
     * Creates a publisher from explicit bootstrap configuration.
     *
     * @param bootstrapServers comma-separated broker list
     * @param extraProps       additional producer properties (acks, retries, …)
     */
    public KafkaEventPublisher(String bootstrapServers, Properties extraProps) {
        var props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer",   StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("acks", "all");
        props.put("retries", "3");
        props.put("enable.idempotence", "true");
        props.putAll(extraProps);
        this.producer = new KafkaProducer<>(props);
    }

    /** Package-private constructor for unit tests — accepts any {@link Producer} implementation. */
    KafkaEventPublisher(Producer<String, String> producer) {
        this.producer = producer;
    }

    @Override
    public CompletableFuture<Integer> publish(String topic, List<SecurityEvent> events) {
        var sent    = new AtomicInteger(0);
        var futures = events.stream()
                .map(event -> toRecord(topic, event))
                .map(record -> {
                    var cf = new CompletableFuture<Void>();
                    producer.send(record, (meta, ex) -> {
                        if (ex != null) {
                            log.error("Failed to send event {}: {}", record.key(), ex.getMessage());
                            cf.completeExceptionally(ex);
                        } else {
                            sent.incrementAndGet();
                            cf.complete(null);
                        }
                    });
                    return cf;
                })
                .toArray(CompletableFuture[]::new);

        return CompletableFuture.allOf(futures)
                .thenApply(ignored -> sent.get());
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }

    private ProducerRecord<String, String> toRecord(String topic, SecurityEvent event) {
        var json = JsonUtils.toJson(event);
        // Partition key = cid so all events for the same customer land in one partition.
        // Heavy (Zipf-frequent) customers are naturally spread across partitions by
        // Kafka's default murmur2 hash of the key string.
        return new ProducerRecord<>(topic, event.cid(), json);
    }
}
