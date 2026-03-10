package com.example.replay.kafka;

import com.example.replay.model.SecurityEvent;
import com.example.replay.util.JsonUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
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
 * Uses {@code eventId} as the record key for deterministic partitioning.
 */
public final class KafkaEventPublisher implements EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventPublisher.class);

    private final KafkaProducer<String, String> producer;

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
                .thenApply(_ -> sent.get());
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }

    private ProducerRecord<String, String> toRecord(String topic, SecurityEvent event) {
        var json = JsonUtils.toJson(event);
        return new ProducerRecord<>(topic, event.eventId(), json);
    }
}
