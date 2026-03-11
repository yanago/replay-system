package com.example.replay.kafka;

import com.example.replay.model.SecurityEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link EventPublisher} used when no Kafka bootstrap servers are configured.
 *
 * <p>Accepts every batch immediately, counting events for observability but
 * not writing to any broker.  Useful for local development and testing without
 * a running Kafka cluster.
 */
public final class NoOpEventPublisher implements EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(NoOpEventPublisher.class);

    private final AtomicLong totalPublished = new AtomicLong(0);

    @Override
    public CompletableFuture<Integer> publish(String topic, List<SecurityEvent> events) {
        if (!events.isEmpty()) {
            totalPublished.addAndGet(events.size());
            log.debug("NoOp publisher: counted {} events for topic '{}' (no Kafka configured)",
                    events.size(), topic);
        }
        return CompletableFuture.completedFuture(events.size());
    }

    @Override
    public void close() {
        log.info("NoOpEventPublisher closed — total events counted: {}", totalPublished.get());
    }
}
