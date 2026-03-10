package com.example.replay.kafka;

import com.example.replay.model.SecurityEvent;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Port for publishing security events to a downstream messaging system.
 */
public interface EventPublisher {

    /**
     * Publishes a batch of events to the given topic.
     *
     * @param topic  destination topic / channel
     * @param events ordered batch to publish
     * @return future that completes with the number of events successfully sent
     */
    CompletableFuture<Integer> publish(String topic, List<SecurityEvent> events);

    /** Gracefully flushes and closes any underlying connections. */
    void close();
}
