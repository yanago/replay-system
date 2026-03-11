package com.example.replay.kafka;

import com.example.replay.model.SecurityEvent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * In-memory {@link EventPublisher} test double.
 *
 * <p>Records the partition key (cid) of every event published so tests can
 * assert that customer-based partitioning is applied correctly.
 */
public final class StubEventPublisher implements EventPublisher {

    /** All cid values passed as record keys, in arrival order. */
    private final List<String> publishedKeys = new CopyOnWriteArrayList<>();

    @Override
    public CompletableFuture<Integer> publish(String topic, List<SecurityEvent> events) {
        events.forEach(e -> publishedKeys.add(e.cid()));
        return CompletableFuture.completedFuture(events.size());
    }

    @Override
    public void close() {}

    /** Returns an unmodifiable snapshot of all cid keys published so far. */
    public List<String> publishedKeys() {
        return Collections.unmodifiableList(new ArrayList<>(publishedKeys));
    }

    /** Total events published across all calls. */
    public int totalPublished() { return publishedKeys.size(); }
}
