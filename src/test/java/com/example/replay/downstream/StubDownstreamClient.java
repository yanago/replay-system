package com.example.replay.downstream;

import com.example.replay.model.SecurityEvent;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * In-memory {@link DownstreamClient} test double.
 *
 * <p>Accepts every batch immediately and counts the total events posted,
 * enabling test assertions on how many events were forwarded downstream.
 */
public final class StubDownstreamClient implements DownstreamClient {

    private final AtomicInteger total = new AtomicInteger(0);

    @Override
    public CompletableFuture<Integer> post(List<SecurityEvent> events) {
        total.addAndGet(events.size());
        return CompletableFuture.completedFuture(events.size());
    }

    @Override
    public void close() {}

    /** Total events accepted across all {@link #post} calls. */
    public int totalPosted() { return total.get(); }
}
