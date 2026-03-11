package com.example.replay.downstream;

import com.example.replay.model.SecurityEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * No-op {@link DownstreamClient} used when no real endpoint is configured.
 *
 * <p>Accepts every batch immediately and logs the first event of each batch
 * at DEBUG level so that replay pipelines can be verified end-to-end without
 * a live downstream service.
 *
 * <p>Thread-safe: the atomic counter is safe for concurrent access from
 * multiple packet workers.
 */
public final class SimulatedDownstreamClient implements DownstreamClient {

    private static final Logger log = LoggerFactory.getLogger(SimulatedDownstreamClient.class);

    private final AtomicLong totalPosted = new AtomicLong(0);

    @Override
    public CompletableFuture<Integer> post(List<SecurityEvent> events) {
        if (!events.isEmpty()) {
            totalPosted.addAndGet(events.size());
            log.debug("Simulated downstream: accepted {} events (sample cid={})",
                    events.size(), events.get(0).cid());
        }
        return CompletableFuture.completedFuture(events.size());
    }

    @Override
    public void close() {
        log.info("SimulatedDownstreamClient closed — total events posted: {}", totalPosted.get());
    }

    /** Total events accepted across all {@link #post} calls. */
    public long totalPosted() { return totalPosted.get(); }
}
