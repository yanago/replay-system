package com.example.replay.downstream;

import com.example.replay.model.SecurityEvent;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Port for forwarding security events to a downstream REST API.
 *
 * <p>Implementations must be thread-safe: multiple {@link PacketWorkerActor}s
 * may call {@link #post} concurrently.
 */
public interface DownstreamClient {

    /**
     * POSTs a batch of events to the downstream endpoint.
     *
     * @param events non-empty batch to forward
     * @return future that completes with the number of events accepted (HTTP 2xx count)
     */
    CompletableFuture<Integer> post(List<SecurityEvent> events);

    /** Releases any resources (connections, thread pools). */
    void close();
}
