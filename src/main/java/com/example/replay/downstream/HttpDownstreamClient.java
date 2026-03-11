package com.example.replay.downstream;

import com.example.replay.model.SecurityEvent;
import com.example.replay.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

/**
 * {@link DownstreamClient} that POSTs event batches as a JSON array to a
 * configurable HTTP endpoint.
 *
 * <h3>Wire format</h3>
 * <pre>
 *   POST {endpointUrl}
 *   Content-Type: application/json
 *
 *   [ { "event_id": "...", "cid": "...", ... }, ... ]
 * </pre>
 *
 * <p>A 2xx response is treated as success; any other status or network error
 * causes the returned future to complete exceptionally, which triggers the
 * caller's error-handling path (e.g., {@link PacketWorkerActor} → PacketFailed).
 *
 * <p>Virtual threads are used for the HTTP executor so that concurrent calls
 * from multiple packet workers do not exhaust a fixed thread pool.
 */
public final class HttpDownstreamClient implements DownstreamClient {

    private static final Logger log = LoggerFactory.getLogger(HttpDownstreamClient.class);

    private final URI        endpoint;
    private final HttpClient httpClient;

    /**
     * @param endpointUrl full URL of the downstream ingest endpoint,
     *                    e.g. {@code http://ingest-service:8090/api/events}
     */
    public HttpDownstreamClient(String endpointUrl) {
        this.endpoint   = URI.create(endpointUrl);
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .build();
    }

    @Override
    public CompletableFuture<Integer> post(List<SecurityEvent> events) {
        if (events.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }

        var body    = JsonUtils.toJson(events);
        var request = HttpRequest.newBuilder(endpoint)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .timeout(Duration.ofSeconds(30))
                .build();

        return httpClient
                .sendAsync(request, HttpResponse.BodyHandlers.discarding())
                .thenApply(response -> {
                    int status = response.statusCode();
                    if (status >= 200 && status < 300) {
                        log.debug("Downstream POST accepted {} events (status={})", events.size(), status);
                        return events.size();
                    }
                    throw new DownstreamException(
                            "Downstream rejected batch: HTTP " + status + " from " + endpoint);
                });
    }

    @Override
    public void close() {
        // java.net.http.HttpClient does not require explicit shutdown
    }

    /** Thrown when the downstream returns a non-2xx HTTP status. */
    public static final class DownstreamException extends RuntimeException {
        public DownstreamException(String msg) { super(msg); }
    }
}
