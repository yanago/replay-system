package com.example.replay.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

/**
 * Zero-dependency HTTP/1.1 server built on {@link ServerSocket}.
 *
 * <p>Each accepted connection is handled on a Java 21 virtual thread, so
 * thousands of concurrent slow clients never exhaust the carrier-thread pool.
 *
 * <p>Usage:
 * <pre>
 *   var server = new MinimalHttpServer(8080)
 *       .get("/health", req -> HttpResponse.ok("{\"status\":\"OK\"}"))
 *       .post("/replay", req -> handleReplay(req));
 *   server.start();
 *   // …
 *   server.stop();
 * </pre>
 */
public final class MinimalHttpServer {

    private static final Logger log = LoggerFactory.getLogger(MinimalHttpServer.class);

    private static final int READ_TIMEOUT_MS  = 5_000;
    private static final int MAX_BODY_BYTES   = 1024 * 1024; // 1 MB

    // method → (path → handler)
    private final Map<String, Map<String, Function<HttpRequest, HttpResponse>>> routes =
            new ConcurrentHashMap<>();

    private final int port;

    private volatile boolean        running = false;
    private          ServerSocket   serverSocket;
    private          ExecutorService executor;

    public MinimalHttpServer(int port) {
        this.port = port;
    }

    // -----------------------------------------------------------------------
    // Route registration (fluent API)
    // -----------------------------------------------------------------------

    public MinimalHttpServer get(String path, Function<HttpRequest, HttpResponse> handler) {
        return register("GET", path, handler);
    }

    public MinimalHttpServer post(String path, Function<HttpRequest, HttpResponse> handler) {
        return register("POST", path, handler);
    }

    public MinimalHttpServer delete(String path, Function<HttpRequest, HttpResponse> handler) {
        return register("DELETE", path, handler);
    }

    public MinimalHttpServer put(String path, Function<HttpRequest, HttpResponse> handler) {
        return register("PUT", path, handler);
    }

    private MinimalHttpServer register(String method, String path,
                                       Function<HttpRequest, HttpResponse> handler) {
        routes.computeIfAbsent(method, key -> new ConcurrentHashMap<>()).put(path, handler);
        return this;
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    /**
     * Binds the server socket and begins accepting connections.
     * Returns immediately; connections are handled on virtual threads.
     *
     * @throws IOException if the port cannot be bound
     */
    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        // Virtual-thread executor: each connection gets its own lightweight thread
        executor = Executors.newVirtualThreadPerTaskExecutor();
        running  = true;
        executor.submit(this::acceptLoop);
        log.info("MinimalHttpServer listening on port {}", serverSocket.getLocalPort());
    }

    /** Stops accepting new connections and awaits in-flight request completion. */
    public void stop() {
        running = false;
        try { serverSocket.close(); } catch (IOException ignored) {}
        executor.shutdown();
        log.info("MinimalHttpServer stopped");
    }

    /** Configured port (may be 0 for OS-assigned). */
    public int port() { return port; }

    /** Actual bound port — differs from {@link #port()} when 0 was requested. */
    public int boundPort() {
        return serverSocket != null ? serverSocket.getLocalPort() : port;
    }

    // -----------------------------------------------------------------------
    // Connection handling
    // -----------------------------------------------------------------------

    private void acceptLoop() {
        while (running) {
            try {
                var socket = serverSocket.accept();
                executor.submit(() -> handleConnection(socket));
            } catch (IOException e) {
                if (running) log.error("Accept error", e);
            }
        }
    }

    private void handleConnection(Socket socket) {
        try (socket) {
            socket.setSoTimeout(READ_TIMEOUT_MS);
            var in  = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            var out = socket.getOutputStream();

            var request  = parseRequest(in);
            var response = request == null
                    ? HttpResponse.badRequest("Malformed request line")
                    : dispatch(request);

            out.write(response.toBytes());
            out.flush();
        } catch (IOException e) {
            log.debug("Connection I/O error: {}", e.getMessage());
        }
    }

    // -----------------------------------------------------------------------
    // Request parsing
    // -----------------------------------------------------------------------

    private HttpRequest parseRequest(BufferedReader in) throws IOException {
        var requestLine = in.readLine();
        if (requestLine == null || requestLine.isBlank()) return null;

        var parts = requestLine.split(" ", 3);
        if (parts.length < 2) return null;

        var method  = parts[0].toUpperCase();
        var path    = parts[1];
        var version = parts.length > 2 ? parts[2] : "HTTP/1.1";

        // Parse headers (normalise keys to lower-case)
        var headers       = new LinkedHashMap<String, String>();
        int contentLength = 0;
        String line;
        while ((line = in.readLine()) != null && !line.isBlank()) {
            int colon = line.indexOf(':');
            if (colon > 0) {
                var key = line.substring(0, colon).trim().toLowerCase();
                var val = line.substring(colon + 1).trim();
                headers.put(key, val);
                if ("content-length".equals(key)) {
                    contentLength = parseContentLength(val);
                }
            }
        }

        // Read body if present (guard against oversized payloads)
        String body = null;
        if (contentLength > 0) {
            int toRead = Math.min(contentLength, MAX_BODY_BYTES);
            var buf    = new char[toRead];
            int read   = in.read(buf, 0, toRead);
            body       = read > 0 ? new String(buf, 0, read) : "";
        }

        return new HttpRequest(method, path, version,
                Map.copyOf(headers), body);
    }

    private int parseContentLength(String value) {
        try { return Integer.parseInt(value); } catch (NumberFormatException e) { return 0; }
    }

    // -----------------------------------------------------------------------
    // Dispatch
    // -----------------------------------------------------------------------

    private HttpResponse dispatch(HttpRequest req) {
        var methodRoutes = routes.get(req.method());
        if (methodRoutes == null) return HttpResponse.methodNotAllowed();

        var handler = methodRoutes.get(req.path());
        if (handler == null) return HttpResponse.notFound(req.path());

        try {
            return handler.apply(req);
        } catch (Exception e) {
            log.error("Handler error for {} {}: {}", req.method(), req.path(), e.getMessage(), e);
            return HttpResponse.internalError(e.getMessage());
        }
    }
}
