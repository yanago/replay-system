package com.example.replay.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

/**
 * Zero-dependency HTTP/1.1 server built on {@link ServerSocket}.
 *
 * <p>Each accepted connection is handled on a Java 21 virtual thread, so
 * thousands of concurrent slow clients never exhaust the carrier-thread pool.
 *
 * <p>Two route flavours:
 * <ul>
 *   <li><b>Exact</b>  – {@code .get("/health", h)}  → fastest, hash-lookup</li>
 *   <li><b>Template</b> – {@code .get("/jobs/{id}", h)} → path params injected
 *       into {@link HttpRequest#pathParams()}, tried in registration order</li>
 * </ul>
 *
 * <p>Usage:
 * <pre>
 *   var server = new MinimalHttpServer(8080)
 *       .get("/health",        req -> HttpResponse.ok("{}"))
 *       .get("/jobs/{id}",     req -> getJob(req.pathParam("id")))
 *       .post("/jobs",         req -> createJob(req));
 *   server.start();
 * </pre>
 */
public final class MinimalHttpServer {

    private static final Logger log = LoggerFactory.getLogger(MinimalHttpServer.class);

    private static final int READ_TIMEOUT_MS = 5_000;
    private static final int MAX_BODY_BYTES  = 1024 * 1024; // 1 MB

    // Exact routes: method → (literal-path → handler)
    private final Map<String, Map<String, Function<HttpRequest, HttpResponse>>> exactRoutes =
            new ConcurrentHashMap<>();

    // Parameterised routes: method → ordered list of (template, handler)
    private final Map<String, List<TemplateRoute>> templateRoutes =
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
        if (path.contains("{")) {
            templateRoutes
                    .computeIfAbsent(method, k -> new CopyOnWriteArrayList<>())
                    .add(new TemplateRoute(new PathTemplate(path), handler));
        } else {
            exactRoutes
                    .computeIfAbsent(method, k -> new ConcurrentHashMap<>())
                    .put(path, handler);
        }
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
        executor     = Executors.newVirtualThreadPerTaskExecutor();
        running      = true;
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

        var headers       = new LinkedHashMap<String, String>();
        int contentLength = 0;
        String line;
        while ((line = in.readLine()) != null && !line.isBlank()) {
            int colon = line.indexOf(':');
            if (colon > 0) {
                var key = line.substring(0, colon).trim().toLowerCase();
                var val = line.substring(colon + 1).trim();
                headers.put(key, val);
                if ("content-length".equals(key)) contentLength = parseContentLength(val);
            }
        }

        String body = null;
        if (contentLength > 0) {
            int toRead = Math.min(contentLength, MAX_BODY_BYTES);
            var buf    = new char[toRead];
            int read   = in.read(buf, 0, toRead);
            body       = read > 0 ? new String(buf, 0, read) : "";
        }

        // pathParams starts empty; router injects them before calling the handler
        return new HttpRequest(method, path, version, Map.copyOf(headers), body, Map.of());
    }

    private int parseContentLength(String value) {
        try { return Integer.parseInt(value); } catch (NumberFormatException e) { return 0; }
    }

    // -----------------------------------------------------------------------
    // Dispatch
    // -----------------------------------------------------------------------

    private HttpResponse dispatch(HttpRequest req) {
        // 1. Exact match
        var exact = exactRoutes.getOrDefault(req.method(), Map.of()).get(req.path());
        if (exact != null) return invoke(exact, req);

        // 2. Template match (tried in registration order)
        for (var route : templateRoutes.getOrDefault(req.method(), List.of())) {
            var params = route.template().match(req.path());
            if (params != null) return invoke(route.handler(), req.withPathParams(params));
        }

        // 3. Check if path exists under another method → 405, else → 404
        return pathExistsForAnyMethod(req.path())
                ? HttpResponse.methodNotAllowed()
                : HttpResponse.notFound(req.path());
    }

    private HttpResponse invoke(Function<HttpRequest, HttpResponse> handler, HttpRequest req) {
        try {
            return handler.apply(req);
        } catch (Exception e) {
            log.error("Handler error for {} {}: {}", req.method(), req.path(), e.getMessage(), e);
            return HttpResponse.internalError(e.getMessage());
        }
    }

    private boolean pathExistsForAnyMethod(String path) {
        return exactRoutes.values().stream().anyMatch(m -> m.containsKey(path))
                || templateRoutes.values().stream()
                        .flatMap(List::stream)
                        .anyMatch(r -> r.template().match(path) != null);
    }

    // -----------------------------------------------------------------------
    // Path template
    // -----------------------------------------------------------------------

    /**
     * Compiles a path pattern like {@code /api/v1/replay/jobs/{id}} into a
     * segment array and matches incoming paths, extracting named parameters.
     */
    private record PathTemplate(String[] segments) {

        PathTemplate(String pattern) {
            this(pattern.split("/", -1));
        }

        /**
         * Returns extracted path parameters if {@code requestPath} matches this
         * template, or {@code null} if it does not.
         */
        Map<String, String> match(String requestPath) {
            var reqSegs = requestPath.split("/", -1);
            if (reqSegs.length != segments.length) return null;

            var params = new HashMap<String, String>();
            for (int i = 0; i < segments.length; i++) {
                var seg = segments[i];
                if (seg.startsWith("{") && seg.endsWith("}")) {
                    params.put(seg.substring(1, seg.length() - 1), reqSegs[i]);
                } else if (!seg.equals(reqSegs[i])) {
                    return null;
                }
            }
            return params;
        }
    }

    private record TemplateRoute(
            PathTemplate template,
            Function<HttpRequest, HttpResponse> handler) {}
}
