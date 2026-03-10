package com.example.replay.rest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class MinimalHttpServerTest {

    // Pick an ephemeral port to avoid clashes with other tests
    private static final int PORT = 18_080;

    private MinimalHttpServer server;
    private HttpClient        client;

    @BeforeEach
    void setUp() throws Exception {
        server = new MinimalHttpServer(PORT)
                .get("/health", req -> HttpResponse.ok("{\"status\":\"OK\"}"))
                .get("/echo",   req -> HttpResponse.ok("{\"path\":\"" + req.path() + "\"}"));

        server.start();
        client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(2))
                .build();
    }

    @AfterEach
    void tearDown() {
        server.stop();
    }

    // -----------------------------------------------------------------------
    // /health
    // -----------------------------------------------------------------------

    @Test
    void health_returns200() throws Exception {
        var response = get("/health");
        assertEquals(200, response.statusCode());
    }

    @Test
    void health_bodyContainsStatusOK() throws Exception {
        var response = get("/health");
        assertTrue(response.body().contains("\"status\":\"OK\""),
                "Body was: " + response.body());
    }

    @Test
    void health_contentTypeIsJson() throws Exception {
        var response = get("/health");
        var ct = response.headers().firstValue("content-type").orElse("");
        assertTrue(ct.startsWith("application/json"), "Content-Type was: " + ct);
    }

    // -----------------------------------------------------------------------
    // 404 / 405
    // -----------------------------------------------------------------------

    @Test
    void unknownPath_returns404() throws Exception {
        var response = get("/no-such-path");
        assertEquals(404, response.statusCode());
    }

    @Test
    void postToGetOnlyRoute_returns405() throws Exception {
        var response = post("/health", "{}");
        assertEquals(405, response.statusCode());
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private java.net.http.HttpResponse<String> get(String path) throws Exception {
        return client.send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + PORT + path))
                        .GET().build(),
                BodyHandlers.ofString());
    }

    private java.net.http.HttpResponse<String> post(String path, String body) throws Exception {
        return client.send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + PORT + path))
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .header("Content-Type", "application/json")
                        .build(),
                BodyHandlers.ofString());
    }
}
