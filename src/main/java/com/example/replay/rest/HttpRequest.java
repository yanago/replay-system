package com.example.replay.rest;

import java.util.Map;

/**
 * Immutable parsed representation of an inbound HTTP/1.1 request.
 *
 * <p>Header names are normalised to lower-case.
 * Path parameters (e.g. {@code {id}} from a route template) are injected
 * by the router before the handler is invoked.
 */
public record HttpRequest(
        String              method,
        String              path,
        String              version,
        Map<String, String> headers,
        String              body,
        Map<String, String> pathParams   // populated by router; empty for exact-match routes
) {
    /** Header value by lower-case name, or {@code null}. */
    public String header(String name) {
        return headers.get(name.toLowerCase());
    }

    /** Content-Type header value, or empty string. */
    public String contentType() {
        return headers.getOrDefault("content-type", "");
    }

    /** Path-parameter value by name, or empty string if not present. */
    public String pathParam(String name) {
        return pathParams.getOrDefault(name, "");
    }

    /** Returns a copy of this request with the given path parameters injected. */
    public HttpRequest withPathParams(Map<String, String> params) {
        return new HttpRequest(method, path, version, headers, body, Map.copyOf(params));
    }
}
