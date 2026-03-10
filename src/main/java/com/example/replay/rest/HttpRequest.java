package com.example.replay.rest;

import java.util.Map;

/**
 * Immutable parsed representation of an inbound HTTP/1.1 request.
 * Header names are normalised to lower-case.
 */
public record HttpRequest(
        String              method,
        String              path,
        String              version,
        Map<String, String> headers,
        String              body        // null when no body
) {
    /** Convenience: header value by lower-case name, or null. */
    public String header(String name) {
        return headers.get(name.toLowerCase());
    }

    /** Content-Type header value, or empty string. */
    public String contentType() {
        return headers.getOrDefault("content-type", "");
    }
}
