package com.example.replay.rest;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Lightweight HTTP/1.1 response serialised directly to bytes.
 * Use the static factory methods for common cases.
 */
public final class HttpResponse {

    private final int                 statusCode;
    private final String              statusText;
    private final Map<String, String> headers;
    private final String              body;

    private HttpResponse(int statusCode, String statusText,
                         Map<String, String> headers, String body) {
        this.statusCode = statusCode;
        this.statusText = statusText;
        this.headers    = headers;
        this.body       = body == null ? "" : body;
    }

    // -----------------------------------------------------------------------
    // Factories
    // -----------------------------------------------------------------------

    public static HttpResponse ok(String body) {
        return json(200, "OK", body);
    }

    public static HttpResponse created(String body) {
        return json(201, "Created", body);
    }

    public static HttpResponse notFound(String path) {
        return json(404, "Not Found",
                "{\"error\":\"Not found\",\"path\":\"" + path + "\"}");
    }

    public static HttpResponse methodNotAllowed() {
        return json(405, "Method Not Allowed",
                "{\"error\":\"Method not allowed\"}");
    }

    public static HttpResponse badRequest(String reason) {
        return json(400, "Bad Request",
                "{\"error\":\"" + escape(reason) + "\"}");
    }

    public static HttpResponse internalError(String reason) {
        return json(500, "Internal Server Error",
                "{\"error\":\"" + escape(reason) + "\"}");
    }

    /**
     * General-purpose factory for responses with a pre-built JSON body.
     * Use when neither {@link #ok}, {@link #created}, nor {@link #badRequest}
     * fit — e.g. 422 Unprocessable Entity with a structured errors array.
     */
    public static HttpResponse of(int statusCode, String statusText, String jsonBody) {
        return json(statusCode, statusText, jsonBody);
    }

    // -----------------------------------------------------------------------
    // Serialisation
    // -----------------------------------------------------------------------

    /** Serialises the response to a complete HTTP/1.1 wire-format byte array. */
    public byte[] toBytes() {
        var bodyBytes = body.getBytes(StandardCharsets.UTF_8);
        var allHeaders = new LinkedHashMap<>(headers);
        allHeaders.put("Content-Length", String.valueOf(bodyBytes.length));
        allHeaders.put("Connection", "close");

        var sb = new StringBuilder();
        sb.append("HTTP/1.1 ").append(statusCode).append(' ').append(statusText).append("\r\n");
        allHeaders.forEach((k, v) -> sb.append(k).append(": ").append(v).append("\r\n"));
        sb.append("\r\n");

        var headerBytes = sb.toString().getBytes(StandardCharsets.US_ASCII);
        var result = new byte[headerBytes.length + bodyBytes.length];
        System.arraycopy(headerBytes, 0, result, 0, headerBytes.length);
        System.arraycopy(bodyBytes,   0, result, headerBytes.length, bodyBytes.length);
        return result;
    }

    public int statusCode() { return statusCode; }
    public String body()    { return body; }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static HttpResponse json(int code, String text, String body) {
        var headers = new LinkedHashMap<String, String>();
        headers.put("Content-Type", "application/json; charset=utf-8");
        return new HttpResponse(code, text, headers, body);
    }

    /** Minimal JSON-string escaping to prevent injection in error messages. */
    private static String escape(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "\\r");
    }
}
