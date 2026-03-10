package com.example.replay.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Application-wide Jackson {@link ObjectMapper} utilities.
 *
 * <p>The shared {@link #MAPPER} instance is configured once:
 * <ul>
 *   <li>ISO-8601 timestamps (not epoch millis)</li>
 *   <li>Unknown JSON properties are silently ignored</li>
 *   <li>{@code null} fields are omitted from serialised output</li>
 * </ul>
 *
 * <p>All methods wrap checked exceptions in the unchecked {@link JsonException}
 * so callers stay free of try/catch boilerplate.
 */
public final class JsonUtils {

    private JsonUtils() {}

    public static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    // -----------------------------------------------------------------------
    // Serialisation
    // -----------------------------------------------------------------------

    /** Serialises {@code obj} to a compact JSON string. */
    public static String toJson(Object obj) {
        try {
            return MAPPER.writeValueAsString(obj);
        } catch (IOException e) {
            throw new JsonException("Serialisation failed for " + obj.getClass().getSimpleName(), e);
        }
    }

    /** Serialises {@code obj} to UTF-8 bytes — ready for a Kafka {@code ProducerRecord} value. */
    public static byte[] toBytes(Object obj) {
        try {
            return MAPPER.writeValueAsBytes(obj);
        } catch (IOException e) {
            throw new JsonException("Byte serialisation failed for " + obj.getClass().getSimpleName(), e);
        }
    }

    /** Returns a human-readable, indented JSON string — useful for logging and diagnostics. */
    public static String prettyPrint(Object obj) {
        try {
            return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(obj);
        } catch (IOException e) {
            throw new JsonException("Pretty-print failed for " + obj.getClass().getSimpleName(), e);
        }
    }

    // -----------------------------------------------------------------------
    // Deserialisation — String
    // -----------------------------------------------------------------------

    /** Deserialises a JSON string into {@code type}. */
    public static <T> T fromJson(String json, Class<T> type) {
        try {
            return MAPPER.readValue(json, type);
        } catch (IOException e) {
            throw new JsonException("Deserialisation failed into " + type.getSimpleName(), e);
        }
    }

    /**
     * Deserialises a JSON string into a generic type.
     * <pre>
     *   List&lt;SecurityEvent&gt; events =
     *       JsonUtils.fromJson(json, new TypeReference&lt;&gt;() {});
     * </pre>
     */
    public static <T> T fromJson(String json, TypeReference<T> typeRef) {
        try {
            return MAPPER.readValue(json, typeRef);
        } catch (IOException e) {
            throw new JsonException("Deserialisation failed for type-ref", e);
        }
    }

    // -----------------------------------------------------------------------
    // Deserialisation — byte[]
    // -----------------------------------------------------------------------

    /**
     * Deserialises a UTF-8 byte array into {@code type}.
     * Suitable for consuming raw Kafka record values.
     */
    public static <T> T fromJson(byte[] bytes, Class<T> type) {
        try {
            return MAPPER.readValue(bytes, type);
        } catch (IOException e) {
            throw new JsonException("Byte deserialisation failed into " + type.getSimpleName(), e);
        }
    }

    /** Deserialises a UTF-8 byte array into a generic type. */
    public static <T> T fromJson(byte[] bytes, TypeReference<T> typeRef) {
        try {
            return MAPPER.readValue(bytes, typeRef);
        } catch (IOException e) {
            throw new JsonException("Byte deserialisation failed for type-ref", e);
        }
    }

    // -----------------------------------------------------------------------
    // Deserialisation — InputStream
    // -----------------------------------------------------------------------

    /**
     * Deserialises a JSON {@link InputStream} into {@code type}.
     * Suitable for reading from files, HTTP response bodies, or S3 objects.
     */
    public static <T> T fromJson(InputStream stream, Class<T> type) {
        try {
            return MAPPER.readValue(stream, type);
        } catch (IOException e) {
            throw new JsonException("Stream deserialisation failed into " + type.getSimpleName(), e);
        }
    }

    // -----------------------------------------------------------------------
    // Dynamic access
    // -----------------------------------------------------------------------

    /**
     * Parses a JSON string into a {@link JsonNode} tree for dynamic field access
     * when the target type is unknown at compile time.
     */
    public static JsonNode toJsonNode(String json) {
        try {
            return MAPPER.readTree(json);
        } catch (IOException e) {
            throw new JsonException("JSON parse failed", e);
        }
    }

    /**
     * Converts any object to a {@link JsonNode} (e.g., to inspect or patch fields
     * before re-serialising).
     */
    public static JsonNode toJsonNode(Object obj) {
        return MAPPER.valueToTree(obj);
    }

    // -----------------------------------------------------------------------
    // List helpers
    // -----------------------------------------------------------------------

    /** Convenience: deserialise a JSON array string into a typed {@link List}. */
    public static <T> List<T> fromJsonList(String json, Class<T> elementType) {
        try {
            var type = MAPPER.getTypeFactory()
                    .constructCollectionType(List.class, elementType);
            return MAPPER.readValue(json, type);
        } catch (IOException e) {
            throw new JsonException("List deserialisation failed for " + elementType.getSimpleName(), e);
        }
    }

    // -----------------------------------------------------------------------
    // Exception
    // -----------------------------------------------------------------------

    public static final class JsonException extends RuntimeException {
        public JsonException(String msg, Throwable cause) { super(msg, cause); }
    }
}
