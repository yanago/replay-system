package com.example.replay.api;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A single field-level validation failure returned in the {@code errors} array
 * of a 422 response body.
 */
public record ValidationError(
        @JsonProperty("field")   String field,
        @JsonProperty("message") String message
) {}
