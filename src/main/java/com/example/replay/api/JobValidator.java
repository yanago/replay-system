package com.example.replay.api;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Stateless validator for {@link JobRequest}.
 * Returns a list of {@link ValidationError}s; an empty list means the request
 * is valid.  All rules are checked independently so callers receive the full
 * set of errors in one shot.
 */
public final class JobValidator {

    /** Kafka topic naming rules: alphanumeric, dot, hyphen, underscore; max 249 chars. */
    private static final Pattern TOPIC_PATTERN = Pattern.compile("[a-zA-Z0-9._-]{1,249}");

    /** Maximum replay window allowed in a single job. */
    private static final Duration MAX_RANGE = Duration.ofDays(366);

    /** Upper cap on speed multiplier — prevent accidental CPU-storm requests. */
    private static final double MAX_SPEED = 1_000.0;

    private JobValidator() {}

    /**
     * Validates {@code req} and returns every detected problem.
     *
     * @param req may be {@code null} (treated as fully missing body)
     * @return immutable list; empty if valid
     */
    public static List<ValidationError> validate(JobRequest req) {
        if (req == null) {
            return List.of(new ValidationError("request_body", "must not be null or empty"));
        }

        var errors = new ArrayList<ValidationError>();

        // ---- required string fields ----------------------------------------
        if (isBlank(req.sourceTable())) {
            errors.add(new ValidationError("source_table", "required, must not be blank"));
        }

        if (isBlank(req.targetTopic())) {
            errors.add(new ValidationError("target_topic", "required, must not be blank"));
        } else if (!TOPIC_PATTERN.matcher(req.targetTopic()).matches()) {
            errors.add(new ValidationError("target_topic",
                    "invalid Kafka topic name — allowed: [a-zA-Z0-9._-], max 249 chars"));
        }

        // ---- required time fields ------------------------------------------
        if (req.fromTime() == null) {
            errors.add(new ValidationError("from_time", "required (ISO-8601, e.g. 2024-01-01T00:00:00Z)"));
        }
        if (req.toTime() == null) {
            errors.add(new ValidationError("to_time", "required (ISO-8601, e.g. 2024-01-02T00:00:00Z)"));
        }

        // ---- cross-field time rules (only when both are present) -----------
        if (req.fromTime() != null && req.toTime() != null) {
            if (!req.fromTime().isBefore(req.toTime())) {
                errors.add(new ValidationError("from_time",
                        "must be strictly before to_time"));
            } else {
                var range = Duration.between(req.fromTime(), req.toTime());
                if (range.compareTo(MAX_RANGE) > 0) {
                    errors.add(new ValidationError("time_range",
                            "window exceeds maximum of 366 days (requested: %d days)"
                                    .formatted(range.toDays())));
                }
            }
        }

        // ---- optional numeric fields ---------------------------------------
        if (req.speedMultiplier() != null) {
            if (req.speedMultiplier() <= 0) {
                errors.add(new ValidationError("speed_multiplier", "must be > 0"));
            } else if (req.speedMultiplier() > MAX_SPEED) {
                errors.add(new ValidationError("speed_multiplier",
                        "must not exceed %.0f".formatted(MAX_SPEED)));
            }
        }

        return List.copyOf(errors);
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
