package com.example.replay.storage;

/**
 * Immutable value-object that carries all JDBC + pool settings.
 *
 * <p>Populated from environment variables:
 * <pre>
 *   POSTGRES_URL          jdbc:postgresql://host:5432/db  (required)
 *   POSTGRES_USER         replay                           (required)
 *   POSTGRES_PASSWORD     secret                           (optional, default "")
 *   POSTGRES_POOL_MAX     10                               (optional)
 *   POSTGRES_POOL_MIN     2                                (optional)
 *   POSTGRES_CONN_TIMEOUT_MS  30000                        (optional)
 * </pre>
 *
 * <p>Call {@link #isConfigured()} to test whether {@code POSTGRES_URL} is
 * present before calling {@link #fromEnv()}.
 */
public record DatabaseConfig(
        String url,
        String username,
        String password,
        int    maxPoolSize,
        int    minIdle,
        long   connectionTimeoutMs) {

    /** Returns {@code true} when {@code POSTGRES_URL} is set in the environment. */
    public static boolean isConfigured() {
        return System.getenv("POSTGRES_URL") != null;
    }

    /** Builds a config from environment variables. Throws if required vars are absent. */
    public static DatabaseConfig fromEnv() {
        return new DatabaseConfig(
                required("POSTGRES_URL"),
                required("POSTGRES_USER"),
                System.getenv().getOrDefault("POSTGRES_PASSWORD", ""),
                Integer.parseInt(System.getenv().getOrDefault("POSTGRES_POOL_MAX", "10")),
                Integer.parseInt(System.getenv().getOrDefault("POSTGRES_POOL_MIN", "2")),
                Long.parseLong(System.getenv().getOrDefault("POSTGRES_CONN_TIMEOUT_MS", "30000")));
    }

    private static String required(String name) {
        var val = System.getenv(name);
        if (val == null || val.isBlank())
            throw new IllegalStateException("Required environment variable not set: " + name);
        return val;
    }
}
