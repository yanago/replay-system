package com.example.replay.storage;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates a fully-initialised {@link HikariDataSource} and applies all pending
 * Flyway migrations before returning it.
 *
 * <p>Usage:
 * <pre>
 *   var ds = DataSourceFactory.create(DatabaseConfig.fromEnv());
 *   var repo = new PostgresReplayJobRepository(ds);
 * </pre>
 *
 * <p>Migrations live in {@code src/main/resources/db/migration} and follow the
 * naming convention {@code V{version}__{description}.sql}.
 */
public final class DataSourceFactory {

    private static final Logger log = LoggerFactory.getLogger(DataSourceFactory.class);

    private DataSourceFactory() {}

    /**
     * Creates a connection pool from {@code cfg} and runs any pending Flyway
     * migrations before returning.
     *
     * @param cfg pool + JDBC settings
     * @return a ready-to-use {@link HikariDataSource}
     */
    public static HikariDataSource create(DatabaseConfig cfg) {
        var ds = buildPool(cfg);
        migrate(ds, cfg);
        return ds;
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    private static HikariDataSource buildPool(DatabaseConfig cfg) {
        var hikari = new HikariConfig();
        hikari.setPoolName("replay-jobs-pool");
        hikari.setJdbcUrl(cfg.url());
        hikari.setUsername(cfg.username());
        hikari.setPassword(cfg.password());
        hikari.setMaximumPoolSize(cfg.maxPoolSize());
        hikari.setMinimumIdle(cfg.minIdle());
        hikari.setConnectionTimeout(cfg.connectionTimeoutMs());
        // Fast fail at startup rather than silently returning broken connections
        hikari.setInitializationFailTimeout(cfg.connectionTimeoutMs());

        log.info("Creating connection pool '{}' → {}", hikari.getPoolName(), cfg.url());
        return new HikariDataSource(hikari);
    }

    private static void migrate(HikariDataSource ds, DatabaseConfig cfg) {
        log.info("Running Flyway migrations on {}", cfg.url());
        var result = Flyway.configure()
                .dataSource(ds)
                .locations("classpath:db/migration")
                .validateOnMigrate(true)
                .load()
                .migrate();
        log.info("Flyway applied {} migration(s), schema now at version {}",
                result.migrationsExecuted, result.targetSchemaVersion);
    }
}
