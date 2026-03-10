package com.example.replay.storage;

import com.example.replay.model.ReplayJob;
import com.example.replay.model.ReplayStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for {@link PostgresReplayJobRepository}.
 *
 * <p>A real PostgreSQL container (postgres:16-alpine) is started once per
 * test class via Testcontainers. Flyway migrations are applied by
 * {@link DataSourceFactory#create} during {@code @BeforeAll} setup,
 * so each test works against the same schema that production uses.
 *
 * <p>Named {@code *IT.java} so Maven Failsafe picks it up as an
 * integration test (runs during {@code mvn verify}, not {@code mvn test}).
 */
@Testcontainers
class PostgresReplayJobRepositoryIT {

    @Container
    static final PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16-alpine");

    // One repo instance shared across all tests; table is truncated in @BeforeEach
    static PostgresReplayJobRepository repo;

    static {
        // Container is started by @Container before the static block runs,
        // so getJdbcUrl() is safe here.
        postgres.start();
        var cfg = new DatabaseConfig(
                postgres.getJdbcUrl(),
                postgres.getUsername(),
                postgres.getPassword(),
                5, 1, 10_000L);
        repo = new PostgresReplayJobRepository(DataSourceFactory.create(cfg));
    }

    @BeforeEach
    void truncate() throws Exception {
        try (var conn = postgres.createConnection("");
             var st   = conn.createStatement()) {
            st.execute("TRUNCATE TABLE replay_jobs");
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static ReplayJob job(String id) {
        return ReplayJob.create(id, "db.events", "test-topic",
                Instant.parse("2024-01-01T00:00:00Z"),
                Instant.parse("2024-01-02T00:00:00Z"),
                1.0);
    }

    // -----------------------------------------------------------------------
    // save / findById
    // -----------------------------------------------------------------------

    @Test
    void save_andFindById_roundTripsAllFields() {
        var j = job("j1");
        repo.save(j);

        var found = repo.findById("j1");
        assertTrue(found.isPresent());
        var r = found.get();
        assertEquals("j1",           r.jobId());
        assertEquals("db.events",    r.sourceTable());
        assertEquals("test-topic",   r.targetTopic());
        assertEquals(1.0,            r.speedMultiplier(), 1e-9);
        assertEquals(ReplayStatus.PENDING, r.status());
        assertEquals(0L,             r.eventsPublished());
        assertNull(r.errorMessage());
    }

    @Test
    void findById_unknownId_returnsEmpty() {
        assertTrue(repo.findById("nope").isEmpty());
    }

    // -----------------------------------------------------------------------
    // update
    // -----------------------------------------------------------------------

    @Test
    void update_changesStatusAndProgress() {
        repo.save(job("j2"));
        var updated = job("j2").withStatus(ReplayStatus.RUNNING).withProgress(42L);
        repo.update(updated);

        var r = repo.findById("j2").orElseThrow();
        assertEquals(ReplayStatus.RUNNING, r.status());
        assertEquals(42L, r.eventsPublished());
    }

    @Test
    void update_failed_storesErrorMessage() {
        repo.save(job("j3"));
        repo.update(job("j3").failed("disk full"));

        var r = repo.findById("j3").orElseThrow();
        assertEquals(ReplayStatus.FAILED, r.status());
        assertEquals("disk full", r.errorMessage());
    }

    // -----------------------------------------------------------------------
    // findAll
    // -----------------------------------------------------------------------

    @Test
    void findAll_returnsJobsNewestFirst() throws InterruptedException {
        repo.save(job("older"));
        Thread.sleep(10);   // ensure distinct created_at timestamps
        repo.save(job("newer"));

        var all = repo.findAll();
        assertEquals(2, all.size());
        assertEquals("newer", all.get(0).jobId());
        assertEquals("older", all.get(1).jobId());
    }

    @Test
    void findAll_emptyTable_returnsEmptyList() {
        assertTrue(repo.findAll().isEmpty());
    }

    // -----------------------------------------------------------------------
    // findByStatus
    // -----------------------------------------------------------------------

    @Test
    void findByStatus_filtersCorrectly() {
        repo.save(job("a").withStatus(ReplayStatus.RUNNING));
        repo.save(job("b").withStatus(ReplayStatus.COMPLETED));
        repo.save(job("c").withStatus(ReplayStatus.RUNNING));

        var running = repo.findByStatus(ReplayStatus.RUNNING);
        assertEquals(2, running.size());
        running.forEach(j -> assertEquals(ReplayStatus.RUNNING, j.status()));
    }

    @Test
    void findByStatus_noMatches_returnsEmptyList() {
        repo.save(job("x"));
        assertTrue(repo.findByStatus(ReplayStatus.FAILED).isEmpty());
    }

    // -----------------------------------------------------------------------
    // Flyway schema validation
    // -----------------------------------------------------------------------

    @Test
    void schema_hasExpectedIndexes() throws Exception {
        try (var conn = postgres.createConnection("");
             var rs   = conn.createStatement().executeQuery("""
                     SELECT indexname FROM pg_indexes
                     WHERE tablename = 'replay_jobs'
                     ORDER BY indexname
                     """)) {
            var names = new java.util.ArrayList<String>();
            while (rs.next()) names.add(rs.getString(1));
            assertTrue(names.contains("replay_jobs_pkey"),         names.toString());
            assertTrue(names.contains("idx_replay_jobs_status"),   names.toString());
            assertTrue(names.contains("idx_replay_jobs_created_at"), names.toString());
        }
    }
}
