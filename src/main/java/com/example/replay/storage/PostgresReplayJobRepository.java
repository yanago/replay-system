package com.example.replay.storage;

import com.example.replay.model.ReplayJob;
import com.example.replay.model.ReplayStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * JDBC-based {@link ReplayJobRepository} backed by PostgreSQL.
 *
 * <p>Schema (auto-created if missing):
 * <pre>
 *   CREATE TABLE replay_jobs (
 *     job_id           TEXT PRIMARY KEY,
 *     source_table     TEXT        NOT NULL,
 *     target_topic     TEXT        NOT NULL,
 *     from_time        TIMESTAMPTZ NOT NULL,
 *     to_time          TIMESTAMPTZ NOT NULL,
 *     speed_multiplier DOUBLE PRECISION NOT NULL DEFAULT 1.0,
 *     status           TEXT        NOT NULL,
 *     created_at       TIMESTAMPTZ NOT NULL,
 *     updated_at       TIMESTAMPTZ NOT NULL,
 *     events_published BIGINT      NOT NULL DEFAULT 0,
 *     error_message    TEXT
 *   );
 * </pre>
 */
public final class PostgresReplayJobRepository implements ReplayJobRepository {

    private static final Logger log = LoggerFactory.getLogger(PostgresReplayJobRepository.class);

    private static final String INSERT = """
            INSERT INTO replay_jobs
              (job_id, source_table, target_topic, from_time, to_time,
               speed_multiplier, status, created_at, updated_at, events_published, error_message)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """;

    private static final String UPDATE = """
            UPDATE replay_jobs
            SET status=?, updated_at=?, events_published=?, error_message=?
            WHERE job_id=?
            """;

    private static final String SELECT_BY_ID = "SELECT * FROM replay_jobs WHERE job_id=?";
    private static final String SELECT_ALL   = "SELECT * FROM replay_jobs ORDER BY created_at DESC";
    private static final String SELECT_BY_STATUS = "SELECT * FROM replay_jobs WHERE status=? ORDER BY created_at DESC";

    private final DataSource ds;

    public PostgresReplayJobRepository(DataSource ds) {
        this.ds = ds;
        initSchema();
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    @Override
    public void save(ReplayJob job) {
        try (var conn = ds.getConnection();
             var ps   = conn.prepareStatement(INSERT)) {
            ps.setString(1, job.jobId());
            ps.setString(2, job.sourceTable());
            ps.setString(3, job.targetTopic());
            ps.setTimestamp(4, Timestamp.from(job.fromTime()));
            ps.setTimestamp(5, Timestamp.from(job.toTime()));
            ps.setDouble(6, job.speedMultiplier());
            ps.setString(7, job.status().name());
            ps.setTimestamp(8, Timestamp.from(job.createdAt()));
            ps.setTimestamp(9, Timestamp.from(job.updatedAt()));
            ps.setLong(10, job.eventsPublished());
            ps.setString(11, job.errorMessage());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new StorageException("Failed to save job " + job.jobId(), e);
        }
    }

    @Override
    public void update(ReplayJob job) {
        try (var conn = ds.getConnection();
             var ps   = conn.prepareStatement(UPDATE)) {
            ps.setString(1, job.status().name());
            ps.setTimestamp(2, Timestamp.from(job.updatedAt()));
            ps.setLong(3, job.eventsPublished());
            ps.setString(4, job.errorMessage());
            ps.setString(5, job.jobId());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new StorageException("Failed to update job " + job.jobId(), e);
        }
    }

    @Override
    public Optional<ReplayJob> findById(String jobId) {
        try (var conn = ds.getConnection();
             var ps   = conn.prepareStatement(SELECT_BY_ID)) {
            ps.setString(1, jobId);
            try (var rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(fromRow(rs)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new StorageException("Failed to find job " + jobId, e);
        }
    }

    @Override
    public List<ReplayJob> findAll() {
        return query(SELECT_ALL);
    }

    @Override
    public List<ReplayJob> findByStatus(ReplayStatus status) {
        try (var conn = ds.getConnection();
             var ps   = conn.prepareStatement(SELECT_BY_STATUS)) {
            ps.setString(1, status.name());
            try (var rs = ps.executeQuery()) {
                return collectRows(rs);
            }
        } catch (SQLException e) {
            throw new StorageException("Failed to list jobs by status " + status, e);
        }
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    private List<ReplayJob> query(String sql) {
        try (var conn = ds.getConnection();
             var ps   = conn.prepareStatement(sql);
             var rs   = ps.executeQuery()) {
            return collectRows(rs);
        } catch (SQLException e) {
            throw new StorageException("Query failed", e);
        }
    }

    private List<ReplayJob> collectRows(ResultSet rs) throws SQLException {
        var list = new ArrayList<ReplayJob>();
        while (rs.next()) list.add(fromRow(rs));
        return list;
    }

    private ReplayJob fromRow(ResultSet rs) throws SQLException {
        return new ReplayJob(
                rs.getString("job_id"),
                rs.getString("source_table"),
                rs.getString("target_topic"),
                toInstant(rs, "from_time"),
                toInstant(rs, "to_time"),
                rs.getDouble("speed_multiplier"),
                ReplayStatus.valueOf(rs.getString("status")),
                toInstant(rs, "created_at"),
                toInstant(rs, "updated_at"),
                rs.getLong("events_published"),
                rs.getString("error_message")
        );
    }

    private static Instant toInstant(ResultSet rs, String col) throws SQLException {
        var ts = rs.getTimestamp(col);
        return ts == null ? null : ts.toInstant();
    }

    private void initSchema() {
        var ddl = """
                CREATE TABLE IF NOT EXISTS replay_jobs (
                  job_id           TEXT PRIMARY KEY,
                  source_table     TEXT             NOT NULL,
                  target_topic     TEXT             NOT NULL,
                  from_time        TIMESTAMPTZ      NOT NULL,
                  to_time          TIMESTAMPTZ      NOT NULL,
                  speed_multiplier DOUBLE PRECISION NOT NULL DEFAULT 1.0,
                  status           TEXT             NOT NULL,
                  created_at       TIMESTAMPTZ      NOT NULL,
                  updated_at       TIMESTAMPTZ      NOT NULL,
                  events_published BIGINT           NOT NULL DEFAULT 0,
                  error_message    TEXT
                )
                """;
        try (var conn = ds.getConnection();
             var ps   = conn.prepareStatement(ddl)) {
            ps.execute();
            log.info("replay_jobs schema verified");
        } catch (SQLException e) {
            throw new StorageException("Schema initialisation failed", e);
        }
    }

    // Small unchecked wrapper so callers stay exception-free
    public static final class StorageException extends RuntimeException {
        public StorageException(String msg, Throwable cause) { super(msg, cause); }
    }
}
