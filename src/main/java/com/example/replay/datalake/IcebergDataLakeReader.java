package com.example.replay.datalake;

import com.example.replay.model.SecurityEvent;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Reads {@link SecurityEvent}s from an Apache Iceberg table stored on a
 * Hadoop-compatible filesystem (local FS, HDFS, S3A).
 *
 * <p>Uses {@link HadoopTables} — no Hive metastore or Iceberg REST catalog
 * required.  The {@code tableLocation} parameter is a filesystem path that
 * contains an Iceberg {@code metadata/} directory (created by
 * {@code DataGenerator} or any Iceberg writer).
 *
 * <p>Filtering is pushed down to the Iceberg scan (partition pruning + file
 * skipping).  Row-level offset pagination allows stateless batch resumption:
 * each {@code readBatch} call re-opens the scan, skips the first
 * {@code batchIndex * batchSize} matching rows, and returns at most
 * {@code batchSize} events.
 *
 * <p>This class is <em>synchronous</em> by design — callers (e.g.
 * {@link com.example.replay.actors.DataReaderActor}) wrap it in
 * {@code CompletableFuture.supplyAsync} and use Pekko's {@code pipeToSelf}.
 */
public final class IcebergDataLakeReader implements DataLakeReader {

    private static final Logger log = LoggerFactory.getLogger(IcebergDataLakeReader.class);

    private final Configuration conf;

    public IcebergDataLakeReader(Configuration conf) {
        this.conf = conf;
    }

    /** Convenience constructor using a default Hadoop configuration. */
    public IcebergDataLakeReader() {
        this(new Configuration());
    }

    // -----------------------------------------------------------------------
    // DataLakeReader
    // -----------------------------------------------------------------------

    @Override
    public List<SecurityEvent> readBatch(
            String tableLocation, Instant from, Instant to,
            int batchIndex, int batchSize) {

        log.debug("readBatch table={} from={} to={} batchIndex={} batchSize={}",
                tableLocation, from, to, batchIndex, batchSize);

        var tables   = new HadoopTables(conf);
        var table    = tables.load(tableLocation);

        // event_time is stored as epoch-microseconds (TimestampType.withoutZone)
        long fromMicros = from.toEpochMilli() * 1_000L;
        long toMicros   = to.toEpochMilli()   * 1_000L;

        var scan = IcebergGenerics.read(table)
                .where(Expressions.and(
                        Expressions.greaterThanOrEqual("event_time", fromMicros),
                        Expressions.lessThan("event_time", toMicros)))
                .build();

        long skip   = (long) batchIndex * batchSize;
        var  events = new ArrayList<SecurityEvent>(batchSize);
        long seen   = 0L;

        for (Record rec : scan) {
            if (seen++ < skip) continue;
            events.add(toEvent(rec));
            if (events.size() >= batchSize) break;
        }

        log.debug("readBatch returned {} events (batchIndex={})", events.size(), batchIndex);
        return events;
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    private SecurityEvent toEvent(Record rec) {
        return new SecurityEvent(
                str(rec, "event_id"),
                str(rec, "cid"),
                fromMicros(rec, "event_timestamp"),
                microsToMillis(rec, "event_time"),
                str(rec, "event_type"),
                str(rec, "source_ip"),
                str(rec, "target_host"),
                str(rec, "severity"),
                Map.of()
        );
    }

    private static Instant fromMicros(Record rec, String field) {
        Object v = rec.getField(field);
        if (v == null) return Instant.EPOCH;
        if (v instanceof LocalDateTime ldt) return ldt.toInstant(ZoneOffset.UTC);
        return Instant.ofEpochMilli(((Number) v).longValue() / 1_000L);
    }

    private static long microsToMillis(Record rec, String field) {
        Object v = rec.getField(field);
        if (v == null) return 0L;
        if (v instanceof LocalDateTime ldt) return ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
        return ((Number) v).longValue() / 1_000L;
    }

    private static String str(Record rec, String field) {
        Object v = rec.getField(field);
        return v == null ? "" : v.toString();
    }
}
