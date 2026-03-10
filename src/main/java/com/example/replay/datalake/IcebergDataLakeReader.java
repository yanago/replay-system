package com.example.replay.datalake;

import com.example.replay.model.SecurityEvent;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Reads {@link SecurityEvent}s from an Apache Iceberg table using the
 * standalone {@code IcebergGenerics} API (no Spark dependency).
 *
 * <p>I/O is submitted to the supplied {@link Executor} so the Pekko dispatcher
 * thread never blocks.
 */
public final class IcebergDataLakeReader implements DataLakeReader {

    private static final Logger log = LoggerFactory.getLogger(IcebergDataLakeReader.class);

    private final Catalog  catalog;
    private final Executor ioExecutor;

    public IcebergDataLakeReader(Catalog catalog, Executor ioExecutor) {
        this.catalog    = catalog;
        this.ioExecutor = ioExecutor;
    }

    @Override
    public CompletableFuture<List<SecurityEvent>> readBatch(
            String table, Instant from, Instant to, long offset, int limit) {

        return CompletableFuture.supplyAsync(() -> {
            log.debug("Reading batch from table={} from={} to={} offset={} limit={}", table, from, to, offset, limit);
            var id      = TableIdentifier.parse(table);
            var iceTable = catalog.loadTable(id);
            return scanBatch(iceTable, from, to, offset, limit);
        }, ioExecutor);
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    private List<SecurityEvent> scanBatch(
            Table iceTable, Instant from, Instant to, long offset, int limit) {

        // Iceberg pushes the time filter down to the file-scan level.
        // Filter on event_time (when the event occurred), stored as epoch-millis LONG.
        var scan = IcebergGenerics.read(iceTable)
                .where(Expressions.and(
                        Expressions.greaterThanOrEqual("event_time", from.toEpochMilli()),
                        Expressions.lessThan("event_time", to.toEpochMilli())))
                .build();

        var events = new ArrayList<SecurityEvent>(limit);
        long skipped = 0L;
        for (Record rec : scan) {
            if (skipped++ < offset) continue;
            events.add(toEvent(rec));
            if (events.size() >= limit) break;
        }
        return events;
    }

    @SuppressWarnings("unchecked")
    private SecurityEvent toEvent(Record rec) {
        return new SecurityEvent(
                str(rec, "event_id"),
                str(rec, "cid"),
                epochMillis(rec, "event_timestamp"),
                epochMillis(rec, "event_time"),
                str(rec, "event_type"),
                str(rec, "source_ip"),
                str(rec, "target_host"),
                str(rec, "severity"),
                (Map<String, String>) rec.getField("attributes")
        );
    }

    private static Instant epochMillis(Record rec, String field) {
        Object v = rec.getField(field);
        return v == null ? Instant.EPOCH : Instant.ofEpochMilli(((Number) v).longValue());
    }

    private static String str(Record rec, String field) {
        Object v = rec.getField(field);
        return v == null ? "" : v.toString();
    }
}
