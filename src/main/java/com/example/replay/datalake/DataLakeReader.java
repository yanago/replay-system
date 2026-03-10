package com.example.replay.datalake;

import com.example.replay.model.SecurityEvent;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Port (hexagonal-architecture boundary) for reading security events from the
 * data lake.  Implementations may target Apache Iceberg, plain Parquet files,
 * or a test double.
 */
public interface DataLakeReader {

    /**
     * Asynchronously reads the next batch of events from {@code table} whose
     * {@code occurred_at} falls within [{@code from}, {@code to}).
     *
     * @param table  fully-qualified Iceberg table name
     * @param from   inclusive lower bound
     * @param to     exclusive upper bound
     * @param offset event offset / page token to resume from
     * @param limit  maximum number of events to return
     * @return future resolving to (possibly empty) ordered event list
     */
    CompletableFuture<List<SecurityEvent>> readBatch(
            String  table,
            Instant from,
            Instant to,
            long    offset,
            int     limit);
}
