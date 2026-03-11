package com.example.replay.datalake;

import com.example.replay.model.SecurityEvent;

import java.time.Instant;
import java.util.List;

/**
 * Port (hexagonal-architecture boundary) for reading security events from the
 * data lake.  Implementations may target Apache Iceberg, plain Parquet files,
 * or a test double.
 *
 * <p>Reads are <em>synchronous</em> and expected to be called from an off-actor
 * thread (e.g. via {@code pipeToSelf} with a virtual-thread executor).
 * Returning an empty list signals "no more data" for this time window.
 */
public interface DataLakeReader {

    /**
     * Synchronously reads the next batch of events from {@code tableLocation}
     * whose {@code event_time} falls within [{@code from}, {@code to}).
     *
     * @param tableLocation path to the Iceberg table directory (e.g. {@code /data/security_events})
     * @param from          inclusive lower bound
     * @param to            exclusive upper bound
     * @param batchIndex    zero-based batch index (file-level pagination cursor)
     * @param batchSize     maximum number of events per batch
     * @return ordered event list; empty signals end of data for this window
     */
    List<SecurityEvent> readBatch(
            String  tableLocation,
            Instant from,
            Instant to,
            int     batchIndex,
            int     batchSize);
}
