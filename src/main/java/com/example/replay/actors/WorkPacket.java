package com.example.replay.actors;

import java.time.Instant;

/**
 * An immutable descriptor for a unit of replay work.
 *
 * <p>A {@code WorkPacket} covers a contiguous time window within a single
 * Iceberg day-partition (or a sub-slice thereof when the partition is large).
 * It carries metadata derived from Iceberg file statistics so that the
 * {@link WorkerPoolActor} can make informed scheduling decisions without
 * re-reading data files.
 *
 * <h3>Fields</h3>
 * <ul>
 *   <li>{@code packetId}        — stable UUID for logging and deduplication</li>
 *   <li>{@code tableLocation}   — Iceberg table path passed to {@link com.example.replay.datalake.DataLakeReader}</li>
 *   <li>{@code from} / {@code to} — inclusive / exclusive time bounds for this slice</li>
 *   <li>{@code estimatedEvents} — row-count from Iceberg metadata (exact for non-split partitions)</li>
 *   <li>{@code weightBytes}     — total file size; used for load-balancing priority</li>
 *   <li>{@code skewScore}       — 0.0 = uniform event distribution across customers;
 *                                 1.0 = maximally skewed (one file much larger than average),
 *                                 indicating heavy-customer concentration that may slow emitters</li>
 *   <li>{@code suggestedWorker} — 0-based worker index assigned by
 *                                 {@link WorkPlanner} LRT scheduling; the pool may override it</li>
 * </ul>
 */
public record WorkPacket(
        String  packetId,
        String  tableLocation,
        Instant from,
        Instant to,
        long    estimatedEvents,
        long    weightBytes,
        double  skewScore,
        int     suggestedWorker) {

    @Override
    public String toString() {
        return "WorkPacket[%s %s→%s events≈%d %dKB skew=%.2f w=%d]".formatted(
                packetId.substring(0, 8), from, to,
                estimatedEvents, weightBytes / 1024, skewScore, suggestedWorker);
    }
}
