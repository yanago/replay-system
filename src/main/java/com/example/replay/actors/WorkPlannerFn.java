package com.example.replay.actors;

import java.time.Instant;
import java.util.List;

/**
 * Strategy for planning work packets before a replay job starts.
 *
 * <p>The default production implementation is {@link WorkPlanner#plan}, which
 * inspects Iceberg file metadata.  Tests inject a stub that returns a fixed
 * list of {@link WorkPacket}s without touching the file system.
 *
 * <p>Implementations are expected to be synchronous.  The caller
 * ({@link ReplayJobActor}) wraps the call in {@code CompletableFuture.supplyAsync}
 * and uses Pekko's {@code pipeToSelf} to avoid blocking the actor dispatcher.
 */
@FunctionalInterface
public interface WorkPlannerFn {

    /**
     * Analyses the data lake and returns an ordered list of work packets
     * for the given table location and time range.
     *
     * @param tableLocation path to the Iceberg table directory
     * @param from          inclusive lower bound
     * @param to            exclusive upper bound
     * @return ordered (largest-first) list of work packets; never {@code null}
     * @throws Exception on any I/O or planning error
     */
    List<WorkPacket> plan(String tableLocation, Instant from, Instant to) throws Exception;
}
