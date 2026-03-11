package com.example.replay.actors;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Test double for {@link WorkPlannerFn}.
 *
 * <p>Returns a fixed number of {@link WorkPacket}s without touching the file
 * system.  Each packet covers an equal slice of the requested time window.
 * The {@code estimatedEvents} value is arbitrary — {@link StubDataLakeReader}
 * ignores it and always returns {@code eventsPerBatch} events per call.
 *
 * <p>Wired into tests via:
 * <pre>
 *   new StubWorkPlanner(1)   // 1 packet → total = StubDataLakeReader.totalBatches × eventsPerBatch
 * </pre>
 */
public final class StubWorkPlanner implements WorkPlannerFn {

    private final int numPackets;

    public StubWorkPlanner(int numPackets) {
        this.numPackets = numPackets;
    }

    @Override
    public List<WorkPacket> plan(String tableLocation, Instant from, Instant to) {
        long spanSecs  = to.getEpochSecond() - from.getEpochSecond();
        long sliceSecs = Math.max(1L, spanSecs / numPackets);

        var packets = new java.util.ArrayList<WorkPacket>(numPackets);
        for (int i = 0; i < numPackets; i++) {
            Instant sliceFrom = from.plusSeconds((long) i * sliceSecs);
            Instant sliceTo   = (i == numPackets - 1) ? to : sliceFrom.plusSeconds(sliceSecs);
            packets.add(new WorkPacket(
                    UUID.randomUUID().toString(),
                    tableLocation,
                    sliceFrom, sliceTo,
                    50L,        // estimatedEvents (arbitrary for testing)
                    1024L,      // weightBytes
                    0.0,        // skewScore
                    i % 2));    // suggestedWorker
        }
        return packets;
    }
}
