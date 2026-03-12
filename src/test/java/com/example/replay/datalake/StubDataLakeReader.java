package com.example.replay.datalake;

import com.example.replay.model.SecurityEvent;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Synchronous test double for {@link DataLakeReader}.
 *
 * <p>Returns {@code eventsPerBatch} synthetic events for batch indices
 * {@code [0, totalBatches)}, and an empty list for any subsequent index,
 * signalling end-of-data.
 *
 * <p>An optional {@code batchDelayMillis} can be configured so tests that
 * need the job to stay in RUNNING state long enough to issue a pause/cancel
 * can do so reliably without races.
 */
public final class StubDataLakeReader implements DataLakeReader {

    private final int  totalBatches;
    private final int  eventsPerBatch;
    private final long batchDelayMillis;

    /**
     * @param totalBatches   number of non-empty batches to return before EOF
     * @param eventsPerBatch events returned in each non-empty batch
     */
    public StubDataLakeReader(int totalBatches, int eventsPerBatch) {
        this(totalBatches, eventsPerBatch, 0L);
    }

    /**
     * @param totalBatches    number of non-empty batches to return before EOF
     * @param eventsPerBatch  events returned in each non-empty batch
     * @param batchDelayMillis milliseconds to sleep before returning each batch;
     *                         useful when tests need the job to remain RUNNING
     *                         long enough to issue a lifecycle command
     */
    public StubDataLakeReader(int totalBatches, int eventsPerBatch, long batchDelayMillis) {
        this.totalBatches    = totalBatches;
        this.eventsPerBatch  = eventsPerBatch;
        this.batchDelayMillis = batchDelayMillis;
    }

    @Override
    public List<SecurityEvent> readBatch(
            String tableLocation, Instant from, Instant to,
            int batchIndex, int batchSize) {

        if (batchDelayMillis > 0) {
            try { Thread.sleep(batchDelayMillis); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }

        if (batchIndex >= totalBatches) return List.of();

        var events = new ArrayList<SecurityEvent>(eventsPerBatch);
        for (int i = 0; i < eventsPerBatch; i++) {
            events.add(new SecurityEvent(
                    "evt-b%d-i%d".formatted(batchIndex, i),
                    "cust-%03d".formatted((batchIndex * eventsPerBatch + i) % 10 + 1),
                    Instant.now(),
                    from.plusSeconds(batchIndex * 60L + i).toEpochMilli(),
                    "TEST_EVENT",
                    "10.0.0." + (i % 255 + 1),
                    "host-" + i,
                    "LOW",
                    Map.of()));
        }
        return events;
    }
}
