package com.example.replay.metrics;

import java.time.Instant;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * Mutable, thread-safe metrics state for a single replay job.
 *
 * <p>All fields are written from multiple {@link PacketWorkerActor} threads
 * concurrently; they use {@link LongAdder} / {@link AtomicInteger} so that
 * updates are lock-free.
 *
 * <p>The EPS sliding window is a {@link ConcurrentLinkedDeque} of
 * {@code long[]{timestampMillis, eventCount}} pairs.  Entries older than
 * 60 seconds are pruned on each {@link MetricsRegistry#recordBatch} call.
 *
 * <p>Batch latencies (fetch + publish, in milliseconds) are kept in a bounded
 * deque capped at {@value #MAX_LATENCY_SAMPLES} samples; the oldest entry is
 * evicted when the cap is exceeded.  P99 is computed from this deque at
 * query time.
 */
final class JobMetricsState {

    static final int MAX_LATENCY_SAMPLES = 1_000;

    final String  jobId;
    final Instant startedAt = Instant.now();

    // ---- progress -------------------------------------------------------
    final AtomicInteger packetsTotal     = new AtomicInteger(0);
    final AtomicInteger packetsCompleted = new AtomicInteger(0);

    // ---- throughput / latency --------------------------------------------
    final LongAdder totalEvents     = new LongAdder();
    final LongAdder totalBatches    = new LongAdder();
    final LongAdder totalFetchMs    = new LongAdder();
    final LongAdder totalPublishMs  = new LongAdder();

    // ---- errors ----------------------------------------------------------
    final LongAdder readErrors    = new LongAdder();
    final LongAdder publishErrors = new LongAdder();

    // ---- sliding window for EPS  [{timestampMillis, eventCount}] ---------
    final ConcurrentLinkedDeque<long[]> epsWindow = new ConcurrentLinkedDeque<>();

    // ---- recent batch total-latency samples for P99 ---------------------
    final ConcurrentLinkedDeque<Long> recentBatchLatencies = new ConcurrentLinkedDeque<>();

    JobMetricsState(String jobId) {
        this.jobId = jobId;
    }
}
