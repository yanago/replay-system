package com.example.replay.actors;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Analyses Apache Iceberg partition metadata and produces a balanced list of
 * {@link WorkPacket}s for a given table / time window.
 *
 * <h3>Algorithm</h3>
 * <ol>
 *   <li>Load the Iceberg table via {@link HadoopTables} (no catalog required).</li>
 *   <li>Call {@code table.newScan().planFiles()} with a time-range predicate to
 *       retrieve only the file tasks that overlap [{@code from}, {@code to}).
 *       Iceberg pushes the predicate down to partition pruning, so only relevant
 *       day-partitions are opened.</li>
 *   <li>Group tasks by partition day (integer: days since epoch stored in the
 *       {@code event_time_day} partition field at position 0).</li>
 *   <li>For each day-partition:
 *     <ul>
 *       <li>Sum {@code recordCount} and {@code fileSizeInBytes} across all files.</li>
 *       <li>Compute a <em>skewScore</em> from the max-to-average file-size ratio.
 *           A partition whose largest file dwarfs the average likely contains a
 *           disproportionate share of heavy-customer (Zipf-skewed) events.</li>
 *       <li>If the partition exceeds {@link #MAX_EVENTS_PER_PACKET}, split it into
 *           equal time slices to cap per-worker latency.</li>
 *     </ul>
 *   </li>
 *   <li>Sort all packets largest-first and apply <em>Longest-Remaining-Time (LRT)</em>
 *       bin-packing across {@code numWorkers} bins, assigning each packet to the
 *       currently least-loaded worker.  This minimises makespan and prevents heavy
 *       (high-skew) customers from stalling a single worker.</li>
 * </ol>
 *
 * <p>The caller wraps {@link #plan} in {@code CompletableFuture.supplyAsync} so
 * Iceberg I/O never blocks the Pekko dispatcher.
 */
public final class WorkPlanner {

    private static final Logger log = LoggerFactory.getLogger(WorkPlanner.class);

    /** Split a day-partition into multiple packets when it exceeds this threshold. */
    static final long MAX_EVENTS_PER_PACKET = 10_000L;

    private WorkPlanner() {}

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /**
     * Produces a work plan for the supplied table and time window.
     *
     * @param tableLocation  path to the Iceberg table directory
     * @param from           inclusive lower bound (epoch-millis mapped to micros)
     * @param to             exclusive upper bound
     * @param hadoopConf     Hadoop configuration for {@link HadoopTables}
     * @param numWorkers     number of workers available for LRT assignment
     * @return ordered list of work packets (largest-first within each worker bin)
     */
    public static List<WorkPacket> plan(
            String tableLocation, Instant from, Instant to,
            Configuration hadoopConf, int numWorkers) {

        var tables = new HadoopTables(hadoopConf);
        var table  = tables.load(tableLocation);

        // Iceberg stores TimestampType.withoutZone values as epoch-microseconds
        long fromMicros = from.toEpochMilli() * 1_000L;
        long toMicros   = to.toEpochMilli()   * 1_000L;

        // ------------------------------------------------------------------
        // 1. Collect FileScanTasks grouped by day-partition index
        // ------------------------------------------------------------------
        Map<Integer, List<FileScanTask>> byDay = new TreeMap<>();

        try (CloseableIterable<FileScanTask> tasks = table.newScan()
                .filter(Expressions.and(
                        Expressions.greaterThanOrEqual("event_time", fromMicros),
                        Expressions.lessThan("event_time", toMicros)))
                .planFiles()) {

            for (FileScanTask task : tasks) {
                // Position 0 = first partition field = event_time_day (days since epoch)
                int dayIndex = task.file().partition().get(0, Integer.class);
                byDay.computeIfAbsent(dayIndex, k -> new ArrayList<>()).add(task);
            }

        } catch (Exception e) {
            throw new RuntimeException("Failed to plan Iceberg scan for " + tableLocation, e);
        }

        if (byDay.isEmpty()) {
            log.warn("No data files found in {} for [{}, {})", tableLocation, from, to);
            return List.of();
        }

        // ------------------------------------------------------------------
        // 2. Convert partition groups → WorkPackets (splitting large ones)
        // ------------------------------------------------------------------
        var packets = new ArrayList<WorkPacket>();

        for (var entry : byDay.entrySet()) {
            int              dayIndex = entry.getKey();
            List<FileScanTask> tasks  = entry.getValue();

            long   totalRecords = tasks.stream().mapToLong(t -> t.file().recordCount()).sum();
            long   totalBytes   = tasks.stream().mapToLong(t -> t.file().fileSizeInBytes()).sum();
            double skewScore    = computeSkewScore(tasks);

            // Day boundaries
            Instant dayStart = Instant.ofEpochSecond((long) dayIndex * 86_400L);
            Instant dayEnd   = dayStart.plus(1, ChronoUnit.DAYS);

            // Clamp to [from, to]
            Instant pFrom = dayStart.isBefore(from) ? from : dayStart;
            Instant pTo   = dayEnd.isAfter(to)      ? to   : dayEnd;

            if (totalRecords > MAX_EVENTS_PER_PACKET) {
                // Split into N time-equal slices so no single worker is overloaded
                int  n            = (int) Math.ceil((double) totalRecords / MAX_EVENTS_PER_PACKET);
                long spanSecs     = pTo.getEpochSecond() - pFrom.getEpochSecond();
                long sliceSecs    = Math.max(1L, spanSecs / n);

                for (int i = 0; i < n; i++) {
                    Instant sliceFrom = pFrom.plusSeconds((long) i * sliceSecs);
                    Instant sliceTo   = (i == n - 1) ? pTo : sliceFrom.plusSeconds(sliceSecs);
                    long    sliceRecs = totalRecords / n + (i < totalRecords % n ? 1L : 0L);

                    packets.add(new WorkPacket(
                            UUID.randomUUID().toString(),
                            tableLocation,
                            sliceFrom, sliceTo,
                            sliceRecs,
                            totalBytes / n,
                            skewScore,
                            0));  // worker assigned below
                }
            } else {
                packets.add(new WorkPacket(
                        UUID.randomUUID().toString(),
                        tableLocation,
                        pFrom, pTo,
                        totalRecords,
                        totalBytes,
                        skewScore,
                        0));
            }
        }

        // ------------------------------------------------------------------
        // 3. LRT worker assignment
        // ------------------------------------------------------------------
        var result = lrtAssign(packets, numWorkers);

        log.info("WorkPlanner: {} packets from {} partitions for [{}, {}), {} workers",
                result.size(), byDay.size(), from, to, numWorkers);
        if (log.isDebugEnabled()) {
            result.forEach(p -> log.debug("  {}", p));
        }

        return result;
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /**
     * Longest-Remaining-Time bin-packing.
     *
     * <p>Packets are sorted by {@code estimatedEvents} descending, then each is
     * assigned to the worker whose current cumulative load is the smallest.
     * This heuristic minimises the expected makespan and naturally distributes
     * high-skew (heavy-customer) packets across all available workers.
     */
    private static List<WorkPacket> lrtAssign(List<WorkPacket> packets, int numWorkers) {
        // Sort largest-first for the LRT heuristic
        var sorted = new ArrayList<>(packets);
        sorted.sort(Comparator.comparingLong(WorkPacket::estimatedEvents).reversed());

        long[] workerLoad = new long[numWorkers];
        var result = new ArrayList<WorkPacket>(sorted.size());

        for (WorkPacket p : sorted) {
            // Pick the least-loaded worker
            int best = 0;
            for (int w = 1; w < numWorkers; w++) {
                if (workerLoad[w] < workerLoad[best]) best = w;
            }
            workerLoad[best] += p.estimatedEvents();

            result.add(new WorkPacket(
                    p.packetId(), p.tableLocation(),
                    p.from(), p.to(),
                    p.estimatedEvents(), p.weightBytes(),
                    p.skewScore(),
                    best));
        }

        return result;
    }

    /**
     * Skew score based on file-size variance within a partition.
     *
     * <p>A partition where one file is much larger than the average likely contains
     * a disproportionate share of events from a few heavy customers (Zipf effect).
     * Score is clamped to [0.0, 1.0].
     */
    private static double computeSkewScore(List<FileScanTask> tasks) {
        if (tasks.size() <= 1) return 0.0;
        long total = tasks.stream().mapToLong(t -> t.file().fileSizeInBytes()).sum();
        long avg   = total / tasks.size();
        long max   = tasks.stream().mapToLong(t -> t.file().fileSizeInBytes()).max().orElse(0L);
        return avg == 0L ? 0.0 : Math.min(1.0, (double) (max - avg) / avg);
    }
}
