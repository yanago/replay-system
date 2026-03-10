package com.example.replay.storage;

import com.example.replay.model.ReplayJob;
import com.example.replay.model.ReplayStatus;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe, in-memory implementation of {@link ReplayJobRepository}.
 *
 * <p>Backed by a {@link ConcurrentHashMap} — safe for concurrent reads from
 * HTTP handler threads and writes from Pekko actor threads simultaneously.
 *
 * <p>Drop-in replacement path: swap this with {@link PostgresReplayJobRepository}
 * in {@code ReplayApplication} without touching any other class.
 */
public final class InMemoryJobRepository implements ReplayJobRepository {

    private final ConcurrentHashMap<String, ReplayJob> store = new ConcurrentHashMap<>();

    @Override
    public void save(ReplayJob job) {
        store.put(job.jobId(), job);
    }

    @Override
    public void update(ReplayJob job) {
        store.put(job.jobId(), job);
    }

    @Override
    public Optional<ReplayJob> findById(String jobId) {
        return Optional.ofNullable(store.get(jobId));
    }

    /** Returns all jobs ordered by {@code created_at} descending (newest first). */
    @Override
    public List<ReplayJob> findAll() {
        return store.values().stream()
                .sorted(Comparator.comparing(ReplayJob::createdAt).reversed())
                .toList();
    }

    @Override
    public List<ReplayJob> findByStatus(ReplayStatus status) {
        return store.values().stream()
                .filter(j -> j.status() == status)
                .sorted(Comparator.comparing(ReplayJob::createdAt).reversed())
                .toList();
    }

    /** Number of stored jobs — useful for tests. */
    public int size() {
        return store.size();
    }
}
