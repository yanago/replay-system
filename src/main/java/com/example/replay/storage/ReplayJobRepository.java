package com.example.replay.storage;

import com.example.replay.model.ReplayJob;
import com.example.replay.model.ReplayStatus;

import java.util.List;
import java.util.Optional;

/**
 * Port for durable replay-job persistence (PostgreSQL implementation ships
 * as the default adapter).
 */
public interface ReplayJobRepository {

    void save(ReplayJob job);

    void update(ReplayJob job);

    Optional<ReplayJob> findById(String jobId);

    List<ReplayJob> findAll();

    List<ReplayJob> findByStatus(ReplayStatus status);
}
