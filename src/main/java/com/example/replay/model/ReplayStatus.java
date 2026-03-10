package com.example.replay.model;

/** Lifecycle states of a {@link ReplayJob}. */
public enum ReplayStatus {
    PENDING,
    RUNNING,
    PAUSED,
    COMPLETED,
    FAILED,
    CANCELLED
}
