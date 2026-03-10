package com.example.replay.storage;

import com.example.replay.model.ReplayJob;
import com.example.replay.model.ReplayStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryJobRepositoryTest {

    InMemoryJobRepository repo;

    @BeforeEach
    void setUp() {
        repo = new InMemoryJobRepository();
    }

    private static ReplayJob job(String id) {
        return ReplayJob.create(id, "db.events", "topic",
                Instant.parse("2024-01-01T00:00:00Z"),
                Instant.parse("2024-01-02T00:00:00Z"),
                1.0);
    }

    @Test
    void save_andFindById_returnsJob() {
        var j = job("j1");
        repo.save(j);
        var found = repo.findById("j1");
        assertTrue(found.isPresent());
        assertEquals("j1", found.get().jobId());
    }

    @Test
    void findById_unknownId_returnsEmpty() {
        assertTrue(repo.findById("nope").isEmpty());
    }

    @Test
    void update_replacesExistingJob() {
        var j = job("j2");
        repo.save(j);
        var updated = j.withStatus(ReplayStatus.COMPLETED);
        repo.update(updated);
        assertEquals(ReplayStatus.COMPLETED, repo.findById("j2").get().status());
    }

    @Test
    void findAll_returnsAllJobs_newestFirst() throws InterruptedException {
        repo.save(job("older"));
        Thread.sleep(5);
        repo.save(job("newer"));

        var all = repo.findAll();
        assertEquals(2, all.size());
        assertEquals("newer", all.get(0).jobId());
        assertEquals("older", all.get(1).jobId());
    }

    @Test
    void findByStatus_filtersCorrectly() {
        repo.save(job("a").withStatus(ReplayStatus.RUNNING));
        repo.save(job("b").withStatus(ReplayStatus.COMPLETED));
        repo.save(job("c").withStatus(ReplayStatus.RUNNING));

        var running = repo.findByStatus(ReplayStatus.RUNNING);
        assertEquals(2, running.size());
        running.forEach(j -> assertEquals(ReplayStatus.RUNNING, j.status()));
    }

    @Test
    void size_reflectsNumberOfJobs() {
        assertEquals(0, repo.size());
        repo.save(job("x"));
        repo.save(job("y"));
        assertEquals(2, repo.size());
    }
}
