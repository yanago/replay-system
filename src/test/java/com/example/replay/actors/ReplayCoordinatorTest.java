package com.example.replay.actors;

import com.example.replay.actors.Messages.CoordinatorCommand;
import com.example.replay.actors.Messages.CoordinatorResponse;
import com.example.replay.model.ReplayJob;
import com.example.replay.model.ReplayStatus;
import com.example.replay.storage.InMemoryJobRepository;
import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class ReplayCoordinatorTest {

    static final ActorTestKit testKit = ActorTestKit.create();

    @AfterAll
    static void teardown() { testKit.shutdownTestKit(); }

    @Test
    void submitJob_returnsJobAccepted() {
        var coordinator = testKit.spawn(ReplayCoordinator.create(new InMemoryJobRepository()), "coordinator-test");
        var probe       = testKit.<CoordinatorResponse>createTestProbe();

        var job = ReplayJob.create("job-1", "events.security", "replay-topic",
                Instant.parse("2024-01-01T00:00:00Z"),
                Instant.parse("2024-01-02T00:00:00Z"),
                1.0);

        coordinator.tell(new CoordinatorCommand.SubmitJob(job, probe.getRef()));

        var response = probe.receiveMessage();
        assertInstanceOf(CoordinatorResponse.JobAccepted.class, response);
        var accepted = (CoordinatorResponse.JobAccepted) response;
        assertEquals("job-1", accepted.job().jobId());
        assertEquals(ReplayStatus.RUNNING, accepted.job().status());
    }

    @Test
    void getJob_notFoundForUnknownId() {
        var coordinator = testKit.spawn(ReplayCoordinator.create(new InMemoryJobRepository()), "coordinator-nf");
        var probe       = testKit.<CoordinatorResponse>createTestProbe();

        coordinator.tell(new CoordinatorCommand.GetJob("no-such-job", probe.getRef()));

        var response = probe.receiveMessage();
        assertInstanceOf(CoordinatorResponse.JobNotFound.class, response);
    }

    @Test
    void listJobs_returnsAllSubmittedJobs() {
        var coordinator = testKit.spawn(ReplayCoordinator.create(new InMemoryJobRepository()), "coordinator-list");
        var probe       = testKit.<CoordinatorResponse>createTestProbe();

        var job = ReplayJob.create("job-list-1", "tbl", "topic",
                Instant.now().minusSeconds(3600), Instant.now(), 2.0);
        coordinator.tell(new CoordinatorCommand.SubmitJob(job, probe.getRef()));
        probe.receiveMessage(); // consume accepted

        coordinator.tell(new CoordinatorCommand.ListJobs(probe.getRef()));
        var list = (CoordinatorResponse.JobList) probe.receiveMessage();
        assertEquals(1, list.jobs().size());
    }
}
