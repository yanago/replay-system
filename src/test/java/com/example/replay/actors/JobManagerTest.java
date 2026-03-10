package com.example.replay.actors;

import com.example.replay.actors.Messages.CoordinatorCommand;
import com.example.replay.actors.Messages.CoordinatorResponse;
import com.example.replay.model.ReplayJob;
import com.example.replay.model.ReplayStatus;
import com.example.replay.storage.InMemoryJobRepository;
import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit;
import org.apache.pekko.actor.testkit.typed.javadsl.TestProbe;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class JobManagerTest {

    static final ActorTestKit testKit = ActorTestKit.create();

    @AfterAll
    static void teardown() { testKit.shutdownTestKit(); }

    // Fresh repo + manager per test to avoid cross-test state
    InMemoryJobRepository                                          repo;
    org.apache.pekko.actor.typed.ActorRef<CoordinatorCommand>     manager;
    TestProbe<CoordinatorResponse>                                 probe;

    @BeforeEach
    void setUp() {
        repo    = new InMemoryJobRepository();
        manager = testKit.spawn(JobManager.create(repo));
        probe   = testKit.createTestProbe();
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static ReplayJob job(String id) {
        return ReplayJob.create(id, "db.events", "replay-topic",
                Instant.parse("2024-01-01T00:00:00Z"),
                Instant.parse("2024-01-02T00:00:00Z"), 1.0);
    }

    private ReplayJob submit(String id) {
        manager.tell(new CoordinatorCommand.SubmitJob(job(id), probe.getRef()));
        var accepted = (CoordinatorResponse.JobAccepted) probe.receiveMessage();
        return accepted.job();
    }

    // -----------------------------------------------------------------------
    // SubmitJob
    // -----------------------------------------------------------------------

    @Test
    void submit_returnsJobAccepted_withRunningStatus() {
        manager.tell(new CoordinatorCommand.SubmitJob(job("j1"), probe.getRef()));

        var resp = probe.receiveMessage();
        assertInstanceOf(CoordinatorResponse.JobAccepted.class, resp);
        var accepted = (CoordinatorResponse.JobAccepted) resp;
        assertEquals("j1",               accepted.job().jobId());
        assertEquals(ReplayStatus.RUNNING, accepted.job().status());
    }

    @Test
    void submit_persistsJobInRepository() {
        submit("j2");

        var stored = repo.findById("j2");
        assertTrue(stored.isPresent());
        assertEquals(ReplayStatus.RUNNING, stored.get().status());
    }

    // -----------------------------------------------------------------------
    // GetJob / ListJobs
    // -----------------------------------------------------------------------

    @Test
    void getJob_unknownId_returnsJobNotFound() {
        manager.tell(new CoordinatorCommand.GetJob("no-such", probe.getRef()));

        assertInstanceOf(CoordinatorResponse.JobNotFound.class, probe.receiveMessage());
    }

    @Test
    void getJob_knownId_returnsSnapshot() {
        submit("j3");
        manager.tell(new CoordinatorCommand.GetJob("j3", probe.getRef()));

        var resp = (CoordinatorResponse.JobSnapshot) probe.receiveMessage();
        assertEquals("j3", resp.job().jobId());
    }

    @Test
    void listJobs_returnsAllSubmitted() {
        submit("la");
        submit("lb");

        manager.tell(new CoordinatorCommand.ListJobs(probe.getRef()));
        var list = (CoordinatorResponse.JobList) probe.receiveMessage();
        assertEquals(2, list.jobs().size());
    }

    // -----------------------------------------------------------------------
    // PauseJob
    // -----------------------------------------------------------------------

    @Test
    void pause_runningJob_returnsJobPaused() {
        submit("p1");

        manager.tell(new CoordinatorCommand.PauseJob("p1", probe.getRef()));
        var resp = probe.receiveMessage();
        assertInstanceOf(CoordinatorResponse.JobPaused.class, resp);
        assertEquals(ReplayStatus.PAUSED, ((CoordinatorResponse.JobPaused) resp).job().status());
    }

    @Test
    void pause_updatesStatusInRepository() {
        submit("p2");
        manager.tell(new CoordinatorCommand.PauseJob("p2", probe.getRef()));
        probe.receiveMessage();

        assertEquals(ReplayStatus.PAUSED, repo.findById("p2").get().status());
    }

    @Test
    void pause_unknownJob_returnsJobNotFound() {
        manager.tell(new CoordinatorCommand.PauseJob("ghost", probe.getRef()));
        assertInstanceOf(CoordinatorResponse.JobNotFound.class, probe.receiveMessage());
    }

    @Test
    void pause_alreadyPausedJob_returnsRejected() {
        submit("p3");
        manager.tell(new CoordinatorCommand.PauseJob("p3", probe.getRef()));
        probe.receiveMessage(); // first pause accepted

        manager.tell(new CoordinatorCommand.PauseJob("p3", probe.getRef()));
        assertInstanceOf(CoordinatorResponse.Rejected.class, probe.receiveMessage());
    }

    // -----------------------------------------------------------------------
    // ResumeJob
    // -----------------------------------------------------------------------

    @Test
    void resume_pausedJob_returnsJobResumed() {
        submit("r1");
        manager.tell(new CoordinatorCommand.PauseJob("r1", probe.getRef()));
        probe.receiveMessage();

        manager.tell(new CoordinatorCommand.ResumeJob("r1", probe.getRef()));
        var resp = probe.receiveMessage();
        assertInstanceOf(CoordinatorResponse.JobResumed.class, resp);
        assertEquals(ReplayStatus.RUNNING, ((CoordinatorResponse.JobResumed) resp).job().status());
    }

    @Test
    void resume_updatesStatusInRepository() {
        submit("r2");
        manager.tell(new CoordinatorCommand.PauseJob("r2", probe.getRef()));
        probe.receiveMessage();
        manager.tell(new CoordinatorCommand.ResumeJob("r2", probe.getRef()));
        probe.receiveMessage();

        assertEquals(ReplayStatus.RUNNING, repo.findById("r2").get().status());
    }

    @Test
    void resume_runningJob_returnsRejected() {
        submit("r3");  // already RUNNING
        manager.tell(new CoordinatorCommand.ResumeJob("r3", probe.getRef()));
        assertInstanceOf(CoordinatorResponse.Rejected.class, probe.receiveMessage());
    }

    // -----------------------------------------------------------------------
    // CancelJob
    // -----------------------------------------------------------------------

    @Test
    void cancel_runningJob_returnsSnapshot_withCancelledStatus() {
        submit("c1");
        manager.tell(new CoordinatorCommand.CancelJob("c1", probe.getRef()));

        var resp = (CoordinatorResponse.JobSnapshot) probe.receiveMessage();
        assertEquals(ReplayStatus.CANCELLED, resp.job().status());
    }

    @Test
    void cancel_pausedJob_succeeds() {
        submit("c2");
        manager.tell(new CoordinatorCommand.PauseJob("c2", probe.getRef()));
        probe.receiveMessage();

        manager.tell(new CoordinatorCommand.CancelJob("c2", probe.getRef()));
        var resp = (CoordinatorResponse.JobSnapshot) probe.receiveMessage();
        assertEquals(ReplayStatus.CANCELLED, resp.job().status());
    }

    @Test
    void cancel_unknownJob_returnsJobNotFound() {
        manager.tell(new CoordinatorCommand.CancelJob("nobody", probe.getRef()));
        assertInstanceOf(CoordinatorResponse.JobNotFound.class, probe.receiveMessage());
    }
}
