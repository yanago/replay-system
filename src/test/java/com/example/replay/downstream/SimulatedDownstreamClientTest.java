package com.example.replay.downstream;

import com.example.replay.model.SecurityEvent;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SimulatedDownstreamClientTest {

    private static SecurityEvent event(String id, String cid) {
        return new SecurityEvent(id, cid, Instant.now(), Instant.now().toEpochMilli(),
                "TEST", "1.2.3.4", "host", "LOW", Map.of());
    }

    @Test
    void post_returnsEventCount() throws Exception {
        var client = new SimulatedDownstreamClient();
        var events = List.of(event("e1", "c1"), event("e2", "c2"), event("e3", "c3"));

        var result = client.post(events).get();

        assertEquals(3, result);
        assertEquals(3L, client.totalPosted());
    }

    @Test
    void post_emptyBatch_returnsZero() throws Exception {
        var client = new SimulatedDownstreamClient();

        var result = client.post(List.of()).get();

        assertEquals(0, result);
        assertEquals(0L, client.totalPosted());
    }

    @Test
    void post_multipleCalls_accumulatesTotal() throws Exception {
        var client = new SimulatedDownstreamClient();

        client.post(List.of(event("e1", "c1"), event("e2", "c2"))).get();
        client.post(List.of(event("e3", "c3"))).get();

        assertEquals(3L, client.totalPosted());
    }
}
