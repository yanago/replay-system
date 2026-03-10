package com.example.replay.actors;

import com.example.replay.model.SecurityEvent;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;

import java.util.List;

/**
 * Publishes event batches to the downstream Kafka topic and reports the
 * result back to the parent {@link ReplayJobActor}.
 *
 * <p><strong>Stub implementation</strong> — {@link #publish} acknowledges
 * each batch immediately. Replace it with a real
 * {@code KafkaEventPublisher.send(events, targetTopic)} call when integrating.
 *
 * <p>Protocol:
 * <ul>
 *   <li>On success → {@link Messages.ReplayJobCommand.BatchEmitted}</li>
 *   <li>On failure → {@link Messages.ReplayJobCommand.BatchFailed}</li>
 * </ul>
 */
public final class DataEmitterActor extends AbstractBehavior<Messages.DataEmitterCommand> {

    private final ActorRef<Messages.ReplayJobCommand> parent;

    // -----------------------------------------------------------------------
    // Factory
    // -----------------------------------------------------------------------

    public static Behavior<Messages.DataEmitterCommand> create(
            ActorRef<Messages.ReplayJobCommand> parent) {
        return Behaviors.setup(ctx -> new DataEmitterActor(ctx, parent));
    }

    private DataEmitterActor(ActorContext<Messages.DataEmitterCommand> ctx,
                              ActorRef<Messages.ReplayJobCommand> parent) {
        super(ctx);
        this.parent = parent;
    }

    // -----------------------------------------------------------------------
    // Behavior
    // -----------------------------------------------------------------------

    @Override
    public Receive<Messages.DataEmitterCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(Messages.DataEmitterCommand.Emit.class,   this::onEmit)
                .onMessage(Messages.DataEmitterCommand.Cancel.class,  msg -> Behaviors.stopped())
                .build();
    }

    // -----------------------------------------------------------------------
    // Handlers
    // -----------------------------------------------------------------------

    private Behavior<Messages.DataEmitterCommand> onEmit(Messages.DataEmitterCommand.Emit msg) {
        try {
            long published = publish(msg.events());
            getContext().getLog().debug("Emitted batch {} ({} events)", msg.seq(), published);
            parent.tell(new Messages.ReplayJobCommand.BatchEmitted(msg.seq(), published));
        } catch (Exception e) {
            getContext().getLog().error("Failed to emit batch {}: {}", msg.seq(), e.getMessage());
            parent.tell(new Messages.ReplayJobCommand.BatchFailed(msg.seq(), e.getMessage()));
        }
        return this;
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /**
     * Publishes events to Kafka. Returns the number published.
     * <p>Stub — replace with {@code KafkaEventPublisher.send(events, topic)}.
     */
    private long publish(List<SecurityEvent> events) {
        // Real implementation: return kafkaPublisher.send(events, job.targetTopic()).get();
        return events.size();
    }
}
