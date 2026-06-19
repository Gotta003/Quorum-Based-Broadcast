package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.Duration;

/*
 * · Description:
 * Client actor that issues read and write requests to the replicas and tracks
 * their completion. For every request a one-shot timeout is scheduled on the
 * Akka scheduler: if the matching reply arrives first the timer is cancelled,
 * otherwise the timeout fires and the relative callback is invoked.
 *
 * Requests are tracked per index. The framework itself identifies a request by
 * (client, replica, index), and the tests
 * never keep more than one in-flight read and one in-flight write per index, so
 * a per-index map is enough to correlate a reply with its pending timer.
 *
 * The replyy Id carried bReplyRead/ReplyWrite is used as the "fromReplica" of
 * the result (it may differ from the contacted replica, e.g. when the write is
 * answered by the coordinator), while the contacted replica ref is the one
 * reported in the timeout (it is the actor the client actually talked to).
 */
public class Client extends AbstractClient {

    // index -> pending read timer
    private final Map<Integer, Cancellable> pendingReads = new HashMap<>();
    // index -> pending write timer
    private final Map<Integer, Cancellable> pendingWrites = new HashMap<>();

    Client(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica, Optional<ActorRef> listener) {
        super(readTimeoutDelay, writeTimeoutDelay, listener, defaultTargetReplica);
    }

    public static Props props(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica) {
        return Props.create(Client.class, () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica, Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica, ActorRef listener) {
        return Props.create(Client.class, () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica, Optional.ofNullable(listener)));
    }

    private Cancellable scheduleOnce(long delayMillis, Object msg) {
        return getContext().getSystem().scheduler().scheduleOnce(
                Duration.create(delayMillis, TimeUnit.MILLISECONDS),
                getSelf(),
                msg,
                getContext().getSystem().dispatcher(),
                getSelf());
    }

    @Override
    public void sendRead(ActorRef replica, int index) {
        // A new request to the same index replaces the previous pending timer.
        Cancellable previous = pendingReads.remove(index);
        if (previous != null) {
            previous.cancel();
        }
        replica.tell(new ClientMessages.ClientRead(index), getSelf());
        Cancellable timer = scheduleOnce(getReadTimeoutDelay(),
                new AbstractClient.ReadTimeout(getSelf(), replica, index));
        pendingReads.put(index, timer);
        debug("READ sent to " + replica.path().name() + " (index=" + index + ")");
    }

    @Override
    public void sendWrite(ActorRef replica, int index, int value) {
        Cancellable previous = pendingWrites.remove(index);
        if (previous != null) {
            previous.cancel();
        }
        replica.tell(new ClientMessages.ClientWrite(index, value), getSelf());
        Cancellable timer = scheduleOnce(getWriteTimeoutDelay(),
                new AbstractClient.WriteTimeout(getSelf(), replica, index, value));
        pendingWrites.put(index, timer);
        debug("WRITE sent to " + replica.path().name() + " (index=" + index + ", value=" + value + ")");
    }

    private void onReplyRead(ClientMessages.ReplyRead msg) {
        Cancellable timer = pendingReads.remove(msg.index);
        if (timer == null) {
            // Late reply for an already completed (or timed-out) request: ignore.
            return;
        }
        timer.cancel();
        callbackOnReadResult(new AbstractClient.ReadResult(true, msg.index, msg.value, msg.replicaId));
    }

    private void onReplyWrite(ClientMessages.ReplyWrite msg) {
        Cancellable timer = pendingWrites.remove(msg.index);
        if (timer == null) {
            return;
        }
        timer.cancel();
        callbackOnWriteResult(new AbstractClient.WriteResult(msg.success, msg.index, msg.value, msg.replicaId));
    }

    private void onReadTimeout(AbstractClient.ReadTimeout msg) {
        // Only fire if the request is still pending (the reply may have won the race).
        if (pendingReads.remove(msg.index) == null) {
            return;
        }
        callbackOnReadTimeout(msg);
    }

    private void onWriteTimeout(AbstractClient.WriteTimeout msg) {
        if (pendingWrites.remove(msg.index) == null) {
            return;
        }
        callbackOnWriteTimeout(msg);
    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                .match(ClientMessages.ReplyRead.class, this::onReplyRead)
                .match(ClientMessages.ReplyWrite.class, this::onReplyWrite)
                .match(AbstractClient.ReadTimeout.class, this::onReadTimeout)
                .match(AbstractClient.WriteTimeout.class, this::onWriteTimeout)
                .build();
    }

}
