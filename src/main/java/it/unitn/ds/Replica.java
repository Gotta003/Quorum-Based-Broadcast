package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class Replica extends AbstractReplica {
    //Variables
    private int epoch;
    private int seq;
    private int[] positions;
    private int currentCoordinatorId;
    private Map<Integer, ActorRef> systemGroup;
    //Constructors
    public Replica(int id) {
        this(id, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL, Optional.empty());
    }

    public Replica(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, Optional<ActorRef> listener) {
        super(id, minLatency, maxLatency, coordinatorBeatInterval, listener);
        // TODO: implement
    }

    public static Props props(int id, int minLatency, int maxLatency, int coordinatorBeatInterval) {
        return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, ActorRef listener) {
        return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.ofNullable(listener)));
    }

    /*HEARTBEAT SIGNAL IMPLEMENTATION for control + timeout*/
    /*Network message sent periodically by coordinator to all cohort replicas*/
    public static final class Heartbeat implements Serializable {
        public final int coordinatorId;
        public Heartbeat(int coordinatorId) {
            this.coordinatorId=coordinatorId;
        }
    }

    /*Internal token as a local timer trigger. Replicas will schedule this message periodically via Scheduler to check if coordinator heartbeat has timed out*/
    public static final class HeartbeatTimeoutCheck implements Serializable {
        //Used only as signaling
    }

    /*Internal token scheduled by a replica after a WRITE_FORWARD. If UPDATE broadcast is not received within the timeout window, this message should trigger crash routine*/
    public static final class WriteForwardTimeout implements Serializable {
        public final int index;
        public final int value;
        public final ActorRef clientRef;
        public WriteForwardTimeout(int index, int value, ActorRef clientRef) {
            this.index=index;
            this.value=value;
            this.clientRef=clientRef;
        }
    }

    @Override
    public int getSystemNumberOfActors() {
        if (this.systemGroup!=null) {
            return this.systemGroup.size();
        }
        return 0;
    }

    @Override
    public void crash(AbstractReplica.Crash how_to_crash) {
        // TODO: implement
    }

    @Override
    public void initSystem(InitSystem sysInit) {
        this.systemGroup=new HashMap<>(sysInit.group);
        this.currentCoordinatorId=sysInit.coordinator_id;
        this.positions=new int[POSITIONS_LIST_LENGTH];
        for(int i=0; i<POSITIONS_LIST_LENGTH; i++) {
            this.positions[i]=0;
        }
        this.epoch=0;
        this.seq=0;
        System.out.println("Replica " + this.id + " started correctly. Current Coordinator ID: " + this.currentCoordinatorId);
    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                // TODO add your message handlers here .match(, )
                .build();
    }

}
