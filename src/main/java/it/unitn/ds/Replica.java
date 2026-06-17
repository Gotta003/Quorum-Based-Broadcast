package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.List;
import java.util.ArrayList;

public class Replica extends AbstractReplica {
    //Variables
    private int epoch;
    private int seq;
    private int[] positions;
    private int currentCoordinatorId;
    private Map<Integer, ActorRef> systemGroup;
    private boolean crashed;
    private List<ProtocolMessages.UPDATE> history;
    private Map<Integer, Integer> ackCounts;
    private AbstractReplica.Crash crashConfig;
    private int crashCounter;
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

    /*Internal token triggered if the next neighbor in the ring failts to send an Election */
    public static final class ElectionAckTimeout implements Serializable {
        public final int expectedAckFromId;
        public ElectionAckTimeout(int expectedAckFromId) {
            this.expectedAckFromId=expectedAckFromId;
        }
    }

    /*Internal safety token triggered if new coordinator fails to send SYNCHRONIZATION in time */
    public static final class SyncTimeout implements Serializable {
        //Just to signal timing token
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
        this.crashConfig=how_to_crash;
        this.crashCounter=how_to_crash.after_n_messages_of_type;
        System.out.println("[Replica " + this.id + "] CRASH CONFIGURATION RECV: Type=" + how_to_crash.type + " Countdown=" + this.crashCounter);
        if(how_to_crash.type==Crash.Type.Now) {
            executeCrash();
        }
    }

    void executeCrash() {
        this.crashed=true;
        System.out.println("[Replica "+this.id + " ] CRASHED Changing behavior no more RECV");
        getContext().become(createBaseReceiveBuilder().build());
    }

    void verifyCrash(Crash.Type currentMessType) {
        if(this.crashConfig!=null && this.crashConfig.type==currentMessType) {
            this.crashCounter--;
            System.out.println("[Replica " + this.id + "] Crash countdown decremented for " + currentMessType + ". Remaining: " + this.crashCounter);
            if(this.crashCounter==0) {
                executeCrash();
            }
        }
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
        this.crashed=false;
        this.history=new ArrayList<>();
        this.ackCounts=new HashMap<>();
        this.crashConfig=null;
        this.crashCounter=0;
        System.out.println("Replica " + this.id + " started correctly. Current Coordinator ID: " + this.currentCoordinatorId);
    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                .match(AbstractReplica.InitSystem.class, this::initSystem)
                .match(ClientMessages.ClientRead.class, this::onClientRead)
                .match(ClientMessages.ClientWrite.class, this::onClientWrite)
                .match(ProtocolMessages.WRITE_FORWARD.class, this::onWriteForward)
                .match(ProtocolMessages.UPDATE.class, this::onUpdate)
                .match(ProtocolMessages.ACK.class, this::onAck)
                .match(ProtocolMessages.WRITE_OK.class, this::onWriteOk)
                //Add other messages here
                .build();
    }

    private void onClientRead(ClientMessages.ClientRead msg) {
        if(this.crashed) {
            return;
        }
        int localValue=this.positions[msg.index];
        System.out.println("[Replica " + this.id + "] RECV READ_REQUEST. Reply value: " + localValue +"\n");
        this.tell(new ClientMessages.ReplyRead(localValue, msg.index, this.id), getSender());
    }

    private void broadcast(Serializable msg) {
        if(this.crashed) {
            return;
        }
        for(ActorRef replicaRef : this.systemGroup.values()) {
            this.tell(msg, replicaRef);
        }
    }

    private void writeForwardToCoordinator(int index, int value, ActorRef clientRef) {
        ActorRef coordinatorRef=this.systemGroup.get(this.currentCoordinatorId);
        if(coordinatorRef!=null) {
            System.out.println("[Replica " + this.id + "] Forwarding write to Coordinator " + this.currentCoordinatorId);
            this.tell(new ProtocolMessages.WRITE_FORWARD(index, value, clientRef), coordinatorRef);
        }
    }

    private void coordinatorWritePipeline(int index, int value, ActorRef clientRef) {
        this.seq++;
        ProtocolMessages.UPDATE pendingUpdate=new ProtocolMessages.UPDATE(this.epoch, this.seq, index, value, clientRef);
        this.history.add(pendingUpdate);
        this.ackCounts.put(this.seq, 0);
        System.out.println("[Coordinator " + this.id + "] UPDATE for seq " + this.seq + " (val: " + value + ")");
        broadcast(new ProtocolMessages.UPDATE(this.epoch, this.seq, index, value, clientRef));
    }

    private void onClientWrite(ClientMessages.ClientWrite msg) {
        if(this.crashed) {
            return;
        }
        System.out.println("[Replica " + this.id + "] RECV WRITE_REQUEST");
        if(this.id!=this.currentCoordinatorId) {
            writeForwardToCoordinator(msg.index, msg.value, getSender());
            return;
        }
        coordinatorWritePipeline(msg.index, msg.value, getSender());
    }

    private void onWriteForward(ProtocolMessages.WRITE_FORWARD msg) {
        if(this.crashed) {
            return;
        }
        System.out.println("[Replica " + this.id + "] RECV WRITE_FORWARD");
        if(this.id==this.currentCoordinatorId) {
            coordinatorWritePipeline(msg.index, msg.value, msg.clientRef);
        }
    }

    private void onUpdate(ProtocolMessages.UPDATE msg) {
        verifyCrash(Crash.Type.Update);
        if(this.crashed) {
            return;
        }
        
        System.out.println("[Replica " + this.id + "] RECV UPDATE command");
        this.history.add(new ProtocolMessages.UPDATE(msg.epoch, msg.seq, msg.index, msg.value, msg.clientRef));
        ActorRef coordinatorRef=this.systemGroup.get(this.currentCoordinatorId);
        if(coordinatorRef!=null) {
            System.out.println("[Replica " + this.id + "] UPDATE seq " + msg.seq + ", sending ACK");
            this.tell(new ProtocolMessages.ACK(msg.epoch, msg.seq, this.id), coordinatorRef);
        }
    }

    private void onAck(ProtocolMessages.ACK msg) { 
        if(this.crashed || this.id!=this.currentCoordinatorId) {
            return;
        }
        System.out.println("[Replica " + this.id + "] RECV ACK");
        int currentCount=this.ackCounts.getOrDefault(msg.seq, 0)+1;
        this.ackCounts.put(msg.seq, currentCount);
        int quorumThres=(this.systemGroup.size()/2)+1;
        System.out.println("[Coordinator " + this.id + "] RECV ACK for seq " + msg.seq + ". Current: " + currentCount+"/"+quorumThres);
        if (currentCount==quorumThres) {
            System.out.println("[Coordinator " + this.id + "] Quorum reached for seq " + msg.seq + ". Start Broadcast WRITE_OK");
            broadcast(new ProtocolMessages.WRITE_OK(msg.epoch, msg.seq));
        }
    }

    private void onWriteOk(ProtocolMessages.WRITE_OK msg) {
        verifyCrash(Crash.Type.WriteOK);
        if(this.crashed) {
            return;
        }
        System.out.println("[Replica " + this.id + "] RECV WRITE_OK for epoch " + msg.epoch + " and seq " + msg.seq);
        ProtocolMessages.UPDATE matchingUpdate=null;
        for(ProtocolMessages.UPDATE u : this.history) {
            if(u.epoch==msg.epoch && u.seq==msg.seq) {
                matchingUpdate=u;
                break;
            }
        }
        if(matchingUpdate!=null) {
            this.positions[matchingUpdate.index]=matchingUpdate.value;
            System.out.println("[Replica " + this.id + "] UPDATE applied to positions [" + matchingUpdate.index + "]="+matchingUpdate.value);
            this.callbackOnUpdateApplied(matchingUpdate.index, matchingUpdate.value);
            if(matchingUpdate.clientRef!=null && matchingUpdate.clientRef!=getContext().getSystem().deadLetters()) {
                this.tell(new ClientMessages.ReplyWrite(true, matchingUpdate.index, matchingUpdate.value, this.id), matchingUpdate.clientRef);
            }
        }
        else {
            System.out.println("[Replica " + this.id + "] WARNING: No match update found for seq " + msg.seq);
        }
    }
}
