package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.Duration;

/**
 * Replica node implementation that implements a replication protocol with multi-agent handling, deterministic fault
 * injection and crash-recovery routine 
 * The class is divided in the following categories:
 * 1) Variables
 * 2) Constructor & Props
 * 3) Internal Messages (HEARTBEAT class) & Tokens
 * 4) Initialization & Status
 * 5) Client Module
 * 6) Normal Write Module
 * 7) Fault Module
 * 8) Heartbeat Implementation & Leader Election
 */
public class Replica extends AbstractReplica {
    //
    //  VARIABLES
    //
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
    // --- Heartbeat & Leader Election (Davide) ---
    private Cancellable heartbeatSender;
    private Cancellable heartbeatMonitor;
    private boolean electing;
    private boolean electionStartedFired;
    private Set<Integer> suspectedCrashed;
    private int electionAckExpectedFrom;
    private Cancellable electionAckTimer;
    private Map<Integer, ProtocolMessages.UPDATE> pendingElectionToken;
    private Cancellable electionGlobalTimer;
    private WriteForwardTimeout writeAfterElection=null;
    private final Map<Integer, ActorRef> locallyContactedWrites=new HashMap<>();

    //
    //  CONSTRUCTORS & PROPS
    //
    public Replica(int id) {
        this(id, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL, Optional.empty());
    }

    public Replica(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, Optional<ActorRef> listener) {
        super(id, minLatency, maxLatency, coordinatorBeatInterval, listener);
        // The implementation is inherit the framework initialization in initSystem override
    }

    public static Props props(int id, int minLatency, int maxLatency, int coordinatorBeatInterval) {
        return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.empty()));
    }

    public static Props propsWithListener(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, ActorRef listener) {
        return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.ofNullable(listener)));
    }

    //
    //  INTERNAL MESSAGES (HEARTBEAT CLASS) AND TOKENS
    //
    /**
     * Network Message sent periodically by the coordinator to all other replicas to tell them that it is alive
     */
    public static final class Heartbeat implements Serializable {
        public final int coordinatorId;
        public Heartbeat(int coordinatorId) {
            this.coordinatorId=coordinatorId;
        }
    }

    /**
     * Internal token acting as a local trigger. Replicas will schedule this message periodically via scheduler
     * to check if coordinator heartbeat has timed out
    */
    public static final class HeartbeatTimeoutCheck implements Serializable {
        //Used only as signaling
    }

    /**
     * Internal self-tick scheduled on the coordinator to drive the periodic broadcast of Heartbeat
     */
    public static final class HeartbeatTick implements Serializable {
        //Used only as signaling
    }

    /**
     * Internal token scheduled by a replica right after sending a WRITE_FORWARD message. If the UPDATE broadcast
     * is not received within the maximum latency window plus tolerance, this message triggers suspicion and then
     * the election algorithm
     */
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

    /**
     * Internal safety token triggered if the next alive neighbor inside Ring structure fails to respond to an ongoing
     * ELECTION sequence
     */
    public static final class ElectionAckTimeout implements Serializable {
        public final int expectedAckFromId;
        public ElectionAckTimeout(int expectedAckFromId) {
            this.expectedAckFromId=expectedAckFromId;
        }
    }

    /**
     * Internal safety token triggered if the new elected coordinator fails to broadcast SYNCHRONIZATION block in time
     */
    public static final class SyncTimeout implements Serializable {
        //Just to signal timing token
    }

    //
    //  INITIALIZATION AND STATUS
    //
    @Override
    public int getSystemNumberOfActors() {
        if (this.systemGroup!=null) {
            return this.systemGroup.size();
        }
        return 0;
    }

    /**
     * Initialize state of the replica and set data arrays to 0
     */
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
        this.electing=false;
        this.electionStartedFired=false;
        this.suspectedCrashed=new HashSet<>();
        this.electionAckExpectedFrom=-1;
        this.pendingElectionToken=null;
        debug("INITIALIZED. Current Coordinator ID: " + this.currentCoordinatorId);
        // Bootstrap the heartbeat role: the coordinator beats, everyone else watches.
        if(this.id==this.currentCoordinatorId) {
            startHeartbeatSender();
        } else {
            armHeartbeatMonitor();
        }
    }

    /**
     * Configure channels associated to any class respect to the method handler
     */
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
                .match(WriteForwardTimeout.class, this::onWriteForwardTimeout)
                .match(Heartbeat.class, this::onHeartbeat)
                .match(HeartbeatTick.class, this::onHeartbeatTick)
                .match(HeartbeatTimeoutCheck.class, this::onHeartbeatTimeoutCheck)
                .match(ElectionMessages.ELECTION.class, this::onElection)
                .match(ElectionMessages.ElectionAck.class, this::onElectionAck)
                .match(ElectionAckTimeout.class, this::onElectionAckTimeout)
                .match(ElectionMessages.SYNCHRONIZATION.class, this::onSynchronization)
                .match(SyncTimeout.class, this::onSyncTimeout)
                .match(DemoMessages.DumpStateRequest.class, msg-> {
                    if(this.crashed) {
                        getSender().tell(new DemoMessages.ReplicaStateReply(this.id, true, false, -1, new int[0], 0), getSelf());
                    }
                    else {
                        getSender().tell(new DemoMessages.ReplicaStateReply(this.id, this.crashed, this.electing, this.currentCoordinatorId, this.positions.clone(), this.history.size()), getSelf());
                    }
                })
                .build();
    }

    //
    //  CLIENT MODULE
    //
    /**
     * Immediate response sending to client the value present in required index
     * @param msg ClientRead Message (Request of reading coming from a client)
     */
    private void onClientRead(ClientMessages.ClientRead msg) {
        if(this.crashed) {
            return;
        }
        int localValue=this.positions[msg.index];
        debug("RECV READ_REQUEST from Client " + msg.index + ". Reply value: " + localValue +"\n");
        this.tell(new ClientMessages.ReplyRead(localValue, msg.index, this.id), getSender());
    }

    /**
     * Handle write request of the client and starts the pipeline if it is the leader or writes forward if part
     * of the cohort
     * @param msg ClientWrite Message (Request of writing coming from a client)
     */
    private void onClientWrite(ClientMessages.ClientWrite msg) {
        if(this.crashed || this.electing) {
            return;
        }
        debug("RECV WRITE_REQUEST from Client " + msg.index);
        this.locallyContactedWrites.put(msg.index, getSender());
        if(this.id!=this.currentCoordinatorId) {
            writeForwardToCoordinator(msg.index, msg.value, getSender());
            return;
        }
        coordinatorWritePipeline(msg.index, msg.value, getSender());
    }

    /**
     * Helper to pack original sender and forward the request to coordinator
     * @param index Client index
     * @param value Value to forward
     * @param clientRef Reference to client Actor
     */
    private void writeForwardToCoordinator(int index, int value, ActorRef clientRef) {
        ActorRef coordinatorRef=this.systemGroup.get(this.currentCoordinatorId);
        if(coordinatorRef!=null) {
            debug("WRITE_FORWARD to Coordinator " + this.currentCoordinatorId);
            this.tell(new ProtocolMessages.WRITE_FORWARD(index, value, clientRef), coordinatorRef);
            long timeoutDelay=3*getMaxLatencyPlusTolerance();
            scheduleOnceSelf(timeoutDelay, new WriteForwardTimeout(index, value, clientRef));
        }
    }

    //
    //  MODULE WRITE PATH (NORMAL PATH)
    //
    /**
     * Leader assigns the sequence progressively, saves in the local log and performs a broadcast
     * @param index Client index
     * @param value Value to broadcast
     * @param clientRef Reference to client Actor
     */
    private void coordinatorWritePipeline(int index, int value, ActorRef clientRef) {
        this.seq++;
        ProtocolMessages.UPDATE pendingUpdate=new ProtocolMessages.UPDATE(this.epoch, this.seq, index, value, clientRef);
        this.history.add(pendingUpdate);
        this.ackCounts.put(this.seq, 1);
        debug("[COORDINATOR] BROADCAST UPDATE for seq " + this.seq + " (val: " + value + ")");
        broadcast(new ProtocolMessages.UPDATE(this.epoch, this.seq, index, value, clientRef), "UPDATE");
    }

    /**
     * Handler triggered if coordinator fails to broadcast UPDATE message within max latency 
     * window after FORWARD request
     * @param msg WRITE_FORWARD message (after a Write Request from a Client to a cohort replica is triggered)
     */
    private void onWriteForwardTimeout(WriteForwardTimeout msg) {
        if(this.crashed || this.electing) {
            return;
        }
        boolean updateReceived=false;
        for(ProtocolMessages.UPDATE u : this.history) {
            if(u.epoch==this.epoch && u.index==msg.index && u.value==msg.value) {
                updateReceived=true;
                break;
            }
        }
        if(!updateReceived) {
            debug("WRITE_FORWARD timeout triggered for index " + msg.index + ". Stop Coordinator " + this.currentCoordinatorId);
            this.writeAfterElection=msg;
            startElection(this.currentCoordinatorId);
        }
    }

    /**
     * Captures the requests forwarded to a cohort and executes the normal write pipeline
     * @param msg WRITE_FORWARD message (after a Write Request from a Client to a cohort replica is triggered)
     */
    private void onWriteForward(ProtocolMessages.WRITE_FORWARD msg) {
        if(this.crashed || this.electing) {
            return;
        }
        debug("[COORDINATOR] RECV WRITE_FORWARD from " + getSender().path().name());
        if(this.id==this.currentCoordinatorId) {
            coordinatorWritePipeline(msg.index, msg.value, msg.clientRef);
        }
    }

    /**
     * Cohort validates update, insert it in the history and sends and ACK to coordinator
     * @param msg UPDATE message (Notify an Update Request)
     */
    private void onUpdate(ProtocolMessages.UPDATE msg) {
        verifyCrash(Crash.Type.Update);
        if(this.crashed) {
            return;
        }
        
        debug("RECV UPDATE from " + getSender().path().name());
        this.history.add(new ProtocolMessages.UPDATE(msg.epoch, msg.seq, msg.index, msg.value, msg.clientRef));
        ActorRef coordinatorRef=this.systemGroup.get(this.currentCoordinatorId);
        if(coordinatorRef!=null) {
            debug("UPDATE seq " + msg.seq + ", sending ACK to Coordinator " + this.currentCoordinatorId);
            this.tell(new ProtocolMessages.ACK(msg.epoch, msg.seq, this.id), coordinatorRef);
            long timeoutDelay=2*getMaxLatencyPlusTolerance();
            scheduleOnceSelf(timeoutDelay, new SyncTimeout());
        }
    }

    /**
     * Collects votes on leader and reached minor majority executes COMMIT
     * @param msg ACK message (cohorts communicates to coordinator that is OK to COMMIT for itself)
     */
    private void onAck(ProtocolMessages.ACK msg) { 
        if(this.crashed || this.id!=this.currentCoordinatorId) {
            return;
        }
        if(!this.ackCounts.containsKey(msg.seq)) {
            return;
        }
        int currentCount=this.ackCounts.getOrDefault(msg.seq, 0)+1;
        this.ackCounts.put(msg.seq, currentCount);
        int quorumThres=(this.systemGroup.size()/2)+1;
        debug("[COORDINATOR] RECV ACK for seq " + msg.seq + " from " + getSender().path().name() + ". Current: " + currentCount+"/"+quorumThres);
        if (currentCount==quorumThres) {
            applyLocalUpdate(msg.epoch, msg.seq);
            debug("[COORDINATOR] Quorum reached for seq " + msg.seq + ". Start Broadcast WRITE_OK");
            broadcast(new ProtocolMessages.WRITE_OK(msg.epoch, msg.seq), "WRITE_OK");
            this.ackCounts.remove(msg.seq);
        }
    }

    /**
     * Cohort receives notification of reached Quorum from leader and executes COMMIT of the state
     * @param msg WRITE_OK message (commit signal that is ok to make the update of the value)
     */
    private void onWriteOk(ProtocolMessages.WRITE_OK msg) {
        verifyCrash(Crash.Type.WriteOK);
        if(this.crashed) {
            return;
        }
        debug("RECV WRITE_OK for epoch " + msg.epoch + " and seq " + msg.seq + " from " + getSender().path().name());
        applyLocalUpdate(msg.epoch, msg.seq);
    }

    /**
     * Must be invoked whenever this replica applies an update to its local state (after updating positions[])
     * @param epoch state epoch reference
     * @param seq state update monotonic number
     */
    private void applyLocalUpdate(int epoch, int seq) {
        if(this.crashed) {
            return;
        }
        boolean coordinator=false;
        if(this.id==currentCoordinatorId) {
            coordinator=true;
        }
        ProtocolMessages.UPDATE matchingUpdate=null;
        for(ProtocolMessages.UPDATE u : this.history) {
            if(u.epoch==epoch && u.seq==seq) {
                matchingUpdate=u;
                break;
            }
        }
        if(matchingUpdate!=null) {
            this.positions[matchingUpdate.index]=matchingUpdate.value;
            if(coordinator) {
                debug("[COORDINATOR] UPDATE TO BE APPLIED to positions [" + matchingUpdate.index + "]="+matchingUpdate.value);
            }
            else {
                debug("UPDATE TO BE APPLIED to positions [" + matchingUpdate.index + "]="+matchingUpdate.value);
            }
            this.callbackOnUpdateApplied(epoch, seq, matchingUpdate.index, matchingUpdate.value, this.currentCoordinatorId);
            if(this.locallyContactedWrites.containsKey(matchingUpdate.index)) {
                ActorRef client=this.locallyContactedWrites.remove(matchingUpdate.index);
                this.tell(new ClientMessages.ReplyWrite(true, matchingUpdate.index, matchingUpdate.value, this.id), client);
            }
        }
        else {
            if(coordinator) {
                debug("[COORDINATOR] [WARNING] No match update found for seq " + seq);
            }
            else {
                debug("[WARNING] No match update found for seq " + seq);
            }
        }
    }

    /**
     * Helper sending serialized envelopes out to all cluster replicas excluding the self sender node
     * @param msg Serialized message of any class being ClientMessages, ElectionMessages or ProtocolMessages
     * @param type Type of string as identification, just for debug printout
     */
    private void broadcast(Serializable msg, String type) {
        if(this.crashed) {
            return;
        }
        boolean coordinator=false;
        if(this.id==currentCoordinatorId) {
            coordinator=true;
        }
        for(Map.Entry<Integer, ActorRef>  entry : this.systemGroup.entrySet()) {
            if(entry.getKey()!=this.id) {
                if(coordinator) {
                    debug("[COORDINATOR] SEND " + type + " to Replica_" + entry.getKey());
                }
                else {
                    debug("SEND " + type + " to Replica_" + entry.getKey());
                }
                this.tell(msg, entry.getValue());
            }
        }
    }

    //
    // FAULT MODULE AND CRASH MANAGEMENT
    //
    /**
     * Receives and memorizes instructions of test to simulate crash detection
     */
    @Override
    public void crash(AbstractReplica.Crash how_to_crash) {
        this.crashConfig=how_to_crash;
        this.crashCounter=how_to_crash.after_n_messages_of_type;
        debug("CRASH CONFIGURATION RECV: Type=" + how_to_crash.type + " Countdown=" + this.crashCounter);
        if(how_to_crash.type==Crash.Type.Now) {
            executeCrash();
        }
    }

    /**
     * Physically transitions the node actor layout into a total muted state, ignoring upcoming system msgs
     */
    void executeCrash() {
        this.crashed=true;
        log("CRASHED");
        cancelTimer(this.heartbeatSender); this.heartbeatSender=null;
        cancelTimer(this.heartbeatMonitor); this.heartbeatMonitor=null;
        cancelTimer(this.electionAckTimer); this.electionAckTimer=null;
        cancelTimer(this.electionGlobalTimer); this.electionGlobalTimer=null;
        getContext().become(createBaseReceiveBuilder().build());
    }

    private void cancelTimer(Cancellable c) {
        if(c!=null) {
            c.cancel();
        }
    }

    /**
     * Evaluation gate checking message ingestion parameters. If type mirrors fault-injection instructions, it
     * decreases countdown to trigger physical drop
     * @param currentMessType Type of message received as a fault to verify the respective crash.
     * These are:
     *  - Now        Crash immediately.
     *  - Heartbeat  Crash after processing heartbeat messages.
     *  - Update     Crash after processing update messages.
     *  - WriteOK    Crash after processing write acknowledgment messages.
     *  - Election   Crash after processing election-related messages.
     */
    void verifyCrash(Crash.Type currentMessType) {
        if(this.crashConfig!=null && this.crashConfig.type==currentMessType) {
            this.crashCounter--;
            debug("Crash countdown decremented for " + currentMessType + ". Remaining: " + this.crashCounter);
            if(this.crashCounter<=0) {
                executeCrash();
            }
        }
    }

    //
    //  MODULE HEARTBEAT AND LEADER ELECTION
    //
    /**
     * Schedules a one-shot self message after the given delay (used for heartbeat monitoring and election ACK timers)
     */
    private Cancellable scheduleOnceSelf(long delayMillis, Serializable msg) {
        return getContext().getSystem().scheduler().scheduleOnce(
                Duration.create(delayMillis, TimeUnit.MILLISECONDS),
                getSelf(), msg,
                getContext().getSystem().dispatcher(), getSelf());
    }

    /**
     * Starts (or restarts) the coordinator role: a fixed-rate self tick that drives the Heartbeat broadcast.
     * Implicitly stops the cohort monitoring since this replica is now the coordinator.
     */
    private void startHeartbeatSender() {
        cancelTimer(this.heartbeatMonitor); this.heartbeatMonitor=null;
        cancelTimer(this.heartbeatSender);
        long interval=getCoordinatorBeatInterval();
        this.heartbeatSender=getContext().getSystem().scheduler().scheduleAtFixedRate(
                Duration.create(0, TimeUnit.MILLISECONDS),
                Duration.create(interval, TimeUnit.MILLISECONDS),
                getSelf(), new HeartbeatTick(),
                getContext().getSystem().dispatcher(), getSelf());
    }

    /**
     * Coordinator self tick: broadcast a Heartbeat to the cohort. Stops itself if no longer coordinator.
     */
    private void onHeartbeatTick(HeartbeatTick tick) {
        if(this.crashed || this.id!=this.currentCoordinatorId) {
            cancelTimer(this.heartbeatSender); this.heartbeatSender=null;
            return;
        }
        broadcast(new Heartbeat(this.id), "HEARTBEAT");
    }

    /**
     * Arms (or rearms) the one-shot timer that fires if the coordinator stops beating.
     * The window must be larger than the beat interval so regular beats keep resetting it.
     */
    private void armHeartbeatMonitor() {
        cancelTimer(this.heartbeatMonitor);
        long timeout=(long)(2.5*getCoordinatorBeatInterval());
        this.heartbeatMonitor=scheduleOnceSelf(timeout, new HeartbeatTimeoutCheck());
    }

    /**
     * A Heartbeat proves the coordinator is alive: trust it and rearm the monitor.
     */
    private void onHeartbeat(Heartbeat msg) {
        verifyCrash(Crash.Type.Heartbeat);
        if(this.crashed) {
            return;
        }
        this.currentCoordinatorId=msg.coordinatorId;
        debug("RECV HEARTBEAT from Coordinator " + msg.coordinatorId);
        armHeartbeatMonitor();
    }

    /**
     * No heartbeat arrived within the window: suspect the coordinator and trigger the ring election.
     */
    private void onHeartbeatTimeoutCheck(HeartbeatTimeoutCheck msg) {
        if(this.crashed || this.id==this.currentCoordinatorId) {
            return;
        }
        log("COORDINATOR " + this.currentCoordinatorId + " SUSPECTED CRASHED");
        startElection(this.currentCoordinatorId);
    }

    /**
     * Ordered ring of replica ids (ascending), used to compute successors.
     */
    private List<Integer> sortedIds() {
        List<Integer> ids=new ArrayList<>(this.systemGroup.keySet());
        Collections.sort(ids);
        return ids;
    }

    /**
     * Next replica on the ring after {@code afterId}, skipping this replica itself and any suspected-crashed node.
     * @return the successor id, or -1 if no other alive replica exists
     */
    private int nextAliveSuccessor(int afterId) {
        List<Integer> ids=sortedIds();
        int n=ids.size();
        int idx=ids.indexOf(afterId);
        if(idx<0) {
            idx=ids.indexOf(this.id);
        }
        for(int k=1; k<=n; k++) {
            int cand=ids.get((idx+k)%n);
            if(cand==this.id) {
                continue;
            }
            if(this.suspectedCrashed.contains(cand)) {
                continue;
            }
            return cand;
        }
        return -1;
    }

    /**
     * Most recent update in the local history, or null if the history is empty.
     */
    private ProtocolMessages.UPDATE latestUpdate() {
        if(this.history.isEmpty()) {
            return null;
        }
        return this.history.get(this.history.size()-1);
    }

    /**
     * Begins an election to replace the suspected coordinator: marks participation, fires the callback once and
     * sends the ring token carrying this replica's candidacy (id + latest update).
     */
    private void startElection(int crashedCoord) {
        startElectionInternal(crashedCoord, true);
    }

    /**
     * Core of {@link #startElection}: marks participation, arms the global stall timer and emits the fresh ring token.
     * @param fireCallback false when restarting the same election (the callback must fire at most once per election)
     */
    private void startElectionInternal(int crashedCoord, boolean fireCallback) {
        if(this.electing) {
            return;
        }
        this.electing=true;
        this.suspectedCrashed.add(crashedCoord);
        if(fireCallback && !this.electionStartedFired) {
            callbackOnElectionStarted(crashedCoord);
            this.electionStartedFired=true;
        }
        armElectionGlobalTimer();
        Map<Integer, ProtocolMessages.UPDATE> token=new HashMap<>();
        token.put(this.id, latestUpdate());
        forwardToken(token, this.id);
    }

    /**
     * Arms the global stall timer. It fires only if the whole election fails to resolve
     * and is intentionally NOT reset on every hop.
     */
    private void armElectionGlobalTimer() {
        cancelTimer(this.electionGlobalTimer);
        long timeout=3L*getSystemNumberOfActors()*getMaxLatencyPlusTolerance();
        this.electionGlobalTimer=scheduleOnceSelf(timeout, new SyncTimeout());
    }

    /**
     * The election did not complete in time: discard the current attempt and restart it from scratch. Nodes skipped
     * in the previous attempt stay in {@code suspectedCrashed}, so a crashed best-candidate is excluded on retry.
     */
    private void onSyncTimeout(SyncTimeout msg) {
        if(this.crashed) {
            return;
        }
        if(this.electing) {
            log("ELECTION timed out without SYNCHRONIZATION, restarting");
            this.electing=false;
            this.electionAckExpectedFrom=-1;
            cancelTimer(this.electionAckTimer); this.electionAckTimer=null;
            this.pendingElectionToken=null;
            startElectionInternal(this.currentCoordinatorId, false);
            return;
        }
        ProtocolMessages.UPDATE last=latestUpdate();
        if(last!=null && this.positions[last.index]!=last.value) {
            debug("WRITE_OK timeout for seq " + last.seq + ". Coordinator Stop " + this.currentCoordinatorId);
            startElection(this.currentCoordinatorId);
        }
    }

    /**
     * Sends the ELECTION token to the next alive successor on the ring and arms the ACK timer to skip dead neighbors.
     * If no other replica is alive, this replica wins outright.
     */
    private void forwardToken(Map<Integer, ProtocolMessages.UPDATE> token, int afterId) {
        int succ=nextAliveSuccessor(afterId);
        if(succ==-1) {
            becomeCoordinator(token);
            return;
        }
        this.pendingElectionToken=token;
        this.electionAckExpectedFrom=succ;
        debug("SEND ELECTION to Replica_" + succ + " candidates=" + token.keySet());
        this.tell(new ElectionMessages.ELECTION(token), this.systemGroup.get(succ));
        cancelTimer(this.electionAckTimer);
        this.electionAckTimer=scheduleOnceSelf(2L*getMaxLatencyPlusTolerance(), new ElectionAckTimeout(succ));
    }

    /**
     * Receives the ring token: acks the predecessor, joins the election if needed, then either appends itself and
     * forwards, or (once the ring has closed) elects the most up-to-date candidate.
     */
    private void onElection(ElectionMessages.ELECTION msg) {
        verifyCrash(Crash.Type.Election);
        if(this.crashed) {
            return;
        }
        // Immediately confirm liveness to the predecessor so it does not skip us.
        this.tell(new ElectionMessages.ElectionAck(this.id), getSender());

        Map<Integer, ProtocolMessages.UPDATE> token=new HashMap<>(msg.candidates);
        // Stale token: the coordinator it refers to has already been settled (it is among the candidates).
        if(!this.electing && token.containsKey(this.currentCoordinatorId)) {
            debug("Ignoring stale ELECTION token (coordinator " + this.currentCoordinatorId + " already settled)");
            return;
        }
        if(!this.electing) {
            this.electing=true;
            this.suspectedCrashed.add(this.currentCoordinatorId);
            if(!this.electionStartedFired) {
                callbackOnElectionStarted(this.currentCoordinatorId);
                this.electionStartedFired=true;
            }
            armElectionGlobalTimer();
        }

        if(!token.containsKey(this.id)) {
            // Still collecting candidates: add ourselves and keep the token going.
            token.put(this.id, latestUpdate());
            forwardToken(token, this.id);
        } else {
            // The token has visited every alive replica: the most up-to-date one wins.
            int winner=computeWinner(token);
            if(winner==this.id) {
                becomeCoordinator(token);
            } else {
                forwardToken(token, this.id);
            }
        }
    }

    /**
     * Confirmation that the awaited successor is alive: cancel the skip timer.
     */
    private void onElectionAck(ElectionMessages.ElectionAck msg) {
        if(this.crashed) {
            return;
        }
        if(msg.senderId==this.electionAckExpectedFrom) {
            cancelTimer(this.electionAckTimer); this.electionAckTimer=null;
            this.electionAckExpectedFrom=-1;
            debug("RECV ElectionAck from Replica_" + msg.senderId);
        }
    }

    /**
     * The awaited successor did not ack in time: mark it crashed and route the token to the next alive replica.
     */
    private void onElectionAckTimeout(ElectionAckTimeout msg) {
        if(this.crashed || !this.electing) {
            return;
        }
        if(msg.expectedAckFromId!=this.electionAckExpectedFrom) {
            return;
        }
        debug("ElectionAck TIMEOUT from Replica_" + msg.expectedAckFromId + ", skipping it");
        this.suspectedCrashed.add(msg.expectedAckFromId);
        if(this.pendingElectionToken!=null) {
            forwardToken(this.pendingElectionToken, msg.expectedAckFromId);
        }
    }

    /**
     * Picks the election winner: highest (epoch, seq), ties broken by highest id.
     */
    private int computeWinner(Map<Integer, ProtocolMessages.UPDATE> token) {
        int bestId=-1;
        int bestEpoch=-1;
        int bestSeq=-1;
        for(Map.Entry<Integer, ProtocolMessages.UPDATE> e : token.entrySet()) {
            int cid=e.getKey();
            ProtocolMessages.UPDATE u=e.getValue();
            int ep=(u==null) ? -1 : u.epoch;
            int sq=(u==null) ? -1 : u.seq;
            boolean better=ep>bestEpoch
                    || (ep==bestEpoch && sq>bestSeq)
                    || (ep==bestEpoch && sq==bestSeq && cid>bestId);
            if(better) {
                bestEpoch=ep;
                bestSeq=sq;
                bestId=cid;
            }
        }
        return bestId;
    }

    /**
     * This replica won: it opens a fresh epoch, announces itself, pushes its history to the cohort via
     * SYNCHRONIZATION and takes over the heartbeat duty.
     */
    private void becomeCoordinator(Map<Integer, ProtocolMessages.UPDATE> token) {
        if(!this.electing) {
            return;
        }
        endElectionState();
        int maxEpoch=0;
        for(ProtocolMessages.UPDATE u : this.history) {
            if(u.epoch>maxEpoch) {
                maxEpoch=u.epoch;
            }
        }
        this.epoch=maxEpoch+1;
        this.seq=0;
        this.currentCoordinatorId=this.id;
        log("WON ELECTION, new coordinator is " + this.id + " (epoch " + this.epoch + ")");
        callbackOnCoordinatorElected(this.id);
        broadcast(new ElectionMessages.SYNCHRONIZATION(this.id, new ArrayList<>(this.history)), "SYNCHRONIZATION");
        startHeartbeatSender();
        recoverPendingWrite();
    }

    /**
     * New coordinator announced: adopt it, fill any gap in the local history (without re-replying to clients) and
     * resume monitoring the (new) coordinator's heartbeats.
     */
    private void onSynchronization(ElectionMessages.SYNCHRONIZATION msg) {
        if(this.crashed) {
            return;
        }
        endElectionState();
        this.suspectedCrashed.clear();
        this.currentCoordinatorId=msg.newCoordinatorId;
        debug("RECV SYNCHRONIZATION, new coordinator is " + msg.newCoordinatorId);
        //Missing max epoch update
        int maxEpochInSync=0;  
        for(ProtocolMessages.UPDATE u : msg.missingUpdates) {
            if(u.epoch>maxEpochInSync) {
                maxEpochInSync=u.epoch;
            }
            if(!historyContains(u.epoch, u.seq)) {
                this.history.add(new ProtocolMessages.UPDATE(u.epoch, u.seq, u.index, u.value, null));
                this.positions[u.index]=u.value;
                callbackOnUpdateApplied(u.epoch, u.seq, u.index, u.value, this.currentCoordinatorId);
            }
            else {
                this.positions[u.index]=u.value;
                boolean alreadyCalled=false;
                for(ProtocolMessages.UPDATE localU :this.history) {
                    if(localU.epoch==u.epoch && localU.seq==u.seq && this.currentCoordinatorId==msg.newCoordinatorId) {
                        alreadyCalled=true;
                    }
                }
                if(!alreadyCalled && this.id!=msg.newCoordinatorId) {
                    callbackOnUpdateApplied(u.epoch, u.seq, u.index, u.value, this.currentCoordinatorId);
                }
            }
        }
        if(this.id!=this.currentCoordinatorId) {
            this.epoch=maxEpochInSync+1;
        }
        this.seq=0;
        callbackOnCoordinatorElected(msg.newCoordinatorId);
        armHeartbeatMonitor();
        recoverPendingWrite();
    }

    /**
     * Resets the per-election bookkeeping once the election is resolved (won or synchronized).
     */
    private void endElectionState() {
        this.electing=false;
        this.electionStartedFired=false;
        this.electionAckExpectedFrom=-1;
        cancelTimer(this.electionAckTimer); this.electionAckTimer=null;
        cancelTimer(this.electionGlobalTimer); this.electionGlobalTimer=null;
        this.pendingElectionToken=null;
    }

    /**
     * True if an update with the given (epoch, seq) is already present in the local history.
     */
    private boolean historyContains(int epoch, int seq) {
        for(ProtocolMessages.UPDATE u : this.history) {
            if(u.epoch==epoch && u.seq==seq) {
                return true;
            }
            //Case leader changes if there is still an update
            /*if(this.id!=this.currentCoordinatorId && u.epoch==this.epoch && u.seq==this.seq) { 
                return true;
            }*/
        }
        return false;
    }

    /**
     * Helper pending write to be issued after election
     */
    private void recoverPendingWrite() {
        if(this.writeAfterElection!=null) {
            WriteForwardTimeout pending=this.writeAfterElection;
            this.writeAfterElection=null;
            debug("Re-submit pending write for index " + pending.index + " to new coordinator " + this.currentCoordinatorId);
            this.locallyContactedWrites.put(pending.index, pending.clientRef);
            if(this.id==this.currentCoordinatorId) {
                coordinatorWritePipeline(pending.index, pending.value, pending.clientRef);
            }
            else {
                writeForwardToCoordinator(pending.index, pending.value, pending.clientRef);
            }
        }
    }
}