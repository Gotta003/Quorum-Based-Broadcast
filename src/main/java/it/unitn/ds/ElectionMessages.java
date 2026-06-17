package it.unitn.ds;

import java.io.Serializable;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.util.List;

/*
· Description:
Class that defines the messages exchanged between replicas during Coordinator election phase and the synchronization. Internal timers and triggers are local to each replica and are inside Replica.java. This class implements 3 messages:
1) ELECTION -> Circulates around the ring gathering the most recent update of each candidate
2) ElectionAck -> Response to sender of an ELECTION message to confirm connection is healthy
3) SYNCHRONIZATION -> Broadcast by election winner to enforce the new coordinator and push the missing updates

· How to use?
These messages must be used inside the Replica (Replica.java) during fault handling routines. So the calls should be adapted to the context like:
1) ELECTION (Forwarding ring token to next neighbor that is alive)
this.tell(new ElectionMessages.ELECTION(candidatesMap), nextNeighborRef)
2) ElectionAck (Acknowledge receipt back to the predecessor in the ring)
this.tell(new ElectionMessages.ElectionAck(this.id), predecessorRef)
3) SYNCHRONIZATION (New leader enforcing state consistency across all replicas)
this.tell(new ElectionMessages.SYNCHRONIZATION(this.id, missingUpdatesList), targetReplicaRef)
*/
public class ElectionMessages {
    private ElectionMessages() {
        //CONSTRUCTOR TO PREVENT INITIALIZATION
    }

    public static final class ELECTION implements Serializable {
        public final Map<Integer, ProtocolMessages.UPDATE> candidates;
        public ELECTION(Map<Integer, ProtocolMessages.UPDATE> candidates) {
            this.candidates=Collections.unmodifiableMap(new HashMap<>(candidates));
        }
    }

    public static final class ElectionAck implements Serializable {
        public final int senderId;
        public ElectionAck(int senderId) {
            this.senderId=senderId;
        }
    }

    public static final class SYNCHRONIZATION implements Serializable {
        public final int newCoordinatorId;
        public final List<ProtocolMessages.UPDATE> missingUpdates;
        public SYNCHRONIZATION(int newCoordinatorId, List<ProtocolMessages.UPDATE> missingUpdates) {
            this.newCoordinatorId=newCoordinatorId;
            this.missingUpdates=List.copyOf(missingUpdates);
        }
    }
}
