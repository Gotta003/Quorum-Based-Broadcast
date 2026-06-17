package it.unitn.ds;

import java.io.Serializable;
import akka.actor.ActorRef;

/*
· Description: 
Class that defines the calls for the two-phase broadcast implementation (check in images the corresponding image)
Are implemented 4 calls that maps the parameters required to perform the actions in the image:
1) WRITE_FORWARD -> It is a write issue from a replica of the cohort to the coordinator replica
2) UPDATE -> Coordinator assigns the epoch and the sequence and sends it in a broadcast mode to all the cohort
3) ACK -> After receiving the update command, replicas sends that they have received that correctly with this one
4) WRITE_OK -> Coordinator notify the quorum and execute the commit, leading to a permanent change of the replica

· How to use?
1) WRITE_FORWARD (replica -> coordinator)
this.tell(new ProtocolMessages.WRITE_FORWARD(index, value, clientRef), coordinatorRef)
2) UPDATE (broadcast from coordinator)
this.tell(new ProtocolMessages.UPDATE(epoch, seq, index, value, clientRef), targetReplica)
3) ACK (replica -> coordinator)
this.tell(new ProtocolMessages.ACK(epoch, seq, this.id), coordinatorRef);
4) WRITE_OK (Broadcast PHASE 2 upon quorum)
this.tell(new ProtocolMessages.WRITE_OK(epoch, seq), targetReplica)
*/
public final class ProtocolMessages {
    private ProtocolMessages() {
        //CONSTRUCTOR TO PREVENT INITIALIZATION
    }
    
    public static final class WRITE_FORWARD implements Serializable {
        public final int index;
        public final int value;
        public final ActorRef clientRef;
        public WRITE_FORWARD(int index, int value, ActorRef clientRef) {
            this.index=index;
            this.value=value;
            this.clientRef=clientRef;
        }
    }

    public static class UPDATE implements Serializable {
        public final int epoch;
        public final int seq;
        public final int index;
        public final int value;
        public final ActorRef clientRef;
        public UPDATE(int epoch, int seq, int index, int value, ActorRef clientRef) {
            this.epoch=epoch;
            this.seq=seq;
            this.index=index;
            this.value=value;
            this.clientRef=clientRef;
        }
    }

    public static class ACK implements Serializable {
        public final int epoch;
        public final int seq;
        public final int replicaId;
        public ACK(int epoch, int seq, int replicaId) {
            this.epoch=epoch;
            this.seq=seq;
            this.replicaId=replicaId;
        }
    }

    public static class WRITE_OK implements Serializable {
        public final int epoch;
        public final int seq;

        public WRITE_OK(int epoch, int seq) {
            this.epoch=epoch;
            this.seq=seq;
        }
    }
}
