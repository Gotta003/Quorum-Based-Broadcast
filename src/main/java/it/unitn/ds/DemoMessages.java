package it.unitn.ds;

import java.io.Serializable;

public class DemoMessages {
    public static final class DumpStateRequest implements Serializable {}

    public static final class ReplicaStateReply implements Serializable {
        public final int id;
        public final boolean crashed;
        public final boolean electing;
        public final int currentCoordinatorId;
        public final int[] positions;
        public final int historySize;

        public ReplicaStateReply(int id, boolean crashed, boolean electing, int currentCoordinatorId, int[] positions, int historySize) {
            this.id=id;
            this.crashed=crashed;
            this.electing=electing;
            this.currentCoordinatorId=currentCoordinatorId;
            this.positions=positions;
            this.historySize=historySize;
        }
    }
}
