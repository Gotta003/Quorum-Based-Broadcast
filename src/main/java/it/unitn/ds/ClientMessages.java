package it.unitn.ds;

import java.io.Serializable;

/*
· Description: 
Class that defines the messages exchanged between Client and Replicas, so the requests and the replies, which can be of two types Reads and Writes. The requests from tests are managed by the abstract framework using AbstractClient.ReadRequest and WriteRequest, which trigger the client's mandatory methods. 
This class implements the network responses:
1) ReplyRead (Reply of Read) -> Response from replica to client of the read value
2) ReplyWrite (Reply of Write) -> Response only if the commit happened correctly (WRITE_OK)

· How to use?
The Client messages have to be used inside the Client Actor (Client.java), instead the Reply inside the Replica (Replica.java). The class is a fake one, so they will be used as follows:
1) ReplyRead (Respond to a read)
this.tell(new ClientMessages.ReplyRead(value, index, this.id), clientRef);
2) ReplyWrite (after WRITE_OK)
this.tell(new ClientMessages.ReplyWrite(true, index, value, this.id), clientRef);

These calls are just illustrative and the variables should be adapted to the context, using tell as the started implementation was based on that.
*/
public class ClientMessages {
    private ClientMessages() {
        //CONSTRUCTOR TO PREVENT INITIALIZATION
    }

    public static final class ReplyRead implements Serializable {
        public final int value;
        public final int index;
        public final int replicaId;
        public ReplyRead(int value, int index, int replicaId) {
            this.value=value;
            this.index=index;
            this.replicaId=replicaId;
        }
    }

    public static final class ReplyWrite implements Serializable {
        public final boolean success;
        public final int index;
        public final int value;
        public final int replicaId;

        public ReplyWrite(boolean success, int index, int value, int replicaId) {
            this.success=success;
            this.index=index;
            this.value=value;
            this.replicaId=replicaId;
        }
    }
}
