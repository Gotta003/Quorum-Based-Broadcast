# Day by Day

## DAY ONE
Matteo
- [x] Define message classes: 
    - [x] Client Messages to communicate: ClientRead, ClientWrite, ReadReply, WriteReply (implemented in `ClientMessages.java`)
    - [x] Protocol Messages to coordinate: WriteForward, Update, Ack, WriteOK (implemented in `ProtocolMessages.java`)
- [x] Define: Heartbeat, HeartbeatTimeoutCheck (as check by replicas), WriteForwardTimeout (timeout during WRITE_FORWARD) (`Replica.java`)
- [x] Implement Replica.initSystem() -> store group map, coordinatorId, and init int[] positions with MAX LENGTH and init of epoch and seq to 0 and getSystemNumberOfActors(), returning the actors in the system (`Replica.java`)