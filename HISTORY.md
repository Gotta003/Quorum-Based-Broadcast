# Day by Day

## DAY ONE
Matteo
- [x] Define message classes: 
    - [x] Client Messages to communicate: ClientRead, ClientWrite, ReadReply, WriteReply (implemented in `ClientMessages.java`)
    - [x] Protocol Messages to coordinate: WriteForward, Update, Ack, WriteOK (implemented in `ProtocolMessages.java`)
- [ ] Define: Update(epoch, seq, index, value, clientRef, originReplica), Ack(epoch, seq), WriteOK(epoch, seq)
- [ ] Define: Heartbeat, HeartbeatTimeoutCheck, WriteForwardTimeout
- [ ] Implement Replica.initSystem() -> store group map, coordinatorId, and init int[] positions with MAX LENGTH and init of epoch and seq to 0
- [ ] Implement getSystemNumberOfActors()