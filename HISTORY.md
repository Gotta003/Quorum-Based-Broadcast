# Day by Day

## DAY TWO (18/06)

**Matteo**
- [x] Logs clearness and adjusting some minor logic, like abort unnecessary ACKs and making more clear the communications who sends and who receives
- [x] Bombing Test 3 write forward to see the robustness of the code
- [x] Adjust log format of replicas and conversion to debug
- [x] Comment out what implemented

## DAY ONE (17/06)

**Matteo**
- [x] Define message classes: 
    - [x] Client Messages to communicate: ClientRead, ClientWrite, ReadReply, WriteReply (implemented in `ClientMessages.java`)
    - [x] Protocol Messages to coordinate: WriteForward, Update, Ack, WriteOK (implemented in `ProtocolMessages.java`)
- [x] Define: Heartbeat, HeartbeatTimeoutCheck (as check by replicas), WriteForwardTimeout (timeout during WRITE_FORWARD) (`Replica.java`)
- [x] Implement Replica.initSystem() -> store group map, coordinatorId, and init int[] positions with MAX LENGTH and init of epoch and seq to 0 and getSystemNumberOfActors(), returning the actors in the system (`Replica.java`)
- [x] Define messages of Crash & Election in a new file called `ElectionMessages.java` and the timeouts in `Replica.java`
    - [x] Message that goes on the ring Election
    - [x] ElectionAck (immediate response of a replica that sends to the previous on the ring to confirm receiving of election message) and ElectionAckTimeout (local timer that triggers if the connected cohort doesn't respond with ACK, signaling crashing and that should be skipped)
    - [x] SyncTimeout (timer of waiting completion of sync)
    - [x] Synchronization (message sent to new coordinator elected in broadcast to all to align histories with missing updates)
- [x] Create methods in replica that correspond to a standard write path for a replica and connect those to createReceive
- [x] Implement the various methods:
    - [x] onClientRead
    - [x] onClientWrite
    - [x] onWriteForward
    - [x] onUpdate
    - [x] onAck
    - [x] onWriteOk
- [x] Crash Methods implementation and managed cased related to Normal Path (Update and WriteOK crashes)
- [x] Create a Fake Write request with no client since they have to be implemented
- [x] Create a Fake Read request