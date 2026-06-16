# Work Division

Matteo
- [ ] **Setup and Message Classes Definion** -> Initialize Replica.java and mapping the setup methods + class of messages (ClientRead, ClientWrite ...) 
- [ ] **Response of Read Normal State** -> Response immediate and synchronous of clients on local array of the replicas
- [ ] **Write Pipeline** -> Normal behavior when a write is issued
- [ ] **Status Update** -> WRITEOK (callbackOnUpdateApplied)

Davide
- [ ] **Client Definition** -> Handle requests sending, planning and crash management (timeout of read and write via Scheduler) Client.java???
- [ ] **Heartbeat** -> crash method (Replica.java) periodic sending of the heartbeats to coordinator and timeout monitor
- [ ] **Election Algorithm** -> Ring-based algorithm via ID with tracking of the candidates and skip of not available ones
- [ ] **Synchronization** -> Reorder of the nodes after the election and broadcasting of the message

Together
- [ ] Debug
- [ ] Report
- [ ] Presentation + Demo (after 26/06)