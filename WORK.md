# Work Division

Matteo

- [x] **Setup and Message Classes Definion** -> Initialize Replica.java and mapping the setup methods + class of messages (ClientRead, ClientWrite ...) 
- [x] **Response of Read Normal State** -> Response immediate and synchronous of clients on local array of the replicas
- [x] **Write Pipeline** -> Normal behavior when a write is issued
- [x] **Status Update** -> WRITEOK (callbackOnUpdateApplied)

Davide
- [ ] **Client Definition** -> Handle requests sending, planning and crash management (timeout of read and write via Scheduler) Client.java???
- [ ] **Heartbeat** -> crash method (Replica.java) periodic sending of the heartbeats to coordinator and timeout monitor
- [ ] **Election Algorithm** -> Ring-based algorithm via ID with tracking of the candidates and skip of not available ones
- [ ] **Synchronization** -> Reorder of the nodes after the election and broadcasting of the message

Together
- [ ] Report
    - [ ] Project Structure - Project Files, how they are organized, were are the messages, which types...
    - [ ] Design - State Machine, high-level description of the algorithm (primarly NORMAL_PATH and CRASH CASE)
    - [ ] Implementation - Code implementation details with pseudocode
    - [ ] Image of State Machine and Exchange of messages in both normal path and crash case
- [ ] Presentation 
- [ ] Demo