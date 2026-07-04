package it.unitn.ds;

import java.time.Duration;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import akka.pattern.Patterns;
import java.util.concurrent.TimeUnit;

public class DemoScenarios {
    private static final long READ_TIMEOUT=2000;
    private static final long WRITE_TIMEOUT=4000;
    private static Map<Integer, ActorRef> setupCluster(ActorSystem system, int nReplicas, int coordinatorId) {
        Map<Integer, ActorRef> replicas=new HashMap<>(nReplicas);
        for(int i=0; i<nReplicas; i++) {
            replicas.put(i, system.actorOf(Replica.props(i, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL), "Replica_"+i));
        }

        AbstractReplica.InitSystem initMsg=new AbstractReplica.InitSystem(replicas, coordinatorId);
        for(Map.Entry<Integer, ActorRef> entry : replicas.entrySet()) {
            entry.getValue().tell(initMsg, ActorRef.noSender());
        }
        try {
            Thread.sleep(1000);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        return replicas;
    }

    //DEMO 1
    public static void runNormalPathDemo(ActorSystem system, int nReplicas, int coordinatorId) {
        System.out.println("\nStandard Write Routine\n");
        Map<Integer, ActorRef> replicas=setupCluster(system, nReplicas, coordinatorId);
        ActorRef client=system.actorOf(Client.props(READ_TIMEOUT, WRITE_TIMEOUT, Optional.empty()), "DemoClient_Normal");
        System.out.println("[DEMO] Client sends (Idx: 5, Value: 100) to Replica_0");
        client.tell(new SendWriteCommand(replicas.get(0), 5, 100), ActorRef.noSender());
        try {
            Thread.sleep(2000);
        }
        catch(InterruptedException e) {
            e.printStackTrace();
        }
        printClusterSnapshot(system, replicas);
        System.out.println("[DEMO] The client sends another write (Index: 3, Value: 200) to Replica_0");
        client.tell(new SendWriteCommand(replicas.get(0), 3, 200), ActorRef.noSender());
        try {
            Thread.sleep(2000);
        }
        catch(InterruptedException e) {
            e.printStackTrace();
        }
        printClusterSnapshot(system, replicas);
    }

    //DEMO 2
    public static void runWriteBombingDemo(ActorSystem system, int nReplicas, int coordinatorId) {
        System.out.println("\nWrite Bombing with multiple clients\n");
        Map<Integer, ActorRef> replicas=setupCluster(system, nReplicas, coordinatorId);
        int numberOfBombs=15;
        System.out.println("[DEMO] Create " + numberOfBombs + " Clients concurrent which send multiple writes requests to replicas");
        for(int i=0; i<numberOfBombs; i++) {
            ActorRef bClient=system.actorOf(Client.props(READ_TIMEOUT, WRITE_TIMEOUT, Optional.empty()), "Client_"+i);
            int targetReplicaId=i%nReplicas;
            int index=i%AbstractReplica.POSITIONS_LIST_LENGTH;
            int value=(i+1)*50;
            bClient.tell(new  SendWriteCommand(replicas.get(targetReplicaId), index, value), ActorRef.noSender());
        }
        try {
            Thread.sleep(1000);
        }
        catch(InterruptedException e) {
            e.printStackTrace();
        }
        printClusterSnapshot(system, replicas);
        try {
            Thread.sleep(2000);
        }
        catch(InterruptedException e) {
            e.printStackTrace();
        }
        printClusterSnapshot(system, replicas);
        try {
            Thread.sleep(3000);
        }
        catch(InterruptedException e) {
            e.printStackTrace();
        }
        printClusterSnapshot(system, replicas);
        try {
            Thread.sleep(4000);
        }
        catch(InterruptedException e) {
            e.printStackTrace();
        }
        printClusterSnapshot(system, replicas);
    }

    //DEMO 3
    public static void runCoordinatorCrashElectionDemo(ActorSystem system, int nReplicas, int coordinatorId) {
        System.out.println("\nCoordinator Crash + Ring Election Demo\n");
        Map<Integer, ActorRef> replicas=setupCluster(system, nReplicas, coordinatorId);
        ActorRef client=system.actorOf(Client.props(READ_TIMEOUT, WRITE_TIMEOUT, Optional.empty()), "DemoClient_Election");

        System.out.println("[DEMO] Client writes (Idx: 0, Value: 1) to Replica_" + coordinatorId + " to build history");
        client.tell(new SendWriteCommand(replicas.get(coordinatorId), 0, 1), ActorRef.noSender());
        try {
            Thread.sleep(2000);
        }
        catch(InterruptedException e) {
            e.printStackTrace();
        }
        printClusterSnapshot(system, replicas);

        System.out.println("[DEMO] Crashing coordinator Replica_" + coordinatorId + " immediately");
        replicas.get(coordinatorId).tell(new AbstractReplica.Crash(AbstractReplica.Crash.Type.Now, 0), ActorRef.noSender());
        System.out.println("[DEMO] Waiting for heartbeat timeout detection and ring election to complete...");
        try {
            Thread.sleep(4000);
        }
        catch(InterruptedException e) {
            e.printStackTrace();
        }
        printClusterSnapshot(system, replicas);

        int lastNode=nReplicas-1;
        System.out.println("[DEMO] Client writes (Idx: 0, Value: 2) to Replica_" + lastNode + " to verify the new coordinator serves requests");
        client.tell(new SendWriteCommand(replicas.get(lastNode), 0, 2), ActorRef.noSender());
        try {
            Thread.sleep(2000);
        }
        catch(InterruptedException e) {
            e.printStackTrace();
        }
        printClusterSnapshot(system, replicas);
    }

    //DEMO 4
    public static void runCrashDuringElectionDemo(ActorSystem system, int nReplicas, int coordinatorId) {
        System.out.println("\nCrash During Election Demo\n");
        Map<Integer, ActorRef> replicas=setupCluster(system, nReplicas, coordinatorId);
        ActorRef client=system.actorOf(Client.props(READ_TIMEOUT, WRITE_TIMEOUT, Optional.empty()), "DemoClient_CrashElection");

        System.out.println("[DEMO] Client writes (Idx: 0, Value: 1) to Replica_" + coordinatorId + " to differentiate node histories");
        client.tell(new SendWriteCommand(replicas.get(coordinatorId), 0, 1), ActorRef.noSender());
        try {
            Thread.sleep(2000);
        }
        catch(InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("[DEMO] Crashing coordinator Replica_" + coordinatorId + " immediately");
        replicas.get(coordinatorId).tell(new AbstractReplica.Crash(AbstractReplica.Crash.Type.Now, 0), ActorRef.noSender());

        int highestNode=nReplicas-1;
        System.out.println("[DEMO] Arming Replica_" + highestNode + " to crash after receiving 1 ELECTION message");
        replicas.get(highestNode).tell(new AbstractReplica.Crash(AbstractReplica.Crash.Type.Election, 1), ActorRef.noSender());

        System.out.println("[DEMO] Waiting for the ring to skip the crashed node and the election to resolve...");
        try {
            Thread.sleep(6000);
        }
        catch(InterruptedException e) {
            e.printStackTrace();
        }
        printClusterSnapshot(system, replicas);

        int targetNode=nReplicas-2;
        System.out.println("[DEMO] Client writes (Idx: 0, Value: 99) to Replica_" + targetNode + " to prove the system recovered");
        client.tell(new SendWriteCommand(replicas.get(targetNode), 0, 99), ActorRef.noSender());
        try {
            Thread.sleep(2000);
        }
        catch(InterruptedException e) {
            e.printStackTrace();
        }
        printClusterSnapshot(system, replicas);
    }

    //DEMO 5
    public static void runWinnerCrashesDuringElectionDemo(ActorSystem system, int nReplicas, int coordinatorId) {
        System.out.println("\nWinner Crashes During Election Demo\n");
        Map<Integer, ActorRef> replicas=setupCluster(system, nReplicas, coordinatorId);
        int winnerCandidate=nReplicas-1;
        int secondBest=nReplicas-2;
        ActorRef client=system.actorOf(Client.props(READ_TIMEOUT, WRITE_TIMEOUT, Optional.empty()), "DemoClient_WinnerCrash");

        System.out.println("[DEMO] Committing three writes to build a shared history on all replicas");
        int[] values={1,2,3};
        for(int v : values) {
            client.tell(new SendWriteCommand(replicas.get(coordinatorId), 0, v), ActorRef.noSender());
            try { Thread.sleep(1000); } catch(InterruptedException e) { e.printStackTrace(); }
        }
        printClusterSnapshot(system, replicas);

        System.out.println("[DEMO] Crashing coordinator Replica_" + coordinatorId + " immediately");
        replicas.get(coordinatorId).tell(new AbstractReplica.Crash(AbstractReplica.Crash.Type.Now, 0), ActorRef.noSender());

        System.out.println("[DEMO] Arming Replica_" + winnerCandidate + " (the winner) to crash on its 2nd ELECTION message");
        replicas.get(winnerCandidate).tell(new AbstractReplica.Crash(AbstractReplica.Crash.Type.Election, 2), ActorRef.noSender());

        System.out.println("[DEMO] Waiting for the global stall timer to exclude the dead winner and elect the runner-up...");
        try { Thread.sleep(10000); } catch(InterruptedException e) { e.printStackTrace(); }
        printClusterSnapshot(system, replicas);

        System.out.println("[DEMO] Client writes (Idx: 0, Value: 99) to Replica_" + secondBest + " to prove the runner-up took over");
        client.tell(new SendWriteCommand(replicas.get(secondBest), 0, 99), ActorRef.noSender());
        try { Thread.sleep(2000); } catch(InterruptedException e) { e.printStackTrace(); }
        printClusterSnapshot(system, replicas);
    }

    // DEMO 5
    public static void runUniformAgreementDemo(ActorSystem system, int nReplicas, int coordinatorId) {
        System.out.println("\nUniform Agreement Demo\n");
        Map<Integer, ActorRef> replicas=setupCluster(system, nReplicas, coordinatorId);
        ActorRef client=system.actorOf(Client.props(READ_TIMEOUT, WRITE_TIMEOUT, Optional.empty()), "DemoClient_UniformAgreement");

        System.out.println("[DEMO] Baseline write (Idx: 0, Value: 10)");
        client.tell(new SendWriteCommand(replicas.get(coordinatorId), 0, 10), ActorRef.noSender());
        try { Thread.sleep(2000); } catch(InterruptedException e) { e.printStackTrace(); }

        System.out.println("[DEMO] Second write (Idx: 0, Value: 42)");
        client.tell(new SendWriteCommand(replicas.get(coordinatorId), 0, 42), ActorRef.noSender());
        try { Thread.sleep(2000); } catch(InterruptedException e) { e.printStackTrace(); }
        printClusterSnapshot(system, replicas);

        System.out.println("[DEMO] Crashing coordinator Replica_" + coordinatorId);
        replicas.get(coordinatorId).tell(new AbstractReplica.Crash(AbstractReplica.Crash.Type.Now, 0), ActorRef.noSender());

        System.out.println("[DEMO] Waiting for election + synchronization...");
        try { Thread.sleep(6000); } catch(InterruptedException e) { e.printStackTrace(); }
        printClusterSnapshot(system, replicas);

        System.out.println("[DEMO] All survivors must show identical Positions + History Size (agreement preserved across leadership change).");

        int probe=(coordinatorId+1)%nReplicas;
        System.out.println("[DEMO] Follow-up write to confirm the new coordinator serves requests");
        client.tell(new SendWriteCommand(replicas.get(probe), 1, 7), ActorRef.noSender());
        try { Thread.sleep(2000); } catch(InterruptedException e) { e.printStackTrace(); }
        printClusterSnapshot(system, replicas);
    }

    public static class SendWriteCommand {
        public final ActorRef targetReplica;
        public final int index;
        public final int value;
        public SendWriteCommand(ActorRef targetReplica, int index, int value) {
            this.targetReplica=targetReplica;
            this.index=index;
            this.value=value;
        }
    }

    

    public static void printClusterSnapshot(ActorSystem system, Map<Integer, ActorRef> replicas) {
        System.out.println("\n========================================================================================");
        System.out.println("                        CLUSTER VISUAL STATE SNAPSHOT                                   ");
        System.out.println("========================================================================================");
        System.out.printf("%-12s | %-10s | %-20s | %-35s | %-12s\n", "Node", "Role", "Coordinator", "Positions Array", "History Size");
        System.out.println("----------------------------------------------------------------------------------------");
        for(int i=0; i<replicas.size(); i++) {
            ActorRef replicaRef=replicas.get(i);
            String nodeName="Replica_"+i;
            try {
                CompletableFuture<Object> future=Patterns.ask(replicaRef, new DemoMessages.DumpStateRequest(), Duration.ofMillis(300)).toCompletableFuture();
                DemoMessages.ReplicaStateReply reply=(DemoMessages.ReplicaStateReply) future.get(350, TimeUnit.MILLISECONDS);
                if(reply.crashed) {
                    System.out.printf("%-12s | \u001B[31m%-10s\u001B[0m | %-20s | %-35s | %-12s\n", nodeName, "CRASHED", "[ UNREACHABLE ]", "[ MUTED ]", "-");
                }
                else {
                    String role=(reply.id==reply.currentCoordinatorId) ? "LEADER" : "COHORT";
                    if(reply.electing) {
                        role="ELECTING";
                    }
                    int[] subArray=Arrays.copyOfRange(reply.positions, 0, Math.min(reply.positions.length, 10));
                    String arrayStr=Arrays.toString(subArray);
                    String coordStr="Replica_"+reply.currentCoordinatorId;
                    System.out.printf("%-12s | %-10s | %-20s | %-35s | %-12d updates\n", nodeName, role, coordStr, arrayStr, reply.historySize);
                }
            }
            catch (Exception e) {
                System.out.printf("%-12s | \u001B[31m%-10s\u001B[0m | %-20s | %-35s | %-12s\n", nodeName, "TIMEOUT", "Unknown", "[ NO RESPONSE ]", "-");
            }
        }
        System.out.println("========================================================================================\n");
    }
}
