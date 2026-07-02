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
