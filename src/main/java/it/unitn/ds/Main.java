package it.unitn.ds;

import java.util.HashMap;
import java.util.Map;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds.AbstractReplica.InitSystem;

public class Main {
    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("START");
        System.out.println("========================================\n");

        final int N_REPLICAS = 6;
        final int COORDINATOR_ID = 2;
        final ActorSystem system = ActorSystem.create("TestMain");

        Logger.setDestinationStdout();
        Logger.setDebugEnabled(true);

        Map<Integer, ActorRef> replicas = new HashMap<>(N_REPLICAS);
        for (int i = 0; i < N_REPLICAS; i++) {
            replicas.put(i,
                system.actorOf(
                    Replica.props(i, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL),
                    "Replica_" + i
                )
            );
        }

        InitSystem initMsg = new InitSystem(replicas, COORDINATOR_ID);
        for (Map.Entry<Integer, ActorRef> entry : replicas.entrySet()) {
            entry.getValue().tell(initMsg, ActorRef.noSender());
        }
        //SIMULATE WRITE PIPELINE
        try {
            Thread.sleep(1000);
        }
        catch(InterruptedException e) {
            e.printStackTrace();
        }
        //BOMBING
        new Thread(()->{
            replicas.get(1).tell(new ClientMessages.ClientWrite(0, 100), ActorRef.noSender());
        }).start();
        new Thread(()->{
            replicas.get(3).tell(new ClientMessages.ClientWrite(0, 200), ActorRef.noSender());
        }).start();
        new Thread(()->{
            replicas.get(2).tell(new ClientMessages.ClientWrite(1, 300), ActorRef.noSender());
        }).start();
        try{
            Thread.sleep(5000);
        }
        catch(InterruptedException e) {}
        /*System.out.println("\n[TEST] Simulate Client writing value 42 at index 0 via replica 2...");
        ClientMessages.ClientWrite fakeWrite1=new ClientMessages.ClientWrite(0, 42);
        ClientMessages.ClientWrite fakeWrite2=new ClientMessages.ClientWrite(1, 57);
        replicas.get(2).tell(fakeWrite1, ActorRef.noSender());
        replicas.get(4).tell(fakeWrite2, ActorRef.noSender());
        try {
            Thread.sleep(3000);
        }
        catch(InterruptedException e) {
            e.printStackTrace();
        }*/
        //SIMULATE READ
        System.out.println("\n[TEST] Simulating Client reading index 0 from all replicas");
        ClientMessages.ClientRead fakeRead1=new ClientMessages.ClientRead(0);
        ClientMessages.ClientRead fakeRead2=new ClientMessages.ClientRead(1);
        replicas.get(0).tell(fakeRead1, ActorRef.noSender());
        replicas.get(1).tell(fakeRead2, ActorRef.noSender());
        try {
            Thread.sleep(1000);
        }
        catch(InterruptedException e) {}

        // TODO: Create your clients
        
        // TODO: Implement your main logic

        system.terminate();

        System.out.println("\n========================================");
        System.out.println("END");
        System.out.println("========================================\n");
    }


}
