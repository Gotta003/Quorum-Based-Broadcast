package it.unitn.ds;

import java.util.HashMap;
import java.util.Map;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds.AbstractReplica.InitSystem;

public class Main {
    public static void main(String[] args) {
        String mode="normal";
        if(args.length>0) {
            mode=args[0].toLowerCase();
        }
        System.out.println("========================================");
        System.out.println("START MODE " + mode.toUpperCase());
        System.out.println("========================================\n");

        final int N_REPLICAS = 6;
        final int COORDINATOR_ID = 2;
        final ActorSystem system = ActorSystem.create("TestMain");

        Logger.setDestinationStdout();
        Logger.setDebugEnabled(false);

        switch(mode) {
            case "normal":
                DemoScenarios.runNormalPathDemo(system, N_REPLICAS, COORDINATOR_ID);
                break;
            case "bomb":
                DemoScenarios.runWriteBombingDemo(system, N_REPLICAS, COORDINATOR_ID);
                break;
            default:
                System.out.println("[ERROR] Mode " + mode + " not implemented.");
                System.out.println("Use one of the following: ['normal', 'bomb', ...]");
                break;
        }


        system.terminate();

        System.out.println("\n========================================");
        System.out.println("END DEMO");
        System.out.println("========================================\n");
    }


}
