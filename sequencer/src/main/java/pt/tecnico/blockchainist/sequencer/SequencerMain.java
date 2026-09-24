package pt.tecnico.blockchainist.sequencer;

import pt.tecnico.blockchainist.sequencer.SequencerServiceImpl;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.util.UUID;

public class SequencerMain {
    // unique ID for this instance of sequencer
    private static final String sequencerId = UUID.randomUUID().toString();

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Argument(s) missing!");
            printUsage();
            return;
        }

        int port = Integer.parseInt(args[0]);
        // Default N and T
        int N = 4;
        int T = 5;

        if (args.length == 3) {
            N = Integer.parseInt(args[1]);
            T = Integer.parseInt(args[2]);
        }
        
        Server server = ServerBuilder.forPort(port)
                .addService(new SequencerServiceImpl(N, T, sequencerId))
                .build();

        server.start();
        System.out.println("Sequencer created in port " + port);
        System.out.println("Max block size: " + N);
        System.out.println("Timer: " + T + " seconds");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("Shutting down sequencer...");
            if (server != null) {
                server.shutdown();
            }
        }));

        server.awaitTermination();
    }

    private static void printUsage() {
        System.err.println("Usage: mvn exec:java -Dexec.args=\"<port>\"");
        System.err.println(" OR  : mvn exec:java -Dexec.args=\"<port> <N> <T>\"");
    }
}