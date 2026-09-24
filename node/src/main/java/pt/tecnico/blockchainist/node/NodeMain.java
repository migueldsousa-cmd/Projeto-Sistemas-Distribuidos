package pt.tecnico.blockchainist.node;

import pt.tecnico.blockchainist.node.domain.NodeState;
import pt.tecnico.blockchainist.contract.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import java.util.Iterator;

import java.io.IOException;

public class NodeMain {
    public static void main(String[] args) throws IOException, InterruptedException {
    	if (args.length < 1) {
			System.err.println("Argument(s) missing!");
            printUsage();
			return;
		}
    	
        int port = Integer.parseInt(args[0]);
        String organization = args[1];

        String[] split = args[2].split(":");
        String sequencerHost = split[0];
        int sequencerPort = Integer.parseInt(split[1]);

        NodeState nodeState = new NodeState(organization);

        // 1. Connect Node to Sequencer
        ManagedChannel sequencerChannel = ManagedChannelBuilder
                .forAddress(sequencerHost, sequencerPort)
                .usePlaintext()
                .build();
        SequencerServiceGrpc.SequencerServiceBlockingStub sequencerStub = 
                SequencerServiceGrpc.newBlockingStub(sequencerChannel);

        // 2. Synchronize Node
        
        // Debug
        System.out.println("Synchronization started...");
        
        Iterator<Block> blockIterator = sequencerStub.syncBlocks(SyncRequest.getDefaultInstance());
        
        while (blockIterator.hasNext()) {
            Block block = blockIterator.next();
            nodeState.applyBlock(block);
        }
        
        // Debug
        System.out.println("Synchronization finished.");

        // 3. Initiate Node as server to Client
        NodeServiceImpl service = new NodeServiceImpl(nodeState, sequencerStub);

        Server server = ServerBuilder.forPort(port)
                .addService(ServerInterceptors.intercept(service, new DelayInterceptor()))
                .build();

        
        server.start();
        System.out.println("Node created. \n"
        		+ " -organization: " + organization
        		+ "\n -port: " + port
        		+ "\n -Sequencer: " + sequencerHost + ":" + sequencerPort);
                
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("Shutting down node...");
            if (server != null) {
                server.shutdown();
            }
            if (sequencerChannel != null) {
                sequencerChannel.shutdown();
            }
        }));

        server.awaitTermination();
    }

    private static void printUsage() {
        System.err.println("Usage: mvn exec:java -Dexec.args=\"<port> <organization> <sequencerHost>:<sequencerPort>\"");
    }
}
