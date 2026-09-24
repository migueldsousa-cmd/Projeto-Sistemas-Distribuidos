package pt.tecnico.blockchainist.sequencer;

import pt.tecnico.blockchainist.contract.*;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.io.InputStream;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import com.google.protobuf.ByteString;

public class SequencerServiceImpl extends SequencerServiceGrpc.SequencerServiceImplBase {
    
    private List<Block> blockchain = new ArrayList<>();
    private List<Transaction> pendingTransactions = new ArrayList<>();

    private int maxBlockSize;
    private int blockTimeout;
    private String sequencerId;

    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private ScheduledFuture<?> timerHandle;

    private static final boolean DEBUG = System.getProperty("debug") != null;

    private PrivateKey sequencerPrivateKey;

     public SequencerServiceImpl(int N, int T, String sequencerId) {
        this.maxBlockSize = N;
        this.blockTimeout = T;
        this.sequencerId = sequencerId;
        try {
            this.sequencerPrivateKey = loadPrivateKey("sequencerPrivate.der");
            System.out.println("Chave privada do Sequencer carregada com sucesso.");
        } catch (Exception e) {
            System.err.println("Erro ao carregar a chave privada: " + e.getMessage());
        }

        startTimer();
    }

    // ----- Auxiliary methods to add Signature -----

    private byte[] readResource(String path) throws Exception {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(path)) {
            if (is == null) {
                throw new IllegalArgumentException("Ficheiro não encontrado: " + path);
            }
            return is.readAllBytes();
        }
    }

    public PrivateKey loadPrivateKey(String resourcePath) throws Exception {
        byte[] keyBytes = readResource(resourcePath);
        PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes);
        KeyFactory kf = KeyFactory.getInstance("RSA");
        return kf.generatePrivate(spec);
    }

    public byte[] sign(byte[] data) throws Exception {
        java.security.Signature sig = java.security.Signature.getInstance("SHA256withRSA");
        sig.initSign(this.sequencerPrivateKey);
        sig.update(data);
        return sig.sign();
    }

    private Signature buildSignature(byte[] data, String userId) {
        try {
            byte[] signatureBytes = sign(data);
            if (DEBUG) System.err.println("Operação assinada com sucesso.");

            Signature signature = Signature.newBuilder()
                    .setSignerIdentifier(userId)
                    .setSignatureValue(ByteString.copyFrom(signatureBytes))
                    .build();

            return signature;

        } catch (Exception e) {
            throw new RuntimeException("Erro ao assinar", e);
        }
    }

    // startTimer function:
    // It is called at the initialization of sequencer and upon the creation of a Block
    private synchronized void startTimer() {
    	// if Timer already exists, it is cancelled
        if (timerHandle != null) {
            timerHandle.cancel(false);
        }
        timerHandle = scheduler.schedule(
        		this::createBlock,
        		blockTimeout, 
        		TimeUnit.SECONDS);
    }

    private synchronized void createBlock() {
        if (pendingTransactions.isEmpty()) {
        	startTimer();
        	return;
        }
        
        // 1. Create Block without Signature
        Block block = Block.newBuilder()
        		.addAllTransactions(pendingTransactions)
        		.build();

        // 2. Construir Signature
        Signature signature = buildSignature(block.toByteArray(), sequencerId);

        // 3. Adicionar Signature
        Block signedBlock = block.toBuilder()
                    .setSignature(signature)
                    .build();
        
        blockchain.add(signedBlock);
        pendingTransactions.clear();
        
        // Debug print
        if (DEBUG) System.err.println("Block created: " + (blockchain.size() - 1));
        
        // Closed block, restarts timer
        startTimer();
    }

    @Override
    public synchronized void broadcast(BroadcastRequest request, StreamObserver<BroadcastResponse> responseObserver) {
    	if (DEBUG) System.err.println("Received transaction");
        pendingTransactions.add(request.getTransaction());

        if (pendingTransactions.size() >= maxBlockSize) {
            createBlock();
        }
        
        BroadcastResponse response = BroadcastResponse.getDefaultInstance();
        
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public synchronized void deliverBlock(DeliverBlockRequest request, StreamObserver<DeliverBlockResponse> responseObserver) {
        int idx = request.getBlockId();

        if (idx < 0 || idx >= blockchain.size()) {
            responseObserver.onError(io.grpc.Status.NOT_FOUND
                .withDescription("Block not ready.")
                .asRuntimeException()
            );

            return;
        }
        if (DEBUG) System.err.println("Delivering block " + idx);

        DeliverBlockResponse response = DeliverBlockResponse.newBuilder()
                .setBlock(blockchain.get(idx))
                .build();
            
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public synchronized void syncBlocks(SyncRequest request, StreamObserver<Block> responseObserver) {
        for (Block block : blockchain) {
            responseObserver.onNext(block);
        }
        responseObserver.onCompleted();
    }
}