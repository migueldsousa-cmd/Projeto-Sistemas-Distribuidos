package pt.tecnico.blockchainist.node;

import pt.tecnico.blockchainist.node.domain.NodeState;
import pt.tecnico.blockchainist.contract.*;
import io.grpc.stub.StreamObserver;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.Context;
import java.io.InputStream;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;

public class NodeServiceImpl extends NodeServiceGrpc.NodeServiceImplBase {

    private NodeState nodeState;
    private SequencerServiceGrpc.SequencerServiceBlockingStub sequencerStub;

    private int nextBlockId = 0;
    private static final boolean DEBUG = System.getProperty("debug") != null;

    private PublicKey clientPublicKey;
    private PublicKey sequencerPublicKey;

    public NodeServiceImpl(NodeState nodeState, SequencerServiceGrpc.SequencerServiceBlockingStub sequencerStub) {
        this.nodeState = nodeState;
        this.sequencerStub = sequencerStub;

        try {
            this.clientPublicKey = loadPublicKey("clientPublic.der");
            this.sequencerPublicKey = loadPublicKey("sequencerPublic.der");
            System.out.println("Chave pública do Node carregada com sucesso.");
        } catch (Exception e) {
            System.err.println("Erro ao carregar a chave pública: " + e.getMessage());
        }

        startPollingThread();
    }

    // Background Thread asking sequencer continuosly
    private void startPollingThread() {
        new Thread(
        		() -> {
	            while (true) {
	                try {
	                    DeliverBlockRequest request = DeliverBlockRequest.newBuilder().setBlockId(nextBlockId).build();
	                    DeliverBlockResponse response = sequencerStub.deliverBlock(request);

                        Block block = response.getBlock();
                        try {
                            if (verifyBlock(block) == false) {
                                System.err.println("Bloco rejeitado: Assinatura do Sequencer inválida");
                                continue;
                            }
                        } catch (Exception e) {
                            System.err.println("Erro ao verificar bloco: " + e.getMessage());
                            continue;
                        }
	                    
                        if (DEBUG) System.err.println("Received block " + nextBlockId);
	                    nodeState.applyBlock(response.getBlock());
                        if (DEBUG) System.err.println("Applied block " + nextBlockId);
	                    nextBlockId++;
	                    
	                    synchronized(nodeState) {
	                        nodeState.notifyAll();
	                    }
	                    
	                } catch (StatusRuntimeException e) {
	                    if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
	                        // Block not closed, wait 1 sec and try again
	                        try { Thread.sleep(1000); } catch (InterruptedException ignored) {}
	                    }
	                }
	            }
	        }
        )
        .start();
    }

    private PublicKey loadPublicKey(String resourcePath) throws Exception {
        byte[] keyBytes = readResource(resourcePath);
        X509EncodedKeySpec spec = new X509EncodedKeySpec(keyBytes);
        KeyFactory kf = KeyFactory.getInstance("RSA");
        return kf.generatePublic(spec);
    }

    private byte[] readResource(String path) throws Exception {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(path)) {
            if (is == null) {
                throw new IllegalArgumentException("Ficheiro não encontrado nos resources: " + path);
            }
            return is.readAllBytes();
        }
    }

    private boolean verifyBlock(Block block) throws Exception {
        java.security.Signature sig = java.security.Signature.getInstance("SHA256withRSA");
        sig.initVerify(sequencerPublicKey);
        Signature receivedSignature = block.getSignature();
        sig.update(block.toByteArray());
        boolean isValid = sig.verify(receivedSignature.getSignatureValue().toByteArray());
        if (isValid == false) {
            return false;
        }
        return true;
    }

    // Simula o atraso pedido no enunciado [Line 204]
    private void handleDelay() {
        Integer delay = DelayInterceptor.DELAY_KEY.get();
        if (delay != null && delay > 0) {
            try {
            	// Debug print
                System.out.println("Delay execution for: " + delay + " seconds");
                
                Thread.sleep(delay * 1000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // Block client thread until transaction is sent in Block from sequencer
    private void waitForExecution(Transaction transaction) throws InterruptedException {
        synchronized(nodeState) {
            while (nodeState.isTransactionExecuted(transaction) == false) {
            	nodeState.wait();
            }
        }
    }

    @Override
    public void createWallet(CreateWalletRequest request, StreamObserver<CreateWalletResponse> responseObserver) {
        if (DEBUG) System.err.println("Received createWallet request");
        handleDelay();
        try {
            // 1. Verificar Assinatura
            java.security.Signature sig = java.security.Signature.getInstance("SHA256withRSA");
            sig.initVerify(clientPublicKey);

            Signature receivedSignature = request.getSignature();
            sig.update(request.toByteArray());
            boolean isValid = sig.verify(receivedSignature.getSignatureValue().toByteArray());
            
            if (isValid == false) {
                throw new Exception("Create Wallet: Assinatura do Client inválida");
            }

            nodeState.validateUserOrganization(request.getUserId());

            // 2. Criar Transaction sem Signature para enviar ao Sequencer
            CreateWalletRequest requestWithoutSignature = request.toBuilder().clearSignature().build();

            Transaction transaction = Transaction.newBuilder()
												 .setClientId(requestWithoutSignature.getClientId())
												 .setSequenceNumber(requestWithoutSignature.getSequenceNumber())
												 .setCreateWallet(requestWithoutSignature)
												 .build();

            // 3. Broadcast to Sequencer
            BroadcastRequest sequencerRequest = BroadcastRequest.newBuilder()
                                                                .setTransaction(transaction)
                                                                .build();

            if (DEBUG) System.err.println("Sending transaction to sequencer");
            sequencerStub.broadcast(sequencerRequest);

            // 4. Wait for block to arrive, then execute changes
            waitForExecution(transaction);

            CreateWalletResponse response = CreateWalletResponse.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(Status.ALREADY_EXISTS.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void deleteWallet(DeleteWalletRequest request, StreamObserver<DeleteWalletResponse> responseObserver) {
        if (DEBUG) System.err.println("Received deleteWallet request");
        handleDelay();
        try {
            // 1. Verificar Assinatura
            java.security.Signature sig = java.security.Signature.getInstance("SHA256withRSA");
            sig.initVerify(clientPublicKey);

            Signature receivedSignature = request.getSignature();
            sig.update(request.toByteArray());
            boolean isValid = sig.verify(receivedSignature.getSignatureValue().toByteArray());
            
            if (isValid == false) {
                throw new Exception("Delete Wallet: Assinatura do Client inválida");
            }

            nodeState.validateUserOrganization(request.getUserId());

            // 2. Criar Transaction sem Signature para enviar ao Sequencer
            DeleteWalletRequest requestWithoutSignature = request.toBuilder().clearSignature().build();

            Transaction transaction = Transaction.newBuilder()
            									 .setClientId(requestWithoutSignature.getClientId())
            									 .setSequenceNumber(requestWithoutSignature.getSequenceNumber())
                                                 .setDeleteWallet(requestWithoutSignature)
                                                 .build();

            // 3. Broadcast to Sequencer
            BroadcastRequest sequencerRequest = BroadcastRequest.newBuilder()
                                                                .setTransaction(transaction)
                                                                .build();

            if (DEBUG) System.err.println("Sending transaction to sequencer");
            sequencerStub.broadcast(sequencerRequest);

            // 4. Wait for block to arrive, then execute changes
            waitForExecution(transaction);

            DeleteWalletResponse response = DeleteWalletResponse.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(Status.FAILED_PRECONDITION.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void transfer(TransferRequest request, StreamObserver<TransferResponse> responseObserver) {
        if (DEBUG) System.err.println("Received transfer request");
        handleDelay();
        try {
            // 1. Verificar Assinatura
            java.security.Signature sig = java.security.Signature.getInstance("SHA256withRSA");
            sig.initVerify(clientPublicKey);

            Signature receivedSignature = request.getSignature();
            sig.update(request.toByteArray());
            boolean isValid = sig.verify(receivedSignature.getSignatureValue().toByteArray());
            
            if (isValid == false) {
                throw new Exception("Transfer: Assinatura do Client inválida");
            }

            nodeState.validateUserOrganization(request.getSrcUserId());

            // 2. Execute transfer immediately
            nodeState.transfer(request.getSrcUserId(), request.getSrcWalletId(), request.getDstWalletId(), request.getValue());
            nodeState.addCausalTransferExecuted(request.getClientId(), request.getSequenceNumber());

            // 3. Criar Transaction sem Signature para enviar ao Sequencer
            TransferRequest requestWithoutSignature = request.toBuilder().clearSignature().build();
            Transaction transaction = Transaction.newBuilder()
					 							 .setClientId(requestWithoutSignature.getClientId())
					 							 .setSequenceNumber(requestWithoutSignature.getSequenceNumber())
                                                 .setTransfer(requestWithoutSignature)
                                                 .build();

            // 4. Broadcast to Sequencer, does not wait for block to execute transfer
            BroadcastRequest sequencerRequest = BroadcastRequest.newBuilder()
                                                                .setTransaction(transaction)
                                                                .build();

            if (DEBUG) System.err.println("Sending transaction to sequencer");
            sequencerStub.broadcast(sequencerRequest);

            TransferResponse response = TransferResponse.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(Status.FAILED_PRECONDITION.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void readBalance(ReadBalanceRequest request, StreamObserver<ReadBalanceResponse> responseObserver) {
        handleDelay();
        try {
            long balance = nodeState.readBalance(request.getWalletId());

            ReadBalanceResponse response = ReadBalanceResponse.newBuilder()
                                                              .setBalance(balance)
                                                              .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void getBlockchainState(GetBlockchainStateRequest request, StreamObserver<GetBlockchainStateResponse> responseObserver) {
        GetBlockchainStateResponse response =
                GetBlockchainStateResponse.newBuilder()
                                          .addAllTransactions(nodeState.getBlockchainState())
                                          .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}