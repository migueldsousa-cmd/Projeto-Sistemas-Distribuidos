package pt.tecnico.blockchainist.client.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import pt.tecnico.blockchainist.contract.*;
import io.grpc.stub.StreamObserver;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.AbstractStub;
import java.io.InputStream;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import com.google.protobuf.ByteString;

public class ClientNodeService {

    /**
	 * Set flag to true to print debug messages. The flag can be set using the
	 * -Ddebug command line option.
	 */
	private static final boolean DEBUG_FLAG = (System.getProperty("debug") != null);

	/** Helper method to print debug messages. */
	private static void debug(String debugMessage) {
		if (DEBUG_FLAG)
			System.err.println(debugMessage);
	}

	private ManagedChannel channel;
    private NodeServiceGrpc.NodeServiceBlockingStub blockingStub;
    private NodeServiceGrpc.NodeServiceStub asyncStub;
    private String organization;
    
    static final Metadata.Key<String> DELAY_HEADER =
    	      Metadata.Key.of("delay", Metadata.ASCII_STRING_MARSHALLER);

    private PrivateKey clientPrivateKey;
    
    public ClientNodeService(String host, int port, String organization) {
    	this.organization = organization;
        
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
                
        this.blockingStub = NodeServiceGrpc.newBlockingStub(channel);
        this.asyncStub = NodeServiceGrpc.newStub(channel);

        try {
            this.clientPrivateKey = loadPrivateKey("clientPrivate.der");
            debug("Chave privada do cliente carregada com sucesso.");
        } catch (Exception e) {
            System.err.println("Erro ao carregar a chave privada: " + e.getMessage());
        }
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
        sig.initSign(this.clientPrivateKey);
        sig.update(data);
        return sig.sign();
    }

    private Signature buildSignature(byte[] data, String userId) {
        try {
            byte[] signatureBytes = sign(data);
            debug("Operação assinada com sucesso.");

            Signature signature = Signature.newBuilder()
                    .setSignerIdentifier(userId)
                    .setSignatureValue(ByteString.copyFrom(signatureBytes))
                    .build();

            return signature;

        } catch (Exception e) {
            throw new RuntimeException("Erro ao assinar", e);
        }
    }

    // Auxiliary method to add delay to stub
    private <T extends AbstractStub<T>> T attachDelay(T stub, int delay) {
        Metadata headers = new Metadata();
        headers.put(DELAY_HEADER, "delay");
        
        T stubWithHeader = 
        		  stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor((headers)));
        
        return stubWithHeader;
    }

    // ----- Blocking stub methods -----

    public CreateWalletResponse createWallet(String userId, String walletId, int delay, String clientId, long sequenceNumber) {
        // 1. Criar request sem assinatura
        CreateWalletRequest request = CreateWalletRequest.newBuilder()
                .setUserId(userId)
                .setWalletId(walletId)
                .setClientId(clientId)
                .setSequenceNumber(sequenceNumber)
                .build();

        // 2. Construir Signature
        Signature signature = buildSignature(request.toByteArray(), userId);

        // 3. Adicionar Signature ao request
        CreateWalletRequest signedRequest = request.toBuilder()
                .setSignature(signature)
                .build();
        
        return attachDelay(blockingStub, delay).createWallet(signedRequest);
    }
    
    public DeleteWalletResponse deleteWallet(String userId, String walletId, int delay, String clientId, long sequenceNumber) {
        // 1. Criar request sem assinatura
        DeleteWalletRequest request = DeleteWalletRequest.newBuilder()
                .setUserId(userId)
                .setWalletId(walletId)
                .setClientId(clientId)
                .setSequenceNumber(sequenceNumber)
                .build();

        // 2. Construir Signature
        Signature signature = buildSignature(request.toByteArray(), userId);

        // 3. Adicionar Signature ao request
        DeleteWalletRequest signedRequest = request.toBuilder()
                    .setSignature(signature)
                    .build();

        return attachDelay(blockingStub, delay).deleteWallet(signedRequest);
    }
    
    public ReadBalanceResponse readBalance(String walletId, int delay) {
        ReadBalanceRequest request = ReadBalanceRequest.newBuilder()
                .setWalletId(walletId)
                .build();

        return attachDelay(blockingStub, delay).readBalance(request);
    }
    
    public TransferResponse transfer(String srcUserId, String srcWalletId, String dstWalletId, long amount, int delay, String clientId, long sequenceNumber) {
        // 1. Criar request sem assinatura
        TransferRequest request = TransferRequest.newBuilder()
                .setSrcUserId(srcUserId)
                .setSrcWalletId(srcWalletId)
                .setDstWalletId(dstWalletId)
                .setValue(amount)
                .setClientId(clientId)
                .setSequenceNumber(sequenceNumber)
                .build();

        // 2. Construir Signature
        Signature signature = buildSignature(request.toByteArray(), srcUserId);

        // 3. Adicionar Signature ao request
        TransferRequest signedRequest = request.toBuilder()
                    .setSignature(signature)
                    .build();
        
        return attachDelay(blockingStub, delay).transfer(signedRequest);
    }
    
    // ----- Assync methods -----

    public void createWalletAsync(String userId, String walletId, int delay, String clientId, long sequenceNumber, StreamObserver<CreateWalletResponse> observer) {
        // 1. Criar request sem assinatura
        CreateWalletRequest request = CreateWalletRequest.newBuilder()
                .setUserId(userId)
                .setWalletId(walletId)
                .setClientId(clientId)
                .setSequenceNumber(sequenceNumber)
                .build();

        // 2. Construir Signature
        Signature signature = buildSignature(request.toByteArray(), userId);

        // 3. Adicionar Signature ao request
        CreateWalletRequest signedRequest = request.toBuilder()
                .setSignature(signature)
                .build();
        
        attachDelay(asyncStub, delay).createWallet(signedRequest, observer);
    }

    public void deleteWalletAsync(String userId, String walletId, int delay, String clientId, long sequenceNumber, StreamObserver<DeleteWalletResponse> observer) {
        // 1. Criar request sem assinatura
        DeleteWalletRequest request = DeleteWalletRequest.newBuilder()
                .setUserId(userId)
                .setWalletId(walletId)
                .setClientId(clientId)
                .setSequenceNumber(sequenceNumber)
                .build();

        // 2. Construir Signature
        Signature signature = buildSignature(request.toByteArray(), userId);

        // 3. Adicionar Signature ao request
        DeleteWalletRequest signedRequest = request.toBuilder()
                    .setSignature(signature)
                    .build();
        
        attachDelay(asyncStub, delay).deleteWallet(signedRequest, observer);
    }

    public void readBalanceAsync(String walletId, int delay, StreamObserver<ReadBalanceResponse> observer) {
        ReadBalanceRequest request = ReadBalanceRequest.newBuilder()
                .setWalletId(walletId)
                .build();
        
        attachDelay(asyncStub, delay).readBalance(request, observer);
    }

    public void transferAsync(String srcUserId, String srcWalletId, String dstWalletId, long amount, int delay, String clientId, long sequenceNumber, StreamObserver<TransferResponse> observer) {
        // 1. Criar request sem assinatura
        TransferRequest request = TransferRequest.newBuilder()
                .setSrcUserId(srcUserId)
                .setSrcWalletId(srcWalletId)
                .setDstWalletId(dstWalletId)
                .setValue(amount)
                .setClientId(clientId)
                .setSequenceNumber(sequenceNumber)
                .build();

        // 2. Construir Signature
        Signature signature = buildSignature(request.toByteArray(), srcUserId);

        // 3. Adicionar Signature ao request
        TransferRequest signedRequest = request.toBuilder()
                    .setSignature(signature)
                    .build();
        
        attachDelay(asyncStub, delay).transfer(signedRequest, observer);
    }
    
    public GetBlockchainStateResponse getBlockchainState() {
        GetBlockchainStateRequest request = GetBlockchainStateRequest.newBuilder().build();
        return blockingStub.getBlockchainState(request);
    }

    public void close() {
        channel.shutdown();
    }
}
