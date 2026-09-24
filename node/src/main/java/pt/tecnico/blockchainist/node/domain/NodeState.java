package pt.tecnico.blockchainist.node.domain;

import pt.tecnico.blockchainist.contract.*;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class NodeState {
    	
	private static class Wallet {
        String ownerId;
        long balance;

        Wallet(String ownerId, long balance) {
            this.ownerId = ownerId;
            this.balance = balance;
        }
    }
	
    private Map<String, Wallet> wallets = new ConcurrentHashMap<>();
    private List<Transaction> ledger = new CopyOnWriteArrayList<>();
    private List<Block> blockchain = new CopyOnWriteArrayList<>();
    private Map<String, Long> sequenceNumberPerClient = new ConcurrentHashMap<>();
    // Set to avoid double execution of causal transfers
    private Set<String> causalTransfersExecuted = ConcurrentHashMap.newKeySet();

    // Static user map to organization
    // The use of Map.ofEntries assumes that no other User is to be inserted while Node is running
    private static final Map<String, String> userOrgMap = Map.ofEntries(
        Map.entry("BC", "OrgA"),
        Map.entry("Alice", "OrgA"),
        Map.entry("Bob", "OrgA"),
        Map.entry("Charlie", "OrgA"),
        Map.entry("David", "OrgB"),
        Map.entry("Emma", "OrgB"),
        Map.entry("Fred", "OrgB"),
        Map.entry("Ginger", "OrgC"),
        Map.entry("Henry", "OrgC"),
        Map.entry("Iris", "OrgC")
    );

    private String nodeOrganization;
	
    public NodeState(String organization) {
        this.nodeOrganization = organization;
        wallets.put("bc", new Wallet("BC", 1000L));
    }

    public void validateUserOrganization(String userId) throws Exception {
        String organization = userOrgMap.get(userId);
        if (organization == null || organization.equals(nodeOrganization) == false) {
            throw new Exception("User " + userId + " does not belong to node organization " + nodeOrganization);
        }
    }

    // Applies block received from sequencer
    public synchronized void applyBlock(Block block) {
        for (Transaction transaction : block.getTransactionsList()) {
        	String clientId = transaction.getClientId();
        	long seqNum = transaction.getSequenceNumber();
            String transferId = clientId + "-" + seqNum;
        	
        	// Verify if already processed this transaction or other one with superior number
            if (sequenceNumberPerClient.containsKey(clientId) && sequenceNumberPerClient.get(clientId) >= seqNum) {
                continue; 
            }
            sequenceNumberPerClient.put(clientId, seqNum);

            try {
                if (transaction.hasCreateWallet()) {
                    createWallet(transaction.getCreateWallet().getUserId(), transaction.getCreateWallet().getWalletId());
                }
                
                if (transaction.hasDeleteWallet()) {
                    deleteWallet(transaction.getDeleteWallet().getUserId(), transaction.getDeleteWallet().getWalletId());
                }
                
                if (transaction.hasTransfer()) {
                    if (causalTransfersExecuted.contains(transferId) == false) {
                        transfer(transaction.getTransfer().getSrcUserId(), transaction.getTransfer().getSrcWalletId(), 
                                transaction.getTransfer().getDstWalletId(), transaction.getTransfer().getValue());
                    }
                    else {
                        causalTransfersExecuted.remove(transferId);
                    } 
                }
                
            } catch (Exception e) {
                // In case of error, transaction is ignored and node continues reading Block
                System.err.println("Error applying transaction: " + e.getMessage());
            }
            
            ledger.add(transaction);
        }
        
        blockchain.add(block);
    }

    public synchronized void createWallet(String userId, String walletId) throws Exception {
    	if (wallets.containsKey(walletId)) {
            throw new Exception("Wallet already exists with ID: " + walletId);
        }
        wallets.put(walletId, new Wallet(userId, 0L));
    }

    public synchronized void deleteWallet(String userId, String walletId) throws Exception {
    	Wallet wallet = wallets.get(walletId);
        if (wallet == null) throw new Exception("Wallet does not exist.");
        if (wallet.balance != 0) throw new Exception("Balance not zero.");
        if (!wallet.ownerId.equals(userId)) throw new Exception("User is not owner.");
        
        wallets.remove(walletId);
    }

    public synchronized void transfer(String srcUserId, String srcWalletId, String dstWalletId, Long amount) throws Exception{
    	Wallet src = wallets.get(srcWalletId);
        Wallet dst = wallets.get(dstWalletId);

        if (src == null) throw new Exception("srcWallet does not exist.");
        if (dst == null) throw new Exception("dstWallet does not exist.");
        if (!src.ownerId.equals(srcUserId)) throw new Exception("User is not owner of srcWallet.");
        if (src.balance < amount) throw new Exception("Insufficient balance.");
        if (amount <= 0) throw new Exception("Amount must be positive.");

        src.balance -= amount;
        dst.balance += amount;
    }

    // Auxiliary function for causal transfer
    // Needs to be separated because it is added before applyBlock
    public void addCausalTransferExecuted(String clientId, long seqNum) {
        causalTransfersExecuted.add(clientId + "-" + seqNum);
    }

    public long readBalance(String walletId) throws Exception {
    	Wallet wallet = wallets.get(walletId);
        if (wallet == null) throw new Exception("Wallet not found.");
        return wallet.balance;
    }

    public boolean isTransactionExecuted(Transaction transaction) {
    	if (ledger.contains(transaction)) return true;
    	else return false;
    }

    public List<Transaction> getBlockchainState() {
        return ledger;
    }

}
