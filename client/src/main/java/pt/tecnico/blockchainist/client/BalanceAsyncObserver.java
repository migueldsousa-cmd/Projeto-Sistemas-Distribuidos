package pt.tecnico.blockchainist.client;

import io.grpc.stub.StreamObserver;
import pt.tecnico.blockchainist.contract.ReadBalanceResponse;

public class BalanceAsyncObserver implements StreamObserver<ReadBalanceResponse> {
    private long commandNumber;

    public BalanceAsyncObserver(long commandNumber) {
        this.commandNumber = commandNumber;
    }

    @Override
    public void onNext(ReadBalanceResponse response) {
        System.out.println(commandNumber + " " + response.getBalance());
        System.out.print("> ");
    }

    @Override
    public void onError(Throwable throwable) {
        System.err.println(commandNumber + " " + throwable.getMessage());
        System.out.print("> ");
    }

    @Override
    public void onCompleted() {
    	// ????
    }
}
