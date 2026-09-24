package pt.tecnico.blockchainist.client;

import io.grpc.stub.StreamObserver;

public class AsyncObserver<R> implements StreamObserver<R>{
	private long commandNumber;

    public AsyncObserver(long commandNumber) {
        this.commandNumber = commandNumber;
    }
	
	@Override
    public void onNext(R r) {
        System.out.println(commandNumber + " OK\n");
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
