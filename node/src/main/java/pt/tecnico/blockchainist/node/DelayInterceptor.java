package pt.tecnico.blockchainist.node;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Context;
import io.grpc.Contexts;

public class DelayInterceptor implements ServerInterceptor {
	private static final Metadata.Key<String> DELAY_HEADER =
		    Metadata.Key.of("delay", Metadata.ASCII_STRING_MARSHALLER);

	public static final Context.Key<Integer> DELAY_KEY = Context.key("delay");
	
    @Override
    public <RequestT, ResponseT> ServerCall.Listener<RequestT> interceptCall(
            ServerCall<RequestT, ResponseT> call,
            Metadata headers,
            ServerCallHandler<RequestT, ResponseT> next) {
        
        // Tries to read the key sent by client in the HEADERS
        String delayValue = headers.get(DELAY_HEADER);
        int delay = 0;
        if (delayValue != null) {
            try {
                delay = Integer.parseInt(delayValue);
            } catch (NumberFormatException e) {
                delay = 0;
            }
        }

        Context context = Context.current().withValue(DELAY_KEY, delay);
        return Contexts.interceptCall(context, call, headers, next);
    }
}
