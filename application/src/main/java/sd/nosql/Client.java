package sd.nosql;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sd.nosql.prototype.Record;
import sd.nosql.prototype.*;

import java.nio.charset.StandardCharsets;


public class Client {
    private static final Logger logger = LoggerFactory.getLogger(Client.class);

    public static void main(String[] args) throws InterruptedException {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8080)
                .usePlaintext()
                .build();

        logger.info("Channel: {}", channel);

        DatabaseServiceGrpc.DatabaseServiceFutureStub stub = DatabaseServiceGrpc.newFutureStub(channel);

        stub.set(RecordInput.newBuilder()
                .setKey(1606078612219L)
                .setRecord(Record.newBuilder()
                        .setTimestamp(System.currentTimeMillis())
                        .setData(ByteString.copyFrom("{\"message\": \" Some message\"}", StandardCharsets.UTF_8))
                        .build())
                .build());

        var last = stub.testAndSet(
                RecordUpdate.newBuilder()
                        .setOldVersion(1)
                        .setKey(1606078612219L)
                        .setRecord(Record.newBuilder()
                                .setTimestamp(System.currentTimeMillis())
                                .setData(ByteString.copyFrom("{\"message\": \" Some message\"}", StandardCharsets.UTF_8))
                                .build())
                        .build());

        last.addListener(()-> logger.info("Gotten"), MoreExecutors.directExecutor());
        Futures.addCallback(last, new FutureCallback<>() {
            @Override
            public void onSuccess(RecordResult recordResult) {
                    logger.info("Result: {}", recordResult);
            }

            @Override
            public void onFailure(Throwable throwable) {

            }
        }, MoreExecutors.directExecutor());
        while (true) {
            if (last.isDone())
                break;
            Thread.sleep(100);
        }
        logger.info("Current Record: {}", stub.get(Key.newBuilder().setKey(1606078612219L).build()));
        channel.shutdown();
    }
}
