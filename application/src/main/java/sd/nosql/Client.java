package sd.nosql;

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

    public static void main(String[] args) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8080)
                .usePlaintext()
                .build();

        logger.info("Channel: {}", channel);

        DatabaseServiceGrpc.DatabaseServiceBlockingStub stub = DatabaseServiceGrpc.newBlockingStub(channel);

        RecordResult resultInsert = stub.set(RecordInput.newBuilder()
                .setKey(1606078612219L)
                .setRecord(Record.newBuilder()
                        .setTimestamp(System.currentTimeMillis())
                        .setData(ByteString.copyFrom("{\"message\": \" Some message\"}", StandardCharsets.UTF_8))
                        .build())
                .build());

        RecordResult resultUpdate = stub.testAndSet(
                RecordUpdate.newBuilder()
                        .setOldVersion(1)
                        .setKey(1606078612219L)
                        .setRecord(Record.newBuilder()
                                .setTimestamp(System.currentTimeMillis())
                                .setData(ByteString.copyFrom("{\"message\": \" Some message\"}", StandardCharsets.UTF_8))
                                .build())
                        .build());

        logger.info("Updated Result: {}", resultUpdate);
        logger.info("Current Record: {}", stub.get(Key.newBuilder().setKey(1606078612219L).build()));
        channel.shutdown();
    }
}
