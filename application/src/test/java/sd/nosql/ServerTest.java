package sd.nosql;


import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sd.nosql.prototype.Record;
import sd.nosql.prototype.*;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class ServerTest {
    private static final Logger logger = LoggerFactory.getLogger(ServerTest.class);
    private DatabaseServiceGrpc.DatabaseServiceBlockingStub stub;

    @BeforeEach
    void init() {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8080)
                .usePlaintext()
                .build();

        logger.info("Channel: {}", channel);
        stub = DatabaseServiceGrpc.newBlockingStub(channel);
    }

    @Test
    void write_parallel_100000_with_multiple_clients_successful() {
        List<Client> managedChannelCircular = IntStream.range(0, 5).mapToObj(i -> new Client(i, ManagedChannelBuilder.forAddress("localhost", 8080)
                .usePlaintext()
                .build())).collect(Collectors.toList());

        LongStream.range(0L, 100000L).parallel().forEach(number -> {
            Client client = managedChannelCircular.get((int) number % 5);
            RecordResult resultInsert = DatabaseServiceGrpc.newBlockingStub(client.getManagedChannel()).set(RecordInput.newBuilder()
                    .setKey(number)
                    .setRecord(Record.newBuilder()
                            .setTimestamp(System.currentTimeMillis())
                            .setData(ByteString.copyFrom(String.format("{\"message\": \" We wont find ourself until we're lost\", \"time\": %d }", number), StandardCharsets.UTF_8))
                            .build())
                    .build());
            client.upCount();
            assert resultInsert.getResultType().equals(ResultType.SUCCESS);
        });
        logger.info("Done");
    }

    @Test
    void write_parallel_100000_successful() {
        LongStream.range(0L, 100000L).parallel().forEach(number -> {
            RecordResult resultInsert = stub.set(RecordInput.newBuilder()
                    .setKey(number)
                    .setRecord(Record.newBuilder()
                            .setTimestamp(System.currentTimeMillis())
                            .setData(ByteString.copyFrom(String.format("{\"message\": \" We wont find ourself until we're lost\", \"time\": %d }", number), StandardCharsets.UTF_8))
                            .build())
                    .build());
            assert resultInsert.getResultType().equals(ResultType.ERROR);
        });
    }

    @Test
    void update_all_10000_in_sequence() {
        LongStream.range(0L, 100000L).parallel().forEach(number -> {
            RecordResult result = stub.get(Key.newBuilder().setKey(number).build());
            assert result.getResultType().equals(ResultType.SUCCESS);
        });
    }

    @Test
    void read_all_10000_in_sequence() {
        LongStream.range(0L, 100000L).parallel().forEach(number -> {
            RecordResult result = stub.get(Key.newBuilder().setKey(number).build());
            assert result.getResultType().equals(ResultType.SUCCESS);
        });
    }

    class Client {
        private int id;
        private ManagedChannel managedChannel;
        private int count;

        public Client(int id, ManagedChannel managedChannel) {
            this.id = id;
            this.managedChannel = managedChannel;
        }

        public ManagedChannel getManagedChannel() {
            return managedChannel;
        }


        public int getCount() {
            return count;
        }

        public void upCount() {
            this.count++;
        }
    }
}