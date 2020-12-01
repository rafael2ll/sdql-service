package sd.nosql;


import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class ServerTest {
    private static final Logger logger = LoggerFactory.getLogger(ServerTest.class);
    private DatabaseServiceGrpc.DatabaseServiceBlockingStub blockingStub;
    private DatabaseServiceGrpc.DatabaseServiceFutureStub asyncStub;

    @BeforeEach
    void init() {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8080)
                .usePlaintext()
                .build();

        logger.info("Channel: {}", channel);
        blockingStub = DatabaseServiceGrpc.newBlockingStub(channel);
        asyncStub = DatabaseServiceGrpc.newFutureStub(channel);
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
                            .setData(ByteString.copyFrom(String.format("{\"message\": \" To every dream that I left behind....counting\", \"time\": %d }", number), StandardCharsets.UTF_8))
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
            RecordResult resultInsert = blockingStub.set(RecordInput.newBuilder()
                    .setKey(number)
                    .setRecord(Record.newBuilder()
                            .setTimestamp(System.currentTimeMillis())
                            .setData(ByteString.copyFrom(String.format("{\"message\": \" To every dream that I left behind....counting\", \"time\": %d }", number), StandardCharsets.UTF_8))
                            .build())
                    .build());
            assert resultInsert.getResultType().equals(ResultType.ERROR);
        });
    }

    @Test
    void write_parallel_100000_async_successful() throws InterruptedException {
        AtomicInteger count = new AtomicInteger();
        LongStream.range(0L, 100000L).parallel().forEach(number -> {
            var asyncResult = asyncStub.set(RecordInput.newBuilder()
                    .setKey(number)
                    .setRecord(Record.newBuilder()
                            .setTimestamp(System.currentTimeMillis())
                            .setData(ByteString.copyFrom(String.format("{\"message\": \" To every dream that I left behind....counting\", \"time\": %d }", number), StandardCharsets.UTF_8))
                            .build())
                    .build());
            Futures.addCallback(asyncResult, new FutureCallback<>() {
                @Override
                public void onSuccess(RecordResult recordResult) {
                    count.getAndIncrement();
                    assert recordResult.getResultType().equals(ResultType.SUCCESS);
                }

                @Override
                public void onFailure(Throwable throwable) {
                    count.getAndIncrement();
                }
            }, MoreExecutors.directExecutor());
        });
        while (count.get() < 100000) {
            logger.info("Count: {}", count.get());
            Thread.sleep(1000);
        }
    }

    @Test
    void update_all_10000_in_sequence() {
        LongStream.range(0L, 100000L).parallel().forEach(number -> {
            RecordResult result = blockingStub.testAndSet(RecordUpdate.newBuilder()
                    .setOldVersion(1)
                    .setKey(number)
                    .setRecord(Record.newBuilder()
                            .setTimestamp(System.currentTimeMillis())
                            .setData(ByteString.copyFrom(String.format("{\"message\": \" To every dream that I left behind new version....counting\", \"time\": %d }", number), StandardCharsets.UTF_8))
                            .build())
                    .build());
            assert result.getResultType().equals(ResultType.SUCCESS);
        });
    }

    @Test
    void read_all_10000_in_sequence() {
        LongStream.range(0L, 100000L).parallel().forEach(number -> {
            RecordResult result = blockingStub.get(Key.newBuilder().setKey(number).build());
            if (number % 1000 == 0) logger.info("Result: {}", result);
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