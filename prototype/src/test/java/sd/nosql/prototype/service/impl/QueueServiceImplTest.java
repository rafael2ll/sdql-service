package sd.nosql.prototype.service.impl;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sd.nosql.prototype.Record;
import sd.nosql.prototype.enums.Operation;
import sd.nosql.prototype.request.QueueRequest;
import sd.nosql.prototype.service.PersistenceService;
import sd.nosql.prototype.service.QueueService;

import java.text.MessageFormat;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

/*
===================================================================================================
            1. Firstly, create a folder inside module prototype, called database
            2. Inside the database folder, create a folder called data
            3. Now you're read to go
            4.Before each test delete all the content inside the folder prototype/database and recreate the folder data

  - Folder structure:
   | -> prototype
      |-> src
      |-> database
        |-> data
          |-> ...backup files automatically created
        |-> version.db // Also programmatically created
===================================================================================================
*/
class QueueServiceImplTest {
    QueueService service;
    PersistenceService persistenceService = new FilePersistenceServiceImpl();

    @BeforeEach
    void init() {
        service = new QueueServiceImpl();
        service.setPersistenceService(persistenceService);
        service.scheduleConsumer(5000);
    }

    @Test
    void write_many_records_while_dumping_expecting_success() throws Exception {
        var i = 0;
        var limit = 1e5;
        do {
            service.produce(new QueueRequest(Operation.SET, (long) i, Record.newBuilder().setTimestamp(System.currentTimeMillis()).setData(ByteString.copyFromUtf8("i=" + i)).build()));
            i++;
        } while (i < limit);
        Thread.sleep(5000);
        System.out.println(persistenceService.read().size());
        System.out.println(i);
        assert persistenceService.read().size() == limit;
    }

    @Test
    void write_and_edit_while_dumping_expecting_success() throws Exception {
        var i = 0;
        var limit = 1e6;
        var updated_text = "i=%d and divisible by 5";
        do {
            service.produce(new QueueRequest(Operation.SET, (long) i, Record.newBuilder().setTimestamp(System.currentTimeMillis()).setData(ByteString.copyFromUtf8("i=" + i)).build()));
            if (i % 5 == 0) {
                service.produce(new QueueRequest(Operation.TEST_SET, (long) i, Record.newBuilder().setTimestamp(System.currentTimeMillis()).setVersion(2).setData(ByteString.copyFromUtf8(String.format(updated_text, i))).build()));
            }
            i++;
        } while (i < limit);
        Thread.sleep(5000);
        System.out.println(persistenceService.read().size());
        System.out.println(i);

        assert persistenceService.read().size() == limit;
        assert persistenceService.read().entrySet().stream().filter((value) -> (value.getKey() % 5) == 0).allMatch(v -> v.getValue().getVersion() == 2 && v.getValue().getData().endsWith(ByteString.copyFromUtf8("by 5")));
    }

    @Test
    void write_and_edit_parallel_while_dumping_expecting_success() throws Exception {
        var min = 0;
        var limit = 1e6;
        var updated_text = "i=%d and divisible by 5";
        IntStream.range(min, (int) limit).parallel().forEach(i -> {
            try {
                service.produce(new QueueRequest(Operation.SET, (long) i, Record.newBuilder().setTimestamp(System.currentTimeMillis()).setData(ByteString.copyFromUtf8("i=" + i)).build()));
                if (i % 5 == 0) {
                    service.produce(new QueueRequest(Operation.TEST_SET, (long) i, Record.newBuilder().setTimestamp(System.currentTimeMillis()).setVersion(2).setData(ByteString.copyFromUtf8(String.format(updated_text, i))).build()));
                }
            } catch (InterruptedException e) {
                System.out.println(e.toString());
            }
        });

        Thread.sleep(5000);
        System.out.println(persistenceService.read().size());
        System.out.println(limit);

        assert persistenceService.read().size() == limit;
        assert persistenceService.read().entrySet().stream().filter((value) -> (value.getKey() % 5) == 0).allMatch(v -> v.getValue().getVersion() == 2 && v.getValue().getData().endsWith(ByteString.copyFromUtf8("by 5")));
    }

    @Test
    void write_and_edit_and_deleting_parallel_while_dumping_expecting_success() throws Exception {
        var min = 0;
        var limit = 1e6;
        var updated_text = "i=%d and divisible by 5";
        IntStream.range(min, (int) limit).parallel().forEach(i -> {
            try {
                service.produce(new QueueRequest(Operation.SET, (long) i, Record.newBuilder().setTimestamp(System.currentTimeMillis()).setData(ByteString.copyFromUtf8("i=" + i)).build()));
                if (i % 5 == 0) {
                    service.produce(new QueueRequest(Operation.TEST_SET, (long) i, Record.newBuilder().setTimestamp(System.currentTimeMillis()).setVersion(2).setData(ByteString.copyFromUtf8(String.format(updated_text, i))).build()));
                }
                if (i % 10 == 0) {
                    service.produce(new QueueRequest(Operation.DEL_VERSION, (long) i, Record.newBuilder().setTimestamp(System.currentTimeMillis()).setVersion(2).setData(ByteString.copyFromUtf8(String.format(updated_text, i))).build()));
                }
            } catch (InterruptedException e) {
                System.out.println(e.toString());
            }
        });

        Thread.sleep(5000);
        System.out.println(persistenceService.read().size());
        System.out.println(limit);

        assert persistenceService.read().size() == limit - limit / 10;
        assert persistenceService.read().entrySet().stream().filter((value) -> (value.getKey() % 5) == 0).allMatch(v -> v.getValue().getVersion() == 2 && v.getValue().getData().endsWith(ByteString.copyFromUtf8("by 5")));
    }

    @Test
    void write_and_edit_and_deleting_parallel_showing_awaiting_time_while_dumping_expecting_success() throws Exception {
        var maxWaitingTime = new AtomicLong();
        var min = 0;
        var limit = 1e6;
        var updated_text = "i=%d and divisible by 5";
        IntStream.range(min, (int) limit).parallel().forEach(i -> {
            try {
                var initTime = System.currentTimeMillis();
                service.produce(new QueueRequest(Operation.SET, (long) i, Record.newBuilder().setTimestamp(System.currentTimeMillis()).setData(ByteString.copyFromUtf8("i=" + i)).build()));
                var wait = System.currentTimeMillis() - initTime;
                if (maxWaitingTime.get() < wait) maxWaitingTime.set(wait);
                if (i % 5 == 0) {
                    initTime = System.currentTimeMillis();
                    service.produce(new QueueRequest(Operation.TEST_SET, (long) i, Record.newBuilder().setTimestamp(System.currentTimeMillis()).setVersion(2).setData(ByteString.copyFromUtf8(String.format(updated_text, i))).build()));
                    wait = System.currentTimeMillis() - initTime;
                    if (maxWaitingTime.get() < wait) maxWaitingTime.set(wait);

                }
                if (i % 10 == 0) {
                    initTime = System.currentTimeMillis();
                    service.produce(new QueueRequest(Operation.DEL_VERSION, (long) i, Record.newBuilder().setTimestamp(System.currentTimeMillis()).setVersion(2).setData(ByteString.copyFromUtf8(String.format(updated_text, i))).build()));
                    wait = System.currentTimeMillis() - initTime;
                    if (maxWaitingTime.get() < wait) maxWaitingTime.set(wait);
                }
            } catch (InterruptedException e) {
                System.out.println(e.toString());
            }
        });

        Thread.sleep(5000);
        System.out.println(persistenceService.read().size());
        System.out.println(limit);

        assert persistenceService.read().size() == limit - limit / 10;
        assert persistenceService.read().entrySet().stream().filter((value) -> (value.getKey() % 5) == 0).allMatch(v -> v.getValue().getVersion() == 2 && v.getValue().getData().endsWith(ByteString.copyFromUtf8("by 5")));
        System.out.println(MessageFormat.format("Max Waited Time: {0}", maxWaitingTime));
    }

}