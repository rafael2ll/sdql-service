package sd.nosql.prototype.service.impl;

import sd.nosql.prototype.Record;
import sd.nosql.prototype.enums.Operation;
import sd.nosql.prototype.request.QueueRequest;
import sd.nosql.prototype.service.QueueService;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class QueueServiceImpl implements QueueService {
    // TODO: Call the abstraction instead of impl
    private Semaphore semaphore = new Semaphore(1, true);
    private FilePersistenceServiceImpl filePersistenceService = new FilePersistenceServiceImpl();
    private LinkedBlockingQueue<QueueRequest> queue = new LinkedBlockingQueue<>();

    @Override
    public void produce(QueueRequest request) throws InterruptedException {
        if (enterCriticalZone()) {
            queue.add(request);
            leaveCriticalZone();
        }
    }

    @Override
    public void consumeAll() throws InterruptedException {
        if (enterCriticalZone()) {
            ConcurrentHashMap<Long, Record> database = filePersistenceService.read();
            while(!queue.isEmpty()) {
                QueueRequest request = queue.remove();
                Operation operation = request.getOperation();
                Long key = request.getKey();
                if (operation == Operation.SET || operation == Operation.TEST_SET) {
                    Record record = request.getRecord();
                    database.put(key, record);
                } else if (operation == Operation.DEL || operation == Operation.DEL_VERSION) {
                    database.remove(key);
                }
            }
            filePersistenceService.write(database);
            leaveCriticalZone();
        }
    }

    private boolean enterCriticalZone() throws InterruptedException {
        return semaphore.tryAcquire(10, TimeUnit.SECONDS);
    }

    private void leaveCriticalZone() {
        semaphore.release();
    }
}