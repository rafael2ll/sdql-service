package sd.nosql.prototype.service.impl;

import sd.nosql.prototype.Record;
import sd.nosql.prototype.enums.Operation;
import sd.nosql.prototype.request.QueueRequest;
import sd.nosql.prototype.service.PersistenceService;
import sd.nosql.prototype.service.QueueService;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class QueueServiceImpl implements QueueService {
    private Semaphore semaphore = new Semaphore(1, true);
    private PersistenceService persistenceService = new FilePersistenceServiceImpl();
    private LinkedBlockingQueue<QueueRequest> queue = new LinkedBlockingQueue<>();

    @Override
    public void produce(QueueRequest request) throws InterruptedException {
        if (enterCriticalZone()) {
            try { queue.add(request); }
            finally { leaveCriticalZone(); }
        }
    }

    @Override
    public void consumeAll() throws InterruptedException {
        if (enterCriticalZone()) {
            try {
                ConcurrentHashMap<Long, Record> database = persistenceService.read();
                while(!queue.isEmpty()) {
                    QueueRequest request = queue.remove();
                    consumeRequest(request, database);
                }
                persistenceService.write(database);
            } finally {
                leaveCriticalZone();
            }
        }
    }

    private boolean enterCriticalZone() throws InterruptedException {
        return semaphore.tryAcquire(10, TimeUnit.SECONDS);
    }

    private void leaveCriticalZone() {
        semaphore.release();
    }

    private void consumeRequest(QueueRequest request, ConcurrentHashMap<Long, Record> database) {
        Operation operation = request.getOperation();
        Long key = request.getKey();
        if (operation == Operation.SET || operation == Operation.TEST_SET) {
            Record record = request.getRecord();
            database.put(key, record);
        } else if (operation == Operation.DEL || operation == Operation.DEL_VERSION) {
            database.remove(key);
        }
    }
}