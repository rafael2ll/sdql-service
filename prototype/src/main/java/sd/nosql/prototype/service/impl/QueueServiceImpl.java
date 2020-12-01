package sd.nosql.prototype.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sd.nosql.prototype.Record;
import sd.nosql.prototype.enums.Operation;
import sd.nosql.prototype.exception.QueueTimeoutException;
import sd.nosql.prototype.request.QueueRequest;
import sd.nosql.prototype.service.PersistenceService;
import sd.nosql.prototype.service.QueueService;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class QueueServiceImpl implements QueueService {
    private static final Logger logger = LoggerFactory.getLogger(QueueServiceImpl.class);

    private static final int TIMEOUT = 10;
    private final Semaphore firstSemaphore = new Semaphore(1, true);
    private final Semaphore secondSemaphore = new Semaphore(1, true);
    private PersistenceService persistenceService = new FilePersistenceServiceImpl();
    private final LinkedBlockingQueue<QueueRequest> firstQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<QueueRequest> secondQueue = new LinkedBlockingQueue<>();

    @Override
    public void setPersistenceService(PersistenceService persistenceService) {
        this.persistenceService = persistenceService;
    }

    @Override
    public void produce(QueueRequest request) throws InterruptedException {
        if (enterCriticalZone(firstSemaphore)) {
            try {
                firstQueue.offer(request);
            } finally {
                leaveCriticalZone(firstSemaphore);
            }
        } else if (enterCriticalZone(secondSemaphore)) {
            try {
                secondQueue.offer(request);
            } finally {
                leaveCriticalZone(secondSemaphore);
            }
        } else {
            throw new QueueTimeoutException("Produce timeout exception");
        }
    }

    @Override
    public void consumeAll() throws InterruptedException {
        copyToDisk(secondQueue, secondSemaphore);
        copyToDisk(firstQueue, firstSemaphore);
    }

    private boolean enterCriticalZone(Semaphore semaphore) throws InterruptedException {
        return semaphore.tryAcquire(TIMEOUT, TimeUnit.SECONDS);
    }

    private void leaveCriticalZone(Semaphore semaphore) {
        semaphore.release();
    }

    private void copyToDisk(LinkedBlockingQueue<QueueRequest> queue, Semaphore semaphore) throws InterruptedException {
        if (enterCriticalZone(semaphore)) {
            ConcurrentHashMap<Long, Record> database = persistenceService.read();
            try {
                while (!queue.isEmpty()) {
                    QueueRequest request = queue.remove();
                    consumeRequest(request, database);
                }
            } finally {
                persistenceService.write(database);
                leaveCriticalZone(semaphore);
                logger.info("Dumped database....");
            }
        } else {
            throw new QueueTimeoutException("Copy to disk timeout exception");
        }
    }

    private void copyFromSecondQueue() throws InterruptedException {
        if (enterCriticalZone(secondSemaphore)) {
            while (!secondQueue.isEmpty()) {
                try {
                    QueueRequest request = secondQueue.remove();
                    firstQueue.add(request);
                } finally {
                    leaveCriticalZone(secondSemaphore);
                    logger.info("Queue re-synced");
                }
            }
        } else {
            throw new QueueTimeoutException("Copy from second queue timeout exception");
        }
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