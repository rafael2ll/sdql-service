package sd.nosql.prototype.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sd.nosql.prototype.Record;
import sd.nosql.prototype.enums.Operation;
import sd.nosql.prototype.request.QueueRequest;
import sd.nosql.prototype.service.PersistenceService;
import sd.nosql.prototype.service.QueueService;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class QueueServiceImpl implements QueueService {
    private static final int TIMEOUT = 10;
    private static final Logger logger = LoggerFactory.getLogger(DatabaseServiceImpl.class);
    private Semaphore firstSemaphore = new Semaphore(1, true);
    private Semaphore secondSemaphore = new Semaphore(1, true);
    private PersistenceService persistenceService = new FilePersistenceServiceImpl();
    private LinkedBlockingQueue<QueueRequest> firstQueue = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<QueueRequest> secondQueue = new LinkedBlockingQueue<>();

    @Override
    public void produce(QueueRequest request) throws InterruptedException {
        if (enterFirstCriticalZone()) {
            try { firstQueue.add(request); }
            finally { leaveFirstCriticalZone(); }
        } else if (enterSecondCriticalZone()) {
            try { secondQueue.add(request); }
            finally { leaveSecondCriticalZone(); }
        }
    }

    @Override
    public void consumeAll() throws InterruptedException {
        if (enterFirstCriticalZone()) {
            try {
                copyToDisk();
                if (enterSecondCriticalZone()) { copyFromSecondQueue(); }
            } finally {
                leaveFirstCriticalZone();
                leaveSecondCriticalZone();
            }
        }
    }

    @Override
    public void scheduleConsumer(int persistenceTimeInMs) {
        Timer timer = new Timer();
        TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
                try {
                    logger.info("Consuming queue requests...");
                    consumeAll();
                }
                catch (Exception e) {
                    logger.info("Error on consuming queue requests", e);
                }
            }
        };
        timer.schedule(timerTask, new Date(), persistenceTimeInMs);
    }

    private boolean enterFirstCriticalZone() throws InterruptedException {
        return firstSemaphore.tryAcquire(TIMEOUT, TimeUnit.SECONDS);
    }

    private boolean enterSecondCriticalZone() throws InterruptedException {
        return secondSemaphore.tryAcquire(TIMEOUT, TimeUnit.SECONDS);
    }

    private void leaveFirstCriticalZone() {
        firstSemaphore.release();
    }

    private void leaveSecondCriticalZone() {
        secondSemaphore.release();
    }

    private void copyToDisk() {
        ConcurrentHashMap<Long, Record> database = persistenceService.read();
        while(!firstQueue.isEmpty()) {
            QueueRequest request = firstQueue.remove();
            consumeRequest(request, database);
        }
        persistenceService.write(database);
    }

    private void copyFromSecondQueue() {
        while(!secondQueue.isEmpty()) {
            QueueRequest request = secondQueue.remove();
            firstQueue.add(request);
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