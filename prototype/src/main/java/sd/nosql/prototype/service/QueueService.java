package sd.nosql.prototype.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sd.nosql.prototype.request.QueueRequest;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

public interface QueueService {
    void produce(QueueRequest request) throws InterruptedException;
    void consumeAll() throws InterruptedException;
    void setPersistenceService(PersistenceService persistenceService);
    default void scheduleConsumer(int persistenceTimeInMs) {
        final Logger logger = LoggerFactory.getLogger(QueueService.class);
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
}
