package sd.nosql.prototype.service;

import sd.nosql.prototype.request.QueueRequest;

public interface QueueService {
    void produce(QueueRequest request) throws InterruptedException;
    void consumeAll() throws InterruptedException;
}
