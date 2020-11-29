package sd.nosql.prototype.service;

import sd.nosql.prototype.Record;

import java.util.concurrent.ConcurrentHashMap;

public interface PersistenceService {
    ConcurrentHashMap<Long, Record> read();
    void write(ConcurrentHashMap<Long, Record> database);
}
