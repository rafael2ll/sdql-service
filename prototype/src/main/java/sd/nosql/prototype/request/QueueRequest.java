package sd.nosql.prototype.request;

import sd.nosql.prototype.Record;
import sd.nosql.prototype.enums.Operation;

public class QueueRequest {
    Operation operation;
    Long key;
    Record record;

    public QueueRequest(Operation operation, Long key, Record record) {
        this.operation = operation;
        this.key = key;
        this.record = record;
    }

    public Operation getOperation() {
        return operation;
    }

    public Long getKey() {
        return key;
    }

    public Record getRecord() {
        return record;
    }
}
