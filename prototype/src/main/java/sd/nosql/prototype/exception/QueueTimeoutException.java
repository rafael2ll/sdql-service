package sd.nosql.prototype.exception;

public class QueueTimeoutException extends RuntimeException {
    public QueueTimeoutException(String message) {
        super(message);
    }
}
