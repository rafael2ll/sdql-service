package sd.nosql.prototype.service.impl;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sd.nosql.prototype.Record;
import sd.nosql.prototype.*;
import sd.nosql.prototype.enums.Operation;
import sd.nosql.prototype.request.QueueRequest;
import sd.nosql.prototype.service.PersistenceService;
import sd.nosql.prototype.service.QueueService;

import java.util.Map;
import java.util.Optional;

public class DatabaseServiceImpl extends DatabaseServiceGrpc.DatabaseServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseServiceImpl.class);
    private final QueueService queueService = new QueueServiceImpl();
    Map<Long, Record> database;

    public DatabaseServiceImpl(int persistenceTimeInMs) {
        PersistenceService persistenceService = new FilePersistenceServiceImpl();
        database = persistenceService.read();
        queueService.setPersistenceService(persistenceService);
        queueService.scheduleConsumer(persistenceTimeInMs);
    }


    @Override
    public void set(RecordInput request, StreamObserver<RecordResult> responseObserver) {
        int times = 0;
        try {
            logger.info("set::{}", request);
            if (!database.containsKey(request.getKey())) {
                Record record = request.getRecord().toBuilder().setVersion(1).build();
                database.put(request.getKey(), record);
                setResponse(responseObserver, ResultType.SUCCESS, null);
                queueService.produce(new QueueRequest(Operation.SET, request.getKey(), record));
            } else {
                Record record = database.get(request.getKey());
                setResponse(responseObserver, ResultType.ERROR, record);
            }
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Error executing set::{}", request, e);
            if (canAttemptAgain(times)) {
                set(request, responseObserver);
            }
        }
    }

    @Override
    public void get(Key request, StreamObserver<RecordResult> responseObserver) {
        logger.info("get::{}", request);
        if (database.containsKey(request.getKey())) {
            Record record = database.get(request.getKey());
            setResponse(responseObserver, ResultType.SUCCESS, record);
        } else {
            setResponse(responseObserver, ResultType.ERROR, null);
        }
        responseObserver.onCompleted();
    }

    @Override
    public void del(Key request, StreamObserver<RecordResult> responseObserver) {
        int times = 0;
        try {
            logger.info("del::{}", request);
            if (database.containsKey(request.getKey())) {
                Record record = database.remove(request.getKey());
                setResponse(responseObserver, ResultType.SUCCESS, record);
                queueService.produce(new QueueRequest(Operation.DEL, request.getKey(), record));
            } else {
                setResponse(responseObserver, ResultType.ERROR, null);
            }
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Error executing del::{}", request, e);
            if (canAttemptAgain(times)) {
                del(request, responseObserver);
            }
        }
    }

    @Override
    public void delVersion(Version request, StreamObserver<RecordResult> responseObserver) {
        int times = 0;
        Optional.ofNullable(database.getOrDefault(request.getKey(), null)).ifPresentOrElse(record -> {
            try {
                logger.info("delVersion::{}", request);
                if (record.getVersion() == request.getVersion()) {
                    Record removedRecord = database.remove(request.getKey());
                    setResponse(responseObserver, ResultType.SUCCESS, record);
                    queueService.produce(new QueueRequest(Operation.DEL_VERSION, request.getKey(), removedRecord));
                } else {
                    setResponse(responseObserver, ResultType.ERROR_WV, record);
                }
            } catch (Exception e) {
                logger.error("Error executing delVersion::{}", request, e);
                if (canAttemptAgain(times)) {
                    delVersion(request, responseObserver);
                }
            }
        }, () -> setResponse(responseObserver, ResultType.ERROR_NE, null));
        responseObserver.onCompleted();
    }

    @Override
    public void testAndSet(RecordUpdate request, StreamObserver<RecordResult> responseObserver) {
        int times = 0;
        Optional.ofNullable(database.getOrDefault(request.getKey(), null)).ifPresentOrElse(record -> {
            try {
                logger.info("testAndSet::{}", request);
                if (record.getVersion() == request.getOldVersion()) {
                    Record newRecord = request.getRecord().toBuilder().setVersion(record.getVersion() + 1).build();
                    database.replace(request.getKey(), newRecord);
                    setResponse(responseObserver, ResultType.SUCCESS, record);
                    queueService.produce(new QueueRequest(Operation.TEST_SET, request.getKey(), newRecord));
                } else {
                    setResponse(responseObserver, ResultType.ERROR_WV, record);
                }
            } catch (Exception e) {
                logger.error("Error executing testAndSet::{}", request, e);
                if (canAttemptAgain(times)) {
                    testAndSet(request, responseObserver);
                }
            }
        }, () -> setResponse(responseObserver, ResultType.ERROR_NE, null));
        responseObserver.onCompleted();
    }

    private void setResponse(StreamObserver<RecordResult> responseObserver, ResultType resultType, Record record) {
        if (record == null) {
            responseObserver.onNext(RecordResult.newBuilder()
                    .setResultType(resultType)
                    .build());
        } else {
            responseObserver.onNext(RecordResult.newBuilder()
                    .setResultType(resultType)
                    .setRecord(record)
                    .build());
        }
    }

    private boolean canAttemptAgain(int times) {
        times++;
        return times <= 4;
    }
}