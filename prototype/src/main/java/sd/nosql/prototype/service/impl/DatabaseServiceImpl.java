package sd.nosql.prototype.service.impl;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sd.nosql.prototype.Record;
import sd.nosql.prototype.*;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class DatabaseServiceImpl extends DatabaseServiceGrpc.DatabaseServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseServiceImpl.class);

    Map<Long, Record> database = new ConcurrentHashMap<>();

    @Override
    public void set(RecordInput request, StreamObserver<RecordResult> responseObserver) {
        logger.info("set::{}", request);
        if (!database.containsKey(request.getKey())) {
            database.put(request.getKey(), request.getRecord().toBuilder().setVersion(1).build());
            responseObserver.onNext(RecordResult.newBuilder()
                    .setResultType(ResultType.SUCCESS)
                    .build());
        } else {
            responseObserver.onNext(RecordResult.newBuilder()
                    .setResultType(ResultType.ERROR)
                    .setRecord(database.get(request.getKey()))
                    .build());
        }
        responseObserver.onCompleted();
    }

    @Override
    public void get(Key request, StreamObserver<RecordResult> responseObserver) {
        if (database.containsKey(request.getKey())) {
            responseObserver.onNext(RecordResult.newBuilder()
                    .setResultType(ResultType.SUCCESS)
                    .setRecord(database.get(request.getKey()))
                    .build());
        } else {
            responseObserver.onNext(RecordResult.newBuilder()
                    .setResultType(ResultType.ERROR)
                    .build());
        }
        responseObserver.onCompleted();

    }

    @Override
    public void del(Key request, StreamObserver<RecordResult> responseObserver) {
        if (database.containsKey(request.getKey())) {
            responseObserver.onNext(RecordResult.newBuilder()
                    .setResultType(ResultType.SUCCESS)
                    .setRecord(database.remove(request.getKey()))
                    .build());
        } else {
            responseObserver.onNext(RecordResult.newBuilder()
                    .setResultType(ResultType.ERROR)
                    .build());
        }
        responseObserver.onCompleted();

    }

    @Override
    public void delVersion(Version request, StreamObserver<RecordResult> responseObserver) {
        Optional.ofNullable(database.getOrDefault(request.getKey(), null)).ifPresentOrElse(record -> {
            if (record.getVersion() == request.getVersion()) {
                responseObserver.onNext(RecordResult.newBuilder()
                        .setResultType(ResultType.SUCCESS)
                        .setRecord(record)
                        .build());
            } else {
                responseObserver.onNext(RecordResult.newBuilder()
                        .setResultType(ResultType.ERROR_WV)
                        .build());
            }
        }, () ->
                responseObserver.onNext(RecordResult.newBuilder()
                        .setResultType(ResultType.ERROR_NE)
                        .build()));
        responseObserver.onCompleted();

    }

    @Override
    public void testAndSet(RecordUpdate request, StreamObserver<RecordResult> responseObserver) {
        Optional.ofNullable(database.getOrDefault(request.getKey(), null)).ifPresentOrElse(record -> {
            if (record.getVersion() == request.getOldVersion()) {
                database.replace(request.getKey(), request.getRecord().toBuilder().setVersion(record.getVersion() + 1).build());
                responseObserver.onNext(RecordResult.newBuilder()
                        .setResultType(ResultType.SUCCESS)
                        .setRecord(record)
                        .build());
            } else {
                responseObserver.onNext(RecordResult.newBuilder()
                        .setResultType(ResultType.ERROR_WV)
                        .setRecord(record)
                        .build());
            }
        }, () ->
                responseObserver.onNext(RecordResult.newBuilder()
                        .setResultType(ResultType.ERROR_NE)
                        .build()));
        responseObserver.onCompleted();
    }

}
