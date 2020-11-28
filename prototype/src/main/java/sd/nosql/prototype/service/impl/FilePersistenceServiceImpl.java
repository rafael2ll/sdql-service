package sd.nosql.prototype.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sd.nosql.prototype.Record;
import sd.nosql.prototype.service.PersistenceService;

import java.io.*;
import java.util.concurrent.ConcurrentHashMap;

public class FilePersistenceServiceImpl implements PersistenceService {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseServiceImpl.class);
    private String path = "/home/bianca/Documentos/UFU/SD/teste/database";

    @Override
    public ConcurrentHashMap<Long, Record> read() {
        try {
            File fileToRead = new File(path);
            if (fileToRead.length() == 0) {
                return new ConcurrentHashMap<Long, Record>();
            } else {
                FileInputStream fileInputStream = new FileInputStream(fileToRead);
                ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
                return (ConcurrentHashMap<Long, Record>)objectInputStream.readObject();
            }
        } catch (Exception e) {
            logger.error("Read database error", e);
            return null;
        }
    }

    @Override
    public void write(ConcurrentHashMap<Long, Record> database) {
        try {
            File fileToWrite = new File(path);
            FileOutputStream fileOutputStream = new FileOutputStream(fileToWrite);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
            objectOutputStream.writeObject(database);
            objectOutputStream.flush();
            objectOutputStream.close();
            fileOutputStream.close();
        } catch (Exception e) {
            logger.error("Write database error", e);
        }
    }
}