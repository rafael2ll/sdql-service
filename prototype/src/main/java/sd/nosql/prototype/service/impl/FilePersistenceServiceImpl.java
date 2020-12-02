package sd.nosql.prototype.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sd.nosql.prototype.Record;
import sd.nosql.prototype.service.PersistenceService;

import java.io.*;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;

public class FilePersistenceServiceImpl implements PersistenceService {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseServiceImpl.class);
    private ObjectMapper objectMapper = new ObjectMapper();
    private String versionPath = "database/version.db";
    private String path = "database/data/bckp_%d.db";
    private int version = 0;

    @Override
    public ConcurrentHashMap<Long, Record> read() {
        if (version == 0) {
            readVersion();
            if (version == 0)
                return new ConcurrentHashMap<>();
        }
        try {
            File fileToRead = new File(String.format(path, version));
            if (fileToRead.length() == 0) {
                return new ConcurrentHashMap<>();
            } else {
                FileInputStream fileInputStream = new FileInputStream(fileToRead);
                ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
                return (ConcurrentHashMap<Long, Record>) objectInputStream.readObject();
            }
        } catch (Exception e) {
            logger.error("Read database error", e);
            return null;
        }
    }

    @Override
    public void write(ConcurrentHashMap<Long, Record> database) {
        try {
            File fileToWrite = new File(String.format(path, ++version));
            FileOutputStream fileOutputStream = new FileOutputStream(fileToWrite);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
            objectOutputStream.writeObject(database);
            objectOutputStream.flush();
            objectOutputStream.close();
            fileOutputStream.close();
            writeVersion();
        } catch (Exception e) {
            logger.error("Write database error", e);
        }
    }

    private void readVersion() {
        try {
            version = objectMapper.readValue(Paths.get(versionPath).toFile(), Integer.class);
        } catch (IOException | NullPointerException e) {
            version = 0;
        }
    }

    private void writeVersion() {
        try {
            objectMapper.writeValue(Paths.get(versionPath).toFile(), version);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}