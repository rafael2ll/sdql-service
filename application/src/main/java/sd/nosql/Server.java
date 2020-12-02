package sd.nosql;

import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sd.nosql.prototype.service.impl.DatabaseServiceImpl;

import java.io.IOException;

/*
========================================================================================================================
            1. Firstly, create a folder inside root, called database
            2. Inside the database folder, create a folder called data
            3. Now you're read to go

  - Folder structure:
   |-> application
     |-> src
   |-> prototype
     |-> src
   |-> database
     |-> data
     |-> ...backup files automatically created
     |-> version.db // Also programmatically created
========================================================================================================================
*/
public class Server {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    public static void main(String[] args) throws IOException, InterruptedException {
        logger.info("Starting server...");
        io.grpc.Server server = ServerBuilder
                .forPort(8080)
                .addService(new DatabaseServiceImpl(30000)).build();
        server.start();
        server.awaitTermination();
    }
}
