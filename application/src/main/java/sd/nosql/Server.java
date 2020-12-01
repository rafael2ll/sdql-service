package sd.nosql;

import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sd.nosql.prototype.service.impl.DatabaseServiceImpl;

import java.io.IOException;

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
