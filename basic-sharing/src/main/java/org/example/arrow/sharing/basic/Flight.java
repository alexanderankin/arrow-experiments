package org.example.arrow.sharing.basic;

import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Flight {
    public static void main(String[] args) {
        new Thread(new Server()).start();
        new Thread(new Client()).start();
    }

    @Slf4j
    public static class Client implements Runnable {

        @Override
        public void run() {
            Location location = Location.forGrpcInsecure("0.0.0.0", 58104);

            try (BufferAllocator allocator = new RootAllocator();
                 FlightClient flightClient = getClient(location, allocator)) {

                Iterable<FlightInfo> flightInfosBefore = flightClient.listFlights(Criteria.ALL);
                List<FlightInfo> flightInfos = new ArrayList<>();
                flightInfosBefore.forEach(flightInfos::add);
                log.info("flightInfos {}", flightInfos);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private FlightClient getClient(Location location, BufferAllocator allocator) {
            return FlightClient.builder(allocator, location).build();
        }
    }

    public static class Server implements Runnable {
        private static final Schema SCHEMA;
        // private static final FlightDescriptor DESCRIPTOR;

        static {
            Field age = new Field("age",
                    FieldType.nullable(new ArrowType.Int(32, true)),
                    null);
            Field name = new Field("name",
                    FieldType.nullable(new ArrowType.Utf8()),
                    null);
            SCHEMA = new Schema(Arrays.asList(age, name));
            // DESCRIPTOR = new FlightDescriptor(true, List.of(""), null);
        }
        private static final FlightInfo FLIGHT_INFO = new FlightInfo(SCHEMA, null, null, 1, 0);

        @Override
        public void run() {
            Location location = Location.forGrpcInsecure("0.0.0.0", 0);

            try (BufferAllocator allocator = new RootAllocator();
                 FlightServer server = getServer(location, allocator)) {
                server.start();
                System.out.println("Server listening on port " + server.getPort());
                server.awaitTermination();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private FlightServer getServer(Location location, BufferAllocator allocator) {
            FlightProducer flightProducer = new TutorialFlightProducer();
            return FlightServer.builder(allocator, location, flightProducer).build();
        }

        public static class TutorialFlightProducer extends NoOpFlightProducer implements FlightProducer {
            // Override methods or use NoOpFlightProducer for only methods needed

            @Override
            public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
                listener.onError(CallStatus.UNAUTHENTICATED.withDescription("please log in first").toRuntimeException());
                listener.onNext(FLIGHT_INFO);
            }
        }
    }
}
