package org.example.arrow.sharing.basic;

import com.google.common.collect.Iterables;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class Flight {
    private static final FieldType NULLABLE_STRING = FieldType.nullable(new ArrowType.Utf8());

    public static void main(String[] args) {
        Location location = Location.forGrpcInsecure("0.0.0.0", 33333);
        try (BufferAllocator allocator = new RootAllocator()) {
            // Server
            try (FlightServer flightServer = FlightServer.builder(allocator, location, new CookbookProducer(allocator, location)).build()) {
                try {
                    flightServer.start();
                    log.info("S1: Server (Location): Listening on port " + flightServer.getPort());
                } catch (IOException e) {
                    System.exit(1);
                }

                // Client
                try (FlightClient flightClient = FlightClient.builder(allocator, location).build()) {
                    log.info("C1: Client (Location): Connected to {}", location.getUri());

                    // Populate data
                    Schema schema = new Schema(List.of(new Field("name", NULLABLE_STRING, null)));
                    try (VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator);
                         VarCharVector varCharVector = (VarCharVector) vectorSchemaRoot.getVector("name")) {
                        varCharVector.allocateNew(3);
                        varCharVector.set(0, "Ronald".getBytes());
                        varCharVector.set(1, "David".getBytes());
                        varCharVector.set(2, "Francisco".getBytes());
                        vectorSchemaRoot.setRowCount(3);
                        FlightClient.ClientStreamListener listener = flightClient.startPut(
                                FlightDescriptor.path("profiles"),
                                vectorSchemaRoot, new AsyncPutListener());
                        listener.putNext();
                        varCharVector.set(0, "Manuel".getBytes());
                        varCharVector.set(1, "Felipe".getBytes());
                        varCharVector.set(2, "JJ".getBytes());
                        vectorSchemaRoot.setRowCount(3);
                        listener.putNext();
                        listener.completed();
                        listener.getResult();
                        log.info("C2: Client (Populate Data): Wrote 2 batches with 3 rows each");
                    }

                    // Get metadata information
                    FlightInfo flightInfo = flightClient.getInfo(FlightDescriptor.path("profiles"));
                    log.info("C3: Client (Get Metadata): {}", flightInfo);

                    // Get data information
                    Ticket t = new Ticket(bytes(FlightDescriptor.path("profiles").getPath().get(0)));
                    try (FlightStream flightStream = flightClient.getStream(t)) {
                        int batch = 0;
                        try (VectorSchemaRoot vectorSchemaRootReceived = flightStream.getRoot()) {
                            log.info("C4: Client (Get Stream):");
                            while (flightStream.next()) {
                                batch++;
                                String tsv = vectorSchemaRootReceived.contentToTSVString();
                                log.info("Client Received batch #{}, Data: {}", batch, tsv);
                            }
                        }
                    } catch (Exception e) {
                        log.error("Exception", e);
                    }

                    // Get all metadata information
                    List<FlightInfo> flightInfosBefore = toList(flightClient.listFlights(Criteria.ALL), FlightInfo.class);
                    log.info("C5: Client (List Flights Info): {}", flightInfosBefore);

                    // Do delete action
                    Action a = new Action("DELETE", bytes(FlightDescriptor.path("profiles").getPath().get(0)));
                    List<Result> deleteActionResult = toList(() -> flightClient.doAction(a), Result.class);

                    for (Result result : deleteActionResult)
                        log.info("C6: Client (Do Delete Action): {}", string(result.getBody()));

                    // Get all metadata information (to validate delete action)
                    List<FlightInfo> flightInfos = toList(flightClient.listFlights(Criteria.ALL), FlightInfo.class);
                    log.info("flightInfos: {}", flightInfos);
                    log.info("C7: Client (List Flights Info): After delete - No records");

                    // Server shut down
                    flightServer.shutdown();
                    log.info("C8: Server shut down successfully");
                }
            } catch (InterruptedException e) {
                log.error("interrupted", e);
            }
        }
    }

    private static <T> List<T> toList(Iterable<T> iterable, Class<T> tClass) {
        return Arrays.asList(Iterables.toArray(iterable, tClass));
    }

    @Value
    static class Dataset {
        List<ArrowRecordBatch> batches;
        Schema schema;
        long rows;
    }

    static class CookbookProducer extends NoOpFlightProducer {
        private final BufferAllocator allocator;
        private final Location location;
        private final ConcurrentHashMap<FlightDescriptor, Dataset> datasets;

        public CookbookProducer(BufferAllocator allocator, Location location) {
            this.allocator = allocator;
            this.location = location;
            this.datasets = new ConcurrentHashMap<>();
        }

        @Override
        public Runnable acceptPut(CallContext context,
                                  FlightStream flightStream,
                                  StreamListener<PutResult> ackStream) {
            List<ArrowRecordBatch> batches = new ArrayList<>();
            return () -> {
                long rows = 0;
                VectorUnloader unloader;
                while (flightStream.next()) {
                    unloader = new VectorUnloader(flightStream.getRoot());
                    try (final ArrowRecordBatch arb = unloader.getRecordBatch()) {
                        batches.add(arb);
                        rows += flightStream.getRoot().getRowCount();
                    }
                }
                Dataset dataset = new Dataset(batches, flightStream.getSchema(), rows);
                datasets.put(flightStream.getDescriptor(), dataset);
                ackStream.onCompleted();
            };
        }

        @Override
        public void getStream(CallContext context,
                              Ticket ticket,
                              ServerStreamListener listener) {
            FlightDescriptor flightDescriptor = FlightDescriptor.path(string(ticket.getBytes()));
            Dataset dataset = this.datasets.get(flightDescriptor);
            if (dataset == null) {
                throw CallStatus.NOT_FOUND.withDescription("Unknown descriptor").toRuntimeException();
            } else {
                VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(
                        this.datasets.get(flightDescriptor).getSchema(), allocator);
                listener.start(vectorSchemaRoot);
                for (ArrowRecordBatch arrowRecordBatch : this.datasets.get(flightDescriptor).getBatches()) {
                    VectorLoader loader = new VectorLoader(vectorSchemaRoot);
                    loader.load(arrowRecordBatch.cloneWithTransfer(allocator));
                    listener.putNext();
                }
                listener.completed();
            }
        }

        @Override
        public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
            FlightDescriptor flightDescriptor = FlightDescriptor.path(
                    string(action.getBody()));
            switch (action.getType()) {
                case "DELETE": {
                    if (datasets.remove(flightDescriptor) != null) {
                        Result result = new Result(bytes("Delete completed"));
                        listener.onNext(result);
                    } else {
                        Result result = new Result(bytes("Delete not completed. Reason: Key did not exist."));
                        listener.onNext(result);
                    }
                    listener.onCompleted();
                }
                case "unsupported":
                default:
                    throw new RuntimeException(getClass().getSimpleName() + " did not understand: " + action.getType());
            }
        }

        @Override
        public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
            FlightEndpoint flightEndpoint = new FlightEndpoint(
                    new Ticket(bytes(descriptor.getPath().get(0))), location);
            return new FlightInfo(
                    datasets.get(descriptor).getSchema(),
                    descriptor,
                    Collections.singletonList(flightEndpoint),
                    /*bytes=*/-1,
                    datasets.get(descriptor).getRows()
            );
        }

        @Override
        public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
            datasets.forEach((k, v) -> listener.onNext(getFlightInfo(null, k)));
            listener.onCompleted();
        }
    }

    private static byte[] bytes(String x) {
        return x.getBytes(StandardCharsets.UTF_8);
    }

    private static String string(byte[] ticket) {
        return new String(ticket, StandardCharsets.UTF_8);
    }
}
