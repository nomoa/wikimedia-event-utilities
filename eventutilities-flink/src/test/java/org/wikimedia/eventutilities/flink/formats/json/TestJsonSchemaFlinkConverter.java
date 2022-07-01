package org.wikimedia.eventutilities.flink.formats.json;


import static org.apache.flink.table.api.Expressions.$;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.junit.ClassRule;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.wikimedia.eventutilities.core.event.EventStream;
import org.wikimedia.eventutilities.core.event.EventStreamFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.common.io.Resources;

@SuppressWarnings({"checkstyle:ClassFanOutComplexity"})
public class TestJsonSchemaFlinkConverter {

    private static final String testStreamConfigsFile =
        Resources.getResource("event_stream_configs.json").toString();

    private static final List<String> schemaBaseUris = Collections.singletonList(
        Resources.getResource("event-schemas/repo4").toString()
    );

    private static final File jsonSchemaFile = new File("src/test/resources/event-schemas/repo4/test/event/1.1.0.json");

    private static ObjectNode jsonSchema;

    private static final DataType expectedDataType = DataTypes.ROW(
        DataTypes.FIELD(
            "$schema",
            DataTypes.STRING(),
            "A URI identifying the JSONSchema for this event. " +
            "This should match an schema's $id in a schema repository. E.g. /schema/title/1.0.0\n"
        ),
        DataTypes.FIELD(
         "meta", DataTypes.ROW(
                DataTypes.FIELD("uri", DataTypes.STRING(), "Unique URI identifying the event or entity"),
                DataTypes.FIELD("request_id", DataTypes.STRING(), "Unique ID of the request that caused the event"),
                DataTypes.FIELD("id", DataTypes.STRING(), "Unique ID of this event"),
                DataTypes.FIELD("dt", DataTypes.STRING(), "UTC event datetime, in ISO-8601 format"),
                DataTypes.FIELD("domain", DataTypes.STRING(), "Domain the event or entity pertains to"),
                DataTypes.FIELD("stream", DataTypes.STRING(), "Name of the stream/queue/dataset that this event belongs in")
            )
        ),
        DataTypes.FIELD("test", DataTypes.STRING()),
        DataTypes.FIELD("test_int", DataTypes.BIGINT()),
        DataTypes.FIELD("test_map", DataTypes.MAP(
            DataTypes.STRING(),
            DataTypes.STRING()
            ),
            "We want to support 'map' types using additionalProperties to specify the value types.  " +
            "(Keys are always strings.) A map that has concrete properties specified is still a map. " +
            "The concrete properties can be used for validation or to require that certain keys exist in the map. " +
            "The concrete properties will not be considered part of the schema. The concrete properties MUST match " +
            "the additionalProperties (map value) schema.\n"
        ),
        DataTypes.FIELD("test_array", DataTypes.ARRAY(
                DataTypes.ROW(
                    DataTypes.FIELD(
                        "prop1",
                        DataTypes.STRING(),
                        "prop 1 field in complex array items"
                    )
                )),
            "Array with items schema"
        )
    );


    static final TypeInformation<Row> expectedTypeInformation = Types.ROW_NAMED(
        // first field names.
        new String[] {
            "$schema",
            "meta",
            "test",
            "test_int",
            "test_map",
            "test_array"
        },
        // then field types, corresponding to field name positions.
        // $schema
        Types.STRING,
        // meta
        Types.ROW_NAMED(
            new String[] {
                "uri",
                "request_id",
                "id",
                "dt",
                "domain",
                "stream"
            },
            // meta.uri
            Types.STRING,
            // meta.request_id
            Types.STRING,
            // meta.id
            Types.STRING,
            // meta.dt
            Types.STRING,
            // meta.domain
            Types.STRING,
            // meta.stream
            Types.STRING
        ),
        // test
        Types.STRING,
        // test_int
        Types.LONG,
        // test_map
        Types.MAP(
            Types.STRING,
            Types.STRING
        ),
        // test_array
        Types.OBJECT_ARRAY(
            Types.ROW_NAMED(
                new String[] {"prop1"},
                Types.STRING
            )
        )
    );

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
        new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                .setNumberSlotsPerTaskManager(2)
                .setNumberTaskManagers(1)
                .build());

    @BeforeAll
    public static void setUp() throws IOException {
        String jsonSchemaString = Files.asCharSource(jsonSchemaFile, Charsets.UTF_8).read();
        ObjectMapper mapper = new ObjectMapper();
        jsonSchema = (ObjectNode)mapper.readTree(jsonSchemaString);
    }


    @Test
    void testToDataType() {
        DataType flinkDataType = JsonSchemaFlinkConverter.toDataType(jsonSchema);
        assertThat(flinkDataType.getLogicalType().asSerializableString())
            .withFailMessage("DataType expected to be converted from JSONSchema")
            .isEqualTo(expectedDataType.getLogicalType().asSerializableString());
    }

    @Test
    void testToSchemaBuilder() {
        Schema tableSchema = JsonSchemaFlinkConverter.toSchemaBuilder(jsonSchema).build();
        Schema expectedSchema = Schema.newBuilder().fromRowDataType(expectedDataType).build();
        assertThat(tableSchema)
            .hasToString(expectedSchema.toString());
    }

    @Test
    void testToTypeInformation() {
        TypeInformation<?> typeInfo = JsonSchemaFlinkConverter.toTypeInformation(jsonSchema);
        assertThat(typeInfo)
            .isEqualTo(expectedTypeInformation);
    }

    @Test
    void testToRowTypeInfo() {
        RowTypeInfo typeInfo = JsonSchemaFlinkConverter.toRowTypeInfo(jsonSchema);
        assertThat(typeInfo)
            .isEqualTo(expectedTypeInformation);
    }

    @Test
    void testFlinkTableIntegration() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Run in BATCH mode to make sure we can collect results and assert at end.
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // EventStreamFactory with test schema repo and stream config.
        EventStreamFactory eventStreamFactory = EventStreamFactory.from(
            schemaBaseUris,
            testStreamConfigsFile
        );

        EventStream exampleEventStream = eventStreamFactory.createEventStream("test.event.example");

        Schema tableSchema = JsonSchemaFlinkConverter.toSchemaBuilder(
            (ObjectNode)exampleEventStream.schema()
        ).build();

        long numberOfRowsToGenerate = 10L;

        TableDescriptor td = TableDescriptor.forConnector("datagen")
            .schema(tableSchema)
            .comment(exampleEventStream.toString())
            // Example: If we were doing Kafka, we could use exampleEventStream to get topics too:
            //.option("topic", exampleEventStream.topics().join(";"))
            .option("number-of-rows", Long.toString(numberOfRowsToGenerate))
            // Make the test_int field generate as a sequence, between 0 and numberOfRowsToGenerate -1
            .option("fields.test_int.kind", "sequence")
            .option("fields.test_int.start", "0")
            .option("fields.test_int.end", Long.toString(numberOfRowsToGenerate - 1))
            .build();

        tEnv.createTemporaryTable("test_event_example", td);
        Table testEventExampleTable = tEnv.from("test_event_example");

        DataType tableDataType = testEventExampleTable.getResolvedSchema().toPhysicalRowDataType();

        // It seems that once the Row DataType has gone through the Flink Table factory,
        // it loses its descriptions from the top level fields in the Row.
        // Sub fields that are RowTypes keep the descriptions?!
        // Assert that field names are the same instead of comparing LogicalTypes directly.
        assertThat(((RowType)tableDataType.getLogicalType()).getFieldNames())
            .isEqualTo(((RowType)expectedDataType.getLogicalType()).getFieldNames());

        long expectedSum = 0L;
        for (long i = (numberOfRowsToGenerate - 1L); i >= 0L; i--) {
            expectedSum += i;
        }
        final long finalExpectedSum = expectedSum;

        // Sum test_int using Table API.
        testEventExampleTable
            .select($("test_int").sum().as("sum"))
            .execute().collect()
            .forEachRemaining((Row row) -> {
                assertThat((long)row.getFieldAs("sum"))
                    .isEqualTo(finalExpectedSum);
            });


        // Sum test_int using DataStream API,
        // testing that we can convert the Table into a DataStream,

        // values are collected in a static variable
        SumSink.sum = 0L;
        // create a stream of custom elements and apply transformations
        DataStream<Row> exampleDataStream = tEnv.toDataStream(testEventExampleTable);

        exampleDataStream
            .map(new ToTestInt())
            .addSink(new SumSink());

        // Add a callback that will assert the correct final sum is collected
        // after the Flink job finishes.
        afterFlinkJob(
            env,
            () -> assertThat(SumSink.sum).isEqualTo(finalExpectedSum)
        );

        // Let's fork the DataStream, map and enrich (add a field),
        // and convert back to Table API.
        EventStream enrichedEventStream =
            eventStreamFactory.createEventStream("test.event.enriched");

        // we are going to map exampleEventStream to enrichedEventStream but using DataStream API,
        // so get the TypeInformation for enrichedEventStream.
        RowTypeInfo enrichedTypeInformation =
            JsonSchemaFlinkConverter.toRowTypeInfo((ObjectNode)enrichedEventStream.schema());

        DataStream<Row> enrichedDataStream = exampleDataStream
            .map(
                new Enrich(),
                enrichedTypeInformation
            );

        Table enrichedEventTable = tEnv.fromDataStream(enrichedDataStream);


        // Test that we can now access the field we added in the DataStream,
        // and use it in the Table API.
        enrichedEventTable
            .groupBy($("enriched_field"))
            .select($("enriched_field").count().as("cnt"))
            .execute().collect()
            .forEachRemaining(row -> {
                assertThat((long)row.getFieldAs("cnt"))
                    .isEqualTo(numberOfRowsToGenerate);
            });

        env.execute();
    }

    // Maps Row to the "test_int" field as a Long.
    public static class ToTestInt implements MapFunction<Row, Long> {
        @Override
        public Long map(Row row) {
            return row.getFieldAs("test_int");
        }
    }
    // create a testing sink
    private static class SumSink implements SinkFunction<Long> {
        public static Long sum = 0L;
        @Override
        public void invoke(Long value, SinkFunction.Context context) throws Exception {
            sum += value;
        }
    }

    // Adds enriched_field with a literal "enriched value" in every Row.
    public static class Enrich implements MapFunction<Row, Row> {
        @Override
        public Row map(Row row) {
            Row newRow = Row.withNames(row.getKind());
            Objects.requireNonNull(row.getFieldNames(true)).forEach(fieldName -> {
                newRow.setField(fieldName, row.getField(fieldName));
            });
            newRow.setField("enriched_field", "enriched value");
            return newRow;
        }

    }

    /**
     * Register a callback to be called after successful completion of a FLink job in the
     * StreamExecutionEnvironment. Throws a RuntimeException if any part of the Flink job fails.
     * Useful for running test assertions after job results are collected in a Sink.
     *
     * @param env StreamExecutionEnvironment
     * @param callback 0 argument callback to call on successful job completion.
     */
    public static void afterFlinkJob(StreamExecutionEnvironment env, Callable<Object> callback) {
        env.registerJobListener(new JobListener() {
            @Override
            public void onJobSubmitted(JobClient jobClient, Throwable throwable)  {
                // We just raise any throwable, otherwise no-op.
                if (throwable != null) {
                    throw new RuntimeException(throwable);
                }
            }

            //Callback on job execution finished, successfully or unsuccessfully.
            @SuppressWarnings("checkstyle:IllegalCatch")
            @Override
            public void onJobExecuted(JobExecutionResult jobExecutionResult, Throwable throwable)  {
                // Raise any throwable, else call the callback
                if (throwable != null) {
                    throw new RuntimeException(throwable);
                } else {
                    try {
                        callback.call();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });
    }

}