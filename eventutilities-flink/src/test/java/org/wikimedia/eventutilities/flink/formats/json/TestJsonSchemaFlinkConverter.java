package org.wikimedia.eventutilities.flink.formats.json;


import static org.apache.flink.table.api.Expressions.$;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.io.IOUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.junit.ClassRule;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.wikimedia.eventutilities.core.event.EventStream;
import org.wikimedia.eventutilities.core.event.EventStreamFactory;
import org.wikimedia.eventutilities.flink.EventRowTypeInfo;
import org.wikimedia.eventutilities.flink.test.utils.FlinkTestUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;


@SuppressWarnings({"checkstyle:ClassFanOutComplexity"})
public class TestJsonSchemaFlinkConverter {

    private static final String testStreamConfigsFile =
        Resources.getResource("event_stream_configs.json").toString();

    private static final List<String> schemaBaseUris = Collections.singletonList(
        Resources.getResource("event-schemas/repo4").toString()
    );

    private static final URL jsonSchemaFile = Resources.getResource("event-schemas/repo4/test/event/1.1.0.json");

    private static ObjectNode jsonSchema;

    private static final DataType expectedDataType = DataTypes.ROW(
        DataTypes.FIELD(
            "$schema",
            DataTypes.STRING(),
            "A URI identifying the JSONSchema for this event. " +
            "This should match an schema's $id in a schema repository. E.g. /schema/title/1.0.0\n"
        ),
        DataTypes.FIELD("dt", DataTypes.TIMESTAMP_LTZ(3), "UTC event datetime, in ISO-8601 format"),
        DataTypes.FIELD(
         "meta", DataTypes.ROW(
                DataTypes.FIELD("stream", DataTypes.STRING(), "Name of the stream/queue/dataset that this event belongs in"),
                DataTypes.FIELD("dt", DataTypes.TIMESTAMP_LTZ(3), "UTC event datetime, in ISO-8601 format"),
                DataTypes.FIELD("id", DataTypes.STRING(), "Unique ID of this event")
            )
        ),
        DataTypes.FIELD("test", DataTypes.STRING()),
        DataTypes.FIELD("test_int", DataTypes.BIGINT()),
        DataTypes.FIELD("test_decimal", DataTypes.DOUBLE()),
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


    static final TypeInformation<Row> expectedTypeInformation = EventRowTypeInfo.create(
        // first field names.
        new String[] {
            "$schema",
            "dt",
            "meta",
            "test",
            "test_int",
            "test_decimal",
            "test_map",
            "test_array"
        },
        // then field types, corresponding to field name positions.
        // $schema
        Types.STRING,
        // dt
        Types.INSTANT,
        // meta
        Types.ROW_NAMED(
            new String[] {
                "stream",
                "dt",
                "id"
            },
            // meta.stream
            Types.STRING,
            // meta.dt
            Types.INSTANT,
            Types.STRING
        ),
        // test
        Types.STRING,
        // test_int
        Types.LONG,
        // test_decimal
        Types.DOUBLE,
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

    static final Row expectedExampleRow;
    static {

        expectedExampleRow = new Row(expectedTypeInformation.getArity());
        expectedExampleRow.setField(0, "/test/event/1.1.0"); // $schema
        expectedExampleRow.setField(1, Instant.parse("2019-01-01T00:00:00Z")); // dt

        Row expectedMeta = new Row(3);
        expectedMeta.setField(0, "test.event.example"); // meta.stream
        expectedMeta.setField(1, Instant.parse("2019-01-01T00:00:30Z")); // meta.dt
        expectedMeta.setField(2, "bbb07628-ffa9-40cf-8cbc-36d15e2049ba");

        expectedExampleRow.setField(2, expectedMeta); // meta

        expectedExampleRow.setField(3, "specific test value"); // test
        expectedExampleRow.setField(4, 2L); // test_int
        expectedExampleRow.setField(5, 2.0D); // test_decimal

        Map<String, String> test_map = new HashMap<>();
        test_map.put("key1", "val1");
        test_map.put("key2", "val2");
        expectedExampleRow.setField(6, test_map); // test_map

        expectedExampleRow.setField(7, null); // test_array
    }

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster = FlinkTestUtils.getTestFlinkCluster();

    @BeforeAll
    public static void setUp() throws IOException {
        String jsonSchemaString = IOUtils.toString(jsonSchemaFile, Charsets.UTF_8);
        ObjectMapper mapper = new ObjectMapper();
        jsonSchema = (ObjectNode)mapper.readTree(jsonSchemaString);
    }


    @Test
    void testToDataType() {
        DataType flinkDataType = JsonSchemaFlinkConverter.toDataType(jsonSchema);
        assertThat(flinkDataType.getLogicalType().asSerializableString())
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
    void testFlinkDeserializationSchemaRow() throws IOException {
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
        ObjectNode exampleEventJsonSchema = (ObjectNode)exampleEventStream.schema();
        byte[] exampleEventJsonBytes = exampleEventStream.exampleEvent().toString().getBytes(StandardCharsets.UTF_8);

        DeserializationSchema<Row> jsonRowDeserializer = JsonSchemaFlinkConverter.toDeserializationSchemaRow(
            exampleEventJsonSchema
        );

        Row exampleRow = jsonRowDeserializer.deserialize(exampleEventJsonBytes);

        assertThat(exampleRow).isEqualTo(expectedExampleRow);
    }

    @Test
    void testFlinkDataStreamIntegration() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Run in BATCH mode to make sure we can collect results and assert at end.
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        // EventStreamFactory with test schema repo and stream config.
        EventStreamFactory eventStreamFactory = EventStreamFactory.from(
            schemaBaseUris,
            testStreamConfigsFile
        );

        EventStream exampleEventStream = eventStreamFactory.createEventStream("test.event.example");

        List<byte[]> inputElements = new ArrayList<>();
        inputElements.add(exampleEventStream.exampleEvent().toString().getBytes(StandardCharsets.UTF_8));

        JsonRowDeserializationSchema deserializer = JsonSchemaFlinkConverter.toDeserializationSchemaRow(
            (ObjectNode)exampleEventStream.schema()
        );

        DataStream<Row> input = env.fromCollection(inputElements).map(deserializer::deserialize);

        EventStream enrichedEventStream = eventStreamFactory.createEventStream("test.event.enriched");
        DataStream<Row> output = input.map(
            new Enrich(),
            JsonSchemaFlinkConverter.toRowTypeInfo((ObjectNode)enrichedEventStream.schema())
        );

        output.addSink(new EnrichedSink());

        // Add a callback that will assert the correct final sum is collected
        // after the Flink job finishes.
        FlinkTestUtils.afterFlinkJob(
            env,
            () -> assertThat(EnrichedSink.countOfEnrichedField).isEqualTo(inputElements.size())
        );
    }

    @Test
    @Disabled("flaky")
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
        // Assert that field names and each top level field DataType match.
        assertThat(((RowType)tableDataType.getLogicalType()).getFieldNames())
            .isEqualTo(((RowType)expectedDataType.getLogicalType()).getFieldNames());

        List<DataType> actualChildren = tableDataType.getChildren();
        List<DataType> expectedChildren = expectedDataType.getChildren();
        for (int i = 0; i < actualChildren.size(); i++) {
            DataType actualChild = actualChildren.get(i);
            DataType expectedChild = expectedChildren.get(i);
            assertThat(actualChild.getLogicalType()).isEqualTo(expectedChild.getLogicalType());
        }

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
        FlinkTestUtils.afterFlinkJob(
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
    private static class EnrichedSink implements SinkFunction<Row> {
        public static Long countOfEnrichedField = 0L;
        @Override
        public void invoke(Row row, SinkFunction.Context context) throws Exception {
            String enrichedField = row.getFieldAs("enriched_field");
            if (enrichedField != null) {
                countOfEnrichedField++;
            }
        }
    }


}
