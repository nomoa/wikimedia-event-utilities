package org.wikimedia.eventutilities.flink.stream;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;
import org.wikimedia.eventutilities.core.event.EventStream;
import org.wikimedia.eventutilities.flink.formats.json.JsonRowDeserializationSchema;
import org.wikimedia.eventutilities.flink.formats.json.JsonSchemaFlinkConverter;
import org.wikimedia.eventutilities.flink.test.utils.FlinkTestUtils;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.Resources;

public class TestEventDataStreamFactory {

    private static final String testStreamConfigsFile =
        Resources.getResource("event_stream_configs.json").toString();

    private static final List<String> schemaBaseUris = Collections.singletonList(
        Resources.getResource("event-schemas/repo4").toString()
    );

    private static final URL testEventsFileURL =
        Resources.getResource("test.event.json");

    private static final String streamName = "test.event.example";

    private static final EventDataStreamFactory factory = EventDataStreamFactory.from(
        schemaBaseUris,
        testStreamConfigsFile
    );

    static final Row expectedExampleRow;
    static {

        expectedExampleRow = new Row(8);
        expectedExampleRow.setField(0, "/test/event/1.1.0"); // $schema
        expectedExampleRow.setField(1, Instant.parse("2019-01-01T00:00:00Z")); // dt

        Row expectedMeta = new Row(2);
        expectedMeta.setField(0, "test.event.example"); // meta.stream
        expectedMeta.setField(1, Instant.parse("2019-01-01T00:00:30Z")); // meta.dt
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

    @Test
    void testGetDeserializer() throws IOException {
        JsonRowDeserializationSchema deserializer = factory.deserializer(streamName);
        EventStream exampleEventStream = factory.getEventStreamFactory().createEventStream(streamName);

        Row exampleRow = deserializer.deserialize(
            exampleEventStream.exampleEvent().toString().getBytes(StandardCharsets.UTF_8)
        );

        assertThat(exampleRow).isEqualTo(expectedExampleRow);
    }

    @Test
    void testKafkaSourceBuilderProducedType() {
        // NOTE: The KafkaSource is not ever actually used by these tests.
        // This test just verifies that the deserialization schema that the
        // KafkaSource will use is the correct one.
        KafkaSourceBuilder<Row> builder = factory.kafkaSourceBuilder(
            streamName,
            "localhost:9092",
            "my_consumer_group"
        );
        KafkaSource<Row> kafkaSource = builder.build();

        RowTypeInfo expectedTypeInfo = JsonSchemaFlinkConverter.toRowTypeInfo(
            (ObjectNode)factory.getEventStreamFactory().createEventStream(streamName).schema()
        );

        assertThat(kafkaSource.getProducedType()).isEqualTo(expectedTypeInfo);
    }

    @Test
    void testRowTypeInfo() {
        RowTypeInfo typeInfo = factory.rowTypeInfo(streamName);

        RowTypeInfo expectedTypeInfo = JsonSchemaFlinkConverter.toRowTypeInfo(
            (ObjectNode)factory.getEventStreamFactory().createEventStream(streamName).schema()
        );

        assertThat(typeInfo).isEqualTo(expectedTypeInfo);
    }

    @Test
    void testDataStreamFromFiles() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Run in BATCH mode to make sure we can collect results and assert at end.
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        DataStream<Row> dataStream = fileDataStream(
            streamName,
            env,
            WatermarkStrategy.noWatermarks(),
            testEventsFileURL.toURI()
        );

        // Count the number of rows that have the "test" field == "expected value"
        // There are 2 of these elemetns in the test.event.json file.
        dataStream.addSink(
            new FieldMatchCountSink("test", "expected value")
        );

        env.execute();

        // Add a callback that will assert the correct final count
        // after the Flink job finishes.
        FlinkTestUtils.afterFlinkJob(
            env,
            () -> assertThat(FieldMatchCountSink.count).isEqualTo(2)
        );

    }

    /**
     * MapFunction that counts every Row that has expectedKey not null and == expectedValue.
     */
    private static class FieldMatchCountSink implements SinkFunction<Row> {

        private final String expectedKey;
        private final String expectedValue;

        FieldMatchCountSink(String expectedKey, String expectedValue) {
            this.expectedKey = expectedKey;
            this.expectedValue = expectedValue;
        }

        public static Long count = 0L;
        @Override
        public void invoke(Row row, SinkFunction.Context context) {
            String value = row.getFieldAs(expectedKey);
            if (value != null && value.equals(expectedValue)) {
                count += 1;
            }
        }
    }


    // NOTE: The below fileStreamSourceBuilder and fileDataStream methods were removed from
    // EventDataStreamFactory public API during code review. These may prove useful as
    // public methods in the future, but for now they are only used for testing here.
    // If we need them exposed, we can promote them back into EventDataStreamFactory later.

    /**
     * Gets a {@link FileSource.FileSourceBuilder} that will generate Rows of events in JSON
     * files.  The Rows will be deserialized using the JSONSchema of streamName.
     * This is a slightly lower level method than
     * fileDataStream(String, StreamExecutionEnvironment, WatermarkStrategy, URI...)
     * You'll probably want to use fileDataStream, especially if you are just using this FileSource
     * for testing purposes.
     *
     * @param streamName
     *  Name of the EventStream, must be declared in EventStreamConfig.
     *
     * @param paths
     *  Flink Paths from which to read JSON events.
     */
    public FileSource.FileSourceBuilder<Row> fileStreamSourceBuilder(
        String streamName,
        Path... paths
    ) {
        return FileSource.forRecordStreamFormat(
            new LineStreamFormat<>(factory.deserializer(streamName)),
            paths
        );
    }

    /**
     * Gets a {@link DataStreamSource} of {@link Row} for streamName that reads JSON events from files.
     *
     * Example:
     *
     * <pre>{@code
     *     EventDataStreamFactory eventDataStreamFactory = EventDataStreamFactory.from(...)
     *     DataStreamSource&lt;Row&gt; eventFileSource = eventDataStreamFactory.dataStreamFromFiles(
     *          "test.event.example",       // EventStream name
     *          env,                        // Flink StreamExecutionEnvironment
     *          WatermarkStrategy.noWatermarks(),
     *          new URI("file:///path/to/test.events.json"
     *     );
     * }</pre>
     *
     * @param streamName
     *  Name of the EventStream, must be declared in EventStreamConfig.
     *
     * @param env
     *  StreamExecutionEnvironment in which to call fromSource.
     *
     * @param watermarkStrategy
     *  For simple testing input from files, you'll likely want to use WatermarkStrategy.noWatermarks();
     *
     * @param files
     *  URIs to files of JSON events that conform to the JSONSchema of streamName.
     *  These will be converted to Flink {@link Path}s.
     */
    public DataStreamSource<Row> fileDataStream(
        String streamName,
        StreamExecutionEnvironment env,
        WatermarkStrategy<Row> watermarkStrategy,
        URI... files
    ) {
        // Convert file URIs into Flink Paths
        Path[] paths = Arrays.stream(files).map(Path::new).toArray(Path[]::new);
        EventStream eventStream = factory.getEventStreamFactory().createEventStream(streamName);

        // Make a nice DataStreamSource description including all the source files.
        String dataSourceDescription = eventStream +
            " from files [" +
            Arrays.stream(paths).map(Path::toString).collect(Collectors.joining(",")) +
            "]";

        return env.fromSource(
            fileStreamSourceBuilder(streamName, paths).build(),
            watermarkStrategy,
            dataSourceDescription
        );
    }


}
