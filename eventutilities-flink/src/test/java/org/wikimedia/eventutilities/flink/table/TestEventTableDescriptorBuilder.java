package org.wikimedia.eventutilities.flink.table;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.wikimedia.eventutilities.core.event.EventStream;
import org.wikimedia.eventutilities.core.event.EventStreamFactory;
import org.wikimedia.eventutilities.flink.formats.json.JsonSchemaConverter;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.Resources;


public class TestEventTableDescriptorBuilder {
    private static final String testStreamConfigsFile =
        Resources.getResource("event_stream_configs.json").toString();

    private static final List<String> schemaBaseUris = Collections.singletonList(
        Resources.getResource("event-schemas/repo4").toString()
    );

    private static final String streamName = "test.event.example";

    private static final EventTableDescriptorBuilder builder = EventTableDescriptorBuilder.from(
        schemaBaseUris,
        testStreamConfigsFile
    );

    @BeforeEach
    void beforeEach() {
        builder.clear();
    }

    @Test
    void testBuilder() {
        EventStreamFactory eventStreamFactory = builder.getEventStreamFactory();
        EventStream eventStream = eventStreamFactory.createEventStream(streamName);

        TableDescriptor td = builder
            .eventStream(streamName)
            .connector("datagen")
            .option("number-of-rows", "10")
            .build();

        String expectedComment = eventStream.toString();
        assertThat(td.getComment().get())
            .isEqualTo(expectedComment);

        assertThat(td.getOptions().get("number-of-rows"))
            .isEqualTo("10");

        Schema expectedSchema = JsonSchemaConverter.toSchemaBuilder((ObjectNode)eventStream.schema()).build();

        assertThat(td.getSchema().get())
            .isEqualTo(expectedSchema);
    }

    @Test
    void testKafkaBuilder() {
        EventStreamFactory eventStreamFactory = builder.getEventStreamFactory();
        EventStream eventStream = eventStreamFactory.createEventStream(streamName);

        TableDescriptor td = builder
            .eventStream(streamName)
            .setupKafka(
                "localhost:9092",
                "my_consumer_group"
            )
            .build();

        Map<String, String> options = td.getOptions();

        assertThat(options.get("properties.bootstrap.servers"))
            .isEqualTo("localhost:9092");

        assertThat(options.get("properties.group.id"))
            .isEqualTo("my_consumer_group");

        assertThat(options.get("topic"))
            .isEqualTo(String.join(";", eventStream.topics()));

        Schema expectedSchema =
            JsonSchemaConverter.toSchemaBuilder((ObjectNode)eventStream.schema())
                .build();

        assertThat(td.getSchema().get())
            .isEqualTo(expectedSchema);
    }

    @Test
    void testWithKafkaTimestampAsWatermark() {
        EventStreamFactory eventStreamFactory = builder.getEventStreamFactory();
        EventStream eventStream = eventStreamFactory.createEventStream(streamName);

        TableDescriptor td = builder
            .eventStream(streamName)
            .setupKafka(
                "localhost:9092",
                "my_consumer_group"
            )
            .withKafkaTimestampAsWatermark()
            .build();

        Schema expectedSchema =
            JsonSchemaConverter.toSchemaBuilder((ObjectNode)eventStream.schema())
                .columnByMetadata(
                    "kafka_timestamp",
                    "TIMESTAMP_LTZ(3) NOT NULL",
                    "timestamp",
                    true)
                .watermark("kafka_timestamp", "kafka_timestamp - INTERVAL '10' SECOND")
                .build();

        List<String> expectedColumnNames = expectedSchema.getColumns().stream()
            .map(Schema.UnresolvedColumn::getName).collect(Collectors.toList());

        List<String> columnNames = td.getSchema().get().getColumns().stream()
            .map(Schema.UnresolvedColumn::getName).collect(Collectors.toList());

        // Adding a watermark column causes the Schemas not to be ==,
        // so just compare the column names instead.
        assertThat(columnNames).isEqualTo(expectedColumnNames);

    }

    @Test
    void testBuildPreconditions() {
        assertThatIllegalStateException().isThrownBy(builder::build)
            .withFailMessage("Must call eventStream and connector before build");
        builder.clear();

        assertThatIllegalStateException().isThrownBy(() -> {
            builder.eventStream(streamName);
            builder.build();
        })
        .withFailMessage("Must call connector before build");
        builder.clear();

        assertThatIllegalStateException().isThrownBy(() -> {
            builder.connector("datagen");
            builder.build();
        })
        .withFailMessage("Must call eventStream before build");
        builder.clear();
    }

    @Test
    void testGetSchemaBuilderPreconditions() {
        assertThatIllegalStateException().isThrownBy(builder::getSchemaBuilder)
            .withFailMessage("Must call eventStream or schemaBuilder before getSchemaBuilder");
        builder.clear();
    }

    @Test
    void testSetupKafkaPreconditions() {
        assertThatIllegalStateException()
            .isThrownBy(() -> {
            builder.setupKafka(
                "localhost:9092",
                "my_consumer_group"
            );
        })
        .withFailMessage("Must call eventStream before setupKafka");
        builder.clear();
    }

}
