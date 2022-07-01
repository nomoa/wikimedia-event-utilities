package org.wikimedia.eventutilities.flink.formats.json;

import static java.util.Collections.singletonList;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.flink.types.Row;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.wikimedia.eventutilities.core.event.EventStream;
import org.wikimedia.eventutilities.core.event.EventStreamFactory;
import org.wikimedia.eventutilities.core.event.JsonEventGenerator;
import org.wikimedia.eventutilities.core.event.JsonEventGenerator.EventNormalizer;
import org.wikimedia.eventutilities.flink.EventRowTypeInfo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

class TestJsonRowSerializationSchema {
    private static final UUID MY_UUID = new UUID(1, 2);
    private static final Instant INGESTION_TIME = Instant.ofEpochSecond(123);
    public static final String SCHEMA_VERSION = "1.1.0";
    public static final String STREAM_NAME = "test.event.example";
    private final ObjectMapper mapper = new ObjectMapper();

    private final EventStreamFactory eventStreamFactory = EventStreamFactory.from(
            singletonList(this.getClass().getResource("/event-schemas/repo4").toString()),
            this.getClass().getResource("/event_stream_configs.json").toString());

    private final JsonEventGenerator jsonEventGenerator = JsonEventGenerator.builder()
            .withUuidSupplier(() -> MY_UUID)
            .jsonMapper(mapper)
            .schemaLoader(eventStreamFactory.getEventSchemaLoader())
            .eventStreamConfig(eventStreamFactory.getEventStreamConfig())
            .ingestionTimeClock(() -> INGESTION_TIME)
            .build();

    private final EventStream eventStream = eventStreamFactory.createEventStream(STREAM_NAME);
    private final EventRowTypeInfo typeInfo = JsonSchemaFlinkConverter.toRowTypeInfo((ObjectNode) eventStream.schema(SCHEMA_VERSION));

    @Test
    void test_serialization() throws IOException {
        EventNormalizer generator = jsonEventGenerator.createEventStreamEventGenerator("test.event.example", "/test/event/1.1.0");

        JsonRowSerializationSchema serializationSchema = JsonRowSerializationSchema.builder()
                .withNormalizationFunction(generator)
                .withObjectMapper(generator.getObjectMapper())
                .withTypeInfo(typeInfo)
                .build();

        Row r = typeInfo.createEmptyRow();
        r.setField("test", "my_string");
        r.setField("test_int", 123L);
        r.setField("test_decimal", 3.14D);
        Map<String, String> map = new HashMap<>();
        map.put("key1", "value1");
        map.put("key2", "value2");
        r.setField("test_map", map);

        Row elt = typeInfo.createEmptySubRow("test_array");
        List<Row> test_array = new ArrayList<>();
        elt.setField("prop1", "element1");
        test_array.add(elt);
        elt = Row.copy(elt);
        elt.setField("prop1", "element2");
        test_array.add(elt);
        r.setField("test_array", test_array.toArray());

        byte[] bytes = serializationSchema.serialize(r);
        String expectedJson = "{\"test\":\"my_string\"," +
                "\"test_int\":123," +
                "\"test_decimal\":3.14," +
                "\"test_map\":{\"key1\":\"value1\",\"key2\":\"value2\"}," +
                "\"test_array\":[{\"prop1\":\"element1\"},{\"prop1\":\"element2\"}]," +
                "\"$schema\":\"/test/event/1.1.0\"," +
                "\"meta\":{\"dt\":\"1970-01-01T00:02:03Z\",\"id\":\"00000000-0000-0001-0000-000000000002\",\"stream\":\"test.event.example\"}}";
        Assertions.assertThat(mapper.readTree(bytes)).isEqualTo(mapper.readTree(expectedJson));
    }

    @Test
    void test_validation_failure() {
        EventNormalizer generator = jsonEventGenerator.createEventStreamEventGenerator("test.event.example", "/test/event/1.1.0");

        JsonRowSerializationSchema serializationSchema = JsonRowSerializationSchema.builder()
                .withNormalizationFunction(generator)
                .withObjectMapper(generator.getObjectMapper())
                .withTypeInfo(typeInfo)
                .build();

        Row r = typeInfo.createEmptyRow();
        r.setField("test", "my_string");
        r.setField("test_int", 123L);
        r.setField("test_decimal", 3.14D);
        Map<String, String> map = new HashMap<>();
        map.put("key2", "value2");
        r.setField("test_map", map);

        Row elt = typeInfo.createEmptySubRow("test_array");
        List<Row> test_array = new ArrayList<>();
        elt.setField("prop1", "element1");
        test_array.add(elt);
        elt = Row.copy(elt);
        elt.setField("prop1", "element2");
        test_array.add(elt);
        r.setField("test_array", test_array.toArray());

        Assertions.assertThatThrownBy(() -> serializationSchema.serialize(r))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Could not serialize row")
                .getCause()
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("error: object has missing required properties ([\"key1\"])");
    }
}
