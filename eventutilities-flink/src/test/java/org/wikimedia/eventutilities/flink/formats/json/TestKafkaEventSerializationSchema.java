package org.wikimedia.eventutilities.flink.formats.json;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.wikimedia.eventutilities.core.SerializableClock.frozenClock;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema.KafkaSinkContext;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.wikimedia.eventutilities.core.event.EventStream;
import org.wikimedia.eventutilities.core.event.EventStreamFactory;
import org.wikimedia.eventutilities.core.event.JsonEventGenerator;
import org.wikimedia.eventutilities.flink.EventRowTypeInfo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

class TestKafkaEventSerializationSchema {

    private static final UUID MY_UUID = new UUID(1, 2);
    private static final Instant INGESTION_TIME = Instant.ofEpochSecond(123);
    private static final Instant FLINK_EVENT_TIME = Instant.ofEpochSecond(124);
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
            .ingestionTimeClock(frozenClock(INGESTION_TIME))
            .build();

    private final EventStream eventStream = eventStreamFactory.createEventStream(STREAM_NAME);
    private final EventRowTypeInfo typeInfo = JsonSchemaFlinkConverter.toRowTypeInfo((ObjectNode) eventStream.schema(SCHEMA_VERSION));

    private static final int PARTITION = 123;
    private static final String TOPIC = "eqiad.test.event.example";

    private final FlinkKafkaPartitioner<Row> partitioner = new FlinkKafkaPartitioner<Row>() {
        @Override
        public int partition(Row row, byte[] key, byte[] value, String targetTopic, int[] partitions) {
            assertThat(key).isNull();
            assertThat(targetTopic).isEqualTo(TOPIC);
            assertThat(partitions).hasSize(PARTITION);
            return PARTITION;
        }
    };

    private final KafkaSinkContext kafkaSinkContext = new KafkaSinkContext() {

        @Override
        public int getParallelInstanceId() {
            return 1;
        }

        @Override
        public int getNumberOfParallelInstances() {
            return 2;
        }

        @Override
        public int[] getPartitionsForTopic(String topic) {
            return new int[PARTITION];
        }
    };

    @Test
    void test_serialization() throws IOException {
        JsonEventGenerator.EventNormalizer generator = jsonEventGenerator.createEventStreamEventGenerator(STREAM_NAME, "/test/event/1.1.0");
        JsonRowSerializationSchema jsonRowSerializationSchema = JsonRowSerializationSchema.builder()
                .withNormalizationFunction(generator)
                .withObjectMapper(generator.getObjectMapper())
                .withTypeInfo(typeInfo)
                .build();
        KafkaEventSerializationSchema serializationSchema = new KafkaEventSerializationSchema(
                typeInfo,
                jsonRowSerializationSchema,
                frozenClock(INGESTION_TIME),
                TOPIC,
                KafkaRecordTimestampStrategy.FLINK_RECORD_EVENT_TIME,
                partitioner
        );

        Row r = typeInfo.createEmptyRow();
        r.setField("test", "my_string");
        r.setField("test_int", 123L);
        r.setField("test_decimal", 3.14D);
        Map<String, String> map = new HashMap<>();
        map.put("key1", "value1");
        map.put("key2", "value2");
        r.setField("test_map", map);

        Row elt = typeInfo.createEmptySubRow("test_array");
        List<Row> testArray = new ArrayList<>();
        elt.setField("prop1", "element1");
        testArray.add(elt);
        elt = Row.copy(elt);
        elt.setField("prop1", "element2");
        testArray.add(elt);
        r.setField("test_array", testArray.toArray());

        ProducerRecord<byte[], byte[]> row = serializationSchema.serialize(r, kafkaSinkContext, FLINK_EVENT_TIME.toEpochMilli());
        assertThat(row.partition()).isEqualTo(PARTITION);
        assertThat(row.topic()).isEqualTo(TOPIC);
        assertThat(row.key()).isNull();
        assertThat(row.timestamp()).isEqualTo(FLINK_EVENT_TIME.toEpochMilli());
        String expectedJson = "{\"test\":\"my_string\"," +
                "\"test_int\":123," +
                "\"test_decimal\":3.14," +
                "\"test_map\":{\"key1\":\"value1\",\"key2\":\"value2\"}," +
                "\"test_array\":[{\"prop1\":\"element1\"},{\"prop1\":\"element2\"}]," +
                "\"dt\":\"1970-01-01T00:02:04Z\"," +
                "\"$schema\":\"/test/event/1.1.0\"," +
                "\"meta\":{\"dt\":\"1970-01-01T00:02:03Z\",\"id\":\"00000000-0000-0001-0000-000000000002\",\"stream\":\"test.event.example\"}}";
        assertThat(mapper.readTree(row.value())).isEqualTo(mapper.readTree(expectedJson));
    }
}
