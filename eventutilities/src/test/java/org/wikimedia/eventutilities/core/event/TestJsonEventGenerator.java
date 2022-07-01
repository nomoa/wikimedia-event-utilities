package org.wikimedia.eventutilities.core.event;

import static java.util.Collections.singletonList;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.wikimedia.eventutilities.core.SerializableClock.frozenClock;
import static org.wikimedia.eventutilities.core.event.JsonEventGenerator.EVENT_TIME_FIELD;
import static org.wikimedia.eventutilities.core.event.JsonEventGenerator.META_FIELD;
import static org.wikimedia.eventutilities.core.event.JsonEventGenerator.META_ID_FIELD;
import static org.wikimedia.eventutilities.core.event.JsonEventGenerator.META_INGESTION_TIME_FIELD;
import static org.wikimedia.eventutilities.core.event.JsonEventGenerator.META_STREAM_FIELD;
import static org.wikimedia.eventutilities.core.event.JsonEventGenerator.SCHEMA_FIELD;

import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.Collections;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.commons.lang3.SerializationUtils;
import org.junit.jupiter.api.Test;
import org.wikimedia.eventutilities.core.json.JsonLoader;
import org.wikimedia.eventutilities.core.json.JsonSchemaLoader;
import org.wikimedia.eventutilities.core.util.ResourceLoader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class TestJsonEventGenerator {
    private static final Instant INGESTION_TIME = Instant.EPOCH.plusMillis(2000);
    private static final UUID MY_UUID = UUID.randomUUID();
    private final Instant eventTime = Instant.EPOCH.plusMillis(1000);
    private final String mainStream = "main_stream";
    private final String schema = "/test/event/1.0.0";
    private final JsonEventGenerator generator;
    private final ObjectMapper objectMapper = new ObjectMapper();


    ResourceLoader resourceLoader = ResourceLoader.builder()
            .setBaseUrls(singletonList(this.getClass().getResource("/event-schemas/repo4")))
            .build();
    JsonLoader jsonLoader = new JsonLoader(resourceLoader);
    EventStreamConfig streamConfig = new EventStreamConfig(
            new StaticEventStreamConfigLoader(this.getClass().getResource("/TestJsonEventGenerator-stream-config.json").toURI(), jsonLoader),
            Collections.emptyMap());
    EventSchemaLoader schemaLoader = EventSchemaLoader.builder()
            .setJsonSchemaLoader(JsonSchemaLoader.build(resourceLoader))
            .build();

    private void feedEvent(ObjectNode root) {
        root.put("test", "some value");
        ObjectNode someMap = root.putObject("test_map");
        someMap.put("foo", "bar");
    };

    private void feedBrokenEvent(ObjectNode node) {}

    public TestJsonEventGenerator() throws URISyntaxException {
        generator = JsonEventGenerator.builder()
                .schemaLoader(schemaLoader)
                .eventStreamConfig(streamConfig)
                .ingestionTimeClock(frozenClock(INGESTION_TIME))
                .withUuidSupplier((Supplier<UUID> & Serializable) () -> MY_UUID)
                .jsonMapper(objectMapper)
                .build();
    }

    private void assertMainEvent(JsonNode event) {
        assertThatJson(event).isObject()
                .containsEntry(SCHEMA_FIELD, schema)
                .containsEntry("test", "some value")
                .containsEntry(EVENT_TIME_FIELD, eventTime.toString());

        assertThatJson(event).node(META_FIELD)
                .isObject()
                .containsEntry(META_INGESTION_TIME_FIELD, INGESTION_TIME.toString())
                .containsEntry(META_STREAM_FIELD, mainStream)
                .containsEntry(META_ID_FIELD, MY_UUID.toString());

        assertThatJson(event).node("test_map")
                .isObject()
                .containsEntry("foo", "bar");
    }

    @Test
    void test_event_is_generated_with_default_values() {
        ObjectNode event = generator.generateEvent(mainStream, schema, this::feedEvent, eventTime);
        assertMainEvent(event);
    }

    @Test
    void test_event_is_generated_with_provided_values() {
        final Instant customIngestionTime = Instant.EPOCH.plusMillis(4000);
        final Instant customEventTime = Instant.EPOCH.plusMillis(3000);
        final String customStream = "custom_stream";
        Consumer<ObjectNode> eventCreator = root -> {
            ObjectNode meta = root.putObject(META_FIELD);
            meta.put(META_INGESTION_TIME_FIELD, customIngestionTime.toString());
            // customStream will be ignored
            meta.put(META_STREAM_FIELD, customStream);
            meta.put(META_ID_FIELD, "real_id");
            root.put(EVENT_TIME_FIELD, customEventTime.toString());
            ObjectNode someMap = root.putObject("test_map");
            root.put("test", "some value");
            someMap.put("foo", "bar");
            // we can't force a schema this way either
            root.put(SCHEMA_FIELD, "/custom/schema");
        };

        ObjectNode event = generator.generateEvent(mainStream, schema, eventCreator, eventTime);

        assertThatJson(event).isObject()
                .containsEntry(SCHEMA_FIELD, schema)
                .containsEntry(EVENT_TIME_FIELD, customEventTime.toString());

        assertThatJson(event).node(META_FIELD)
                .isObject()
                .containsEntry(META_INGESTION_TIME_FIELD, customIngestionTime.toString())
                .containsEntry(META_STREAM_FIELD, mainStream)
                .containsEntry(META_ID_FIELD, "real_id");
    }

    @Test
    void test_event_stream_generator() {
        JsonEventGenerator.EventNormalizer eventStreamEventGenerator = generator.createEventStreamEventGenerator(mainStream, schema);

        assertThat(eventStreamEventGenerator.getObjectMapper()).isSameAs(objectMapper);
        ObjectNode event = eventStreamEventGenerator.generateEvent(this::feedEvent, eventTime);

        assertMainEvent(event);
    }

    @Test
    void test_event_stream_generator_is_serializable() {
        JsonEventGenerator.EventNormalizer origin = generator.createEventStreamEventGenerator(mainStream, schema);
        JsonEventGenerator.EventNormalizer deserialized = SerializationUtils.deserialize(SerializationUtils.serialize(origin));
        ObjectNode event = deserialized.generateEvent(this::feedEvent, eventTime);
        assertMainEvent(event);
    }

    @Test
    void test_default_event_stream_generator_is_serializable() {
        JsonEventGenerator defaultGenerator = JsonEventGenerator.builder()
                .schemaLoader(schemaLoader)
                .eventStreamConfig(streamConfig)
                .build();

        JsonEventGenerator.EventNormalizer origin = defaultGenerator.createEventStreamEventGenerator(mainStream, schema);
        JsonEventGenerator.EventNormalizer deserialized = SerializationUtils.deserialize(SerializationUtils.serialize(origin));
        ObjectNode fromDeser = deserialized.generateEvent(this::feedEvent, eventTime);
        assertThatJson(fromDeser).isObject()
                .containsEntry(SCHEMA_FIELD, schema);
        assertThatJson(fromDeser).node(META_FIELD)
                .isObject()
                .containsEntry(META_STREAM_FIELD, mainStream);

    }

    @Test
    void test_event_is_validated() {
        assertThatThrownBy(() -> generator.generateEvent(mainStream, schema, this::feedBrokenEvent, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Cannot validate the generated event");
    }

    @Test
    void test_stream_must_match_schema() {
        Consumer<ObjectNode> eventCreator = root -> {};
        assertThatThrownBy(() -> generator.generateEvent(mainStream, "/random/schema", eventCreator, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cannot load schema for event");
    }

    @Test
    void test_stream_must_have_schema_definition() {
        Consumer<ObjectNode> eventCreator = root -> {};
        assertThatThrownBy(() -> generator.generateEvent("unknown_stream", schema, eventCreator, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cannot find any schema titles for stream [unknown_stream]");
    }

    @Test
    void test_schema_allowed_for_stream() {
        Consumer<ObjectNode> eventCreator = root -> {};
        try {
            generator.generateEvent("unrelated_stream", schema, eventCreator, null);
        } catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage()).startsWith("Schema [" + schema + "] with title " +
                    "[test/event] does not match allowed titles for stream [unrelated_stream]," +
                    " allowed titles are: [unrelated/schema]");
            return;
        }
        throw new AssertionError();
    }

    @Test
    void test_can_be_serialized_as_bytes() throws IOException {
        ObjectNode sourceEvent = generator.generateEvent(mainStream, schema, this::feedEvent, eventTime);
        byte[] bytes = generator.serializeAsBytes(sourceEvent);
        JsonNode event = objectMapper.readTree(bytes);
        assertMainEvent(event);

    }

    @Test
    void test_non_validating_generator() {
        assertThatThrownBy(() -> generator.generateEvent(mainStream, schema, this::feedBrokenEvent, null, true)).isInstanceOf(IllegalArgumentException.class);
        generator.generateEvent(mainStream, schema, this::feedBrokenEvent, null, false);
    }
}
