package org.wikimedia.eventutilities.core.event;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.wikimedia.eventutilities.core.event.JsonEventGenerator.EVENT_TIME_FIELD;
import static org.wikimedia.eventutilities.core.event.JsonEventGenerator.META_FIELD;
import static org.wikimedia.eventutilities.core.event.JsonEventGenerator.META_ID_FIELD;
import static org.wikimedia.eventutilities.core.event.JsonEventGenerator.META_INGESTION_TIME_FIELD;
import static org.wikimedia.eventutilities.core.event.JsonEventGenerator.META_STREAM_FIELD;
import static org.wikimedia.eventutilities.core.event.JsonEventGenerator.SCHEMA_FIELD;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.UUID;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.wikimedia.eventutilities.core.json.JsonLoader;
import org.wikimedia.eventutilities.core.json.JsonSchemaLoader;
import org.wikimedia.eventutilities.core.util.ResourceLoader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class TestJsonEventGenerator {
    private final Instant ingestionTime = Instant.EPOCH.plusMillis(2000);
    private final Instant eventTime = Instant.EPOCH.plusMillis(1000);
    private final String mainStream = "main_stream";
    private final String schema = "/test/event/1.0.0";
    private final JsonEventGenerator generator;
    private final UUID myUuid = UUID.randomUUID();

    public TestJsonEventGenerator() throws URISyntaxException {
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

        generator = JsonEventGenerator.builder()
                .schemaLoader(schemaLoader)
                .eventStreamConfig(streamConfig)
                .ingestionTimeClock(() -> ingestionTime)
                .withUuidSupplier(() -> myUuid)
                .build();
    }

    @Test
    public void test_event_is_generated_with_default_values() {
        Consumer<ObjectNode> eventCreator = root -> {
            root.put("test", "some value");
            ObjectNode someMap = root.putObject("test_map");
            someMap.put("foo", "bar");
        };

        ObjectNode event = generator.generateEvent(mainStream, schema, eventCreator, eventTime);

        assertEquals(schema, event.get(SCHEMA_FIELD).textValue());

        JsonNode meta = event.get(META_FIELD);
        assertNotNull(meta);
        assertEquals(ingestionTime.toString(), meta.get(META_INGESTION_TIME_FIELD).textValue());
        assertEquals(mainStream, meta.get(META_STREAM_FIELD).textValue());
        assertEquals(this.myUuid.toString(), meta.get(META_ID_FIELD).textValue());

        assertEquals("some value", event.get("test").textValue());
        JsonNode map = event.get("test_map");
        assertEquals("bar", map.get("foo").textValue());
        assertEquals(eventTime.toString(), event.get(EVENT_TIME_FIELD).textValue());
    }

    @Test
    public void test_event_is_generated_with_provided_values() {
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

        assertEquals(schema, event.get(SCHEMA_FIELD).textValue());

        JsonNode meta = event.get(META_FIELD);
        assertNotNull(meta);
        assertEquals(customIngestionTime.toString(), meta.get(META_INGESTION_TIME_FIELD).textValue());
        assertEquals(mainStream, meta.get(META_STREAM_FIELD).textValue());
        assertEquals("real_id", meta.get(META_ID_FIELD).textValue());

        assertEquals(customEventTime.toString(), event.get(EVENT_TIME_FIELD).textValue());
    }

    @Test
    public void test_event_is_validated() {
        Consumer<ObjectNode> eventCreator = root -> {};
        try {
            generator.generateEvent(mainStream, schema, eventCreator, null);
        } catch (IllegalArgumentException iae) {
            assertEquals("Cannot validate the generated event", iae.getMessage());
            return;
        }
        throw new AssertionError();
    }

    @Test
    public void test_stream_must_match_schema() {
        Consumer<ObjectNode> eventCreator = root -> {};
        try {
            generator.generateEvent(mainStream, "/random/schema", eventCreator, null);
        } catch (IllegalArgumentException iae) {
            assertEquals("Cannot load schema for event", iae.getMessage());
            return;
        }
        throw new AssertionError();
    }

    @Test
    public void test_stream_must_have_schema_definition() {
        Consumer<ObjectNode> eventCreator = root -> {};
        try {
            generator.generateEvent("unknown_stream", schema, eventCreator, null);
        } catch (IllegalArgumentException iae) {
            assertEquals("Cannot find any schema titles for stream [unknown_stream]", iae.getMessage());
            return;
        }
        throw new AssertionError();
    }

    @Test
    public void test_schema_allowed_for_stream() {
        Consumer<ObjectNode> eventCreator = root -> {};
        try {
            generator.generateEvent("unrelated_stream", schema, eventCreator, null);
        } catch (IllegalArgumentException iae) {
            assertEquals("Schema [" + schema + "] with title " +
                    "[test/event] does not match allowed titles for stream [unrelated_stream]," +
                    " allowed titles are: [unrelated/schema]", iae.getMessage());
            return;
        }
        throw new AssertionError();
    }

    @Test
    public void test_can_be_serialized_as_bytes() throws IOException {
        Consumer<ObjectNode> eventCreator = root -> {
            root.put("test", "some value");
            ObjectNode someMap = root.putObject("test_map");
            someMap.put("foo", "bar");
        };

        ObjectNode event = generator.generateEvent(mainStream, schema, eventCreator, eventTime);
        String actualJson = "{" +
                "\"test\":\"some value\"," +
                "\"test_map\":{\"foo\":\"bar\"}," +
                "\"dt\":\"1970-01-01T00:00:01Z\"," +
                "\"$schema\":\"/test/event/1.0.0\"," +
                "\"meta\":{\"dt\":\"1970-01-01T00:00:02Z\",\"id\":\"" + this.myUuid + "\",\"stream\":\"main_stream\"}}";
        assertEquals(actualJson, new String(generator.serializeAsBytes(event), StandardCharsets.UTF_8));
    }
}
