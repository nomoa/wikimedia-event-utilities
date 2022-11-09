package org.wikimedia.eventutilities.core.event;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.wikimedia.eventutilities.core.util.ResourceLoader;
import org.wikimedia.eventutilities.core.json.JsonLoader;
import org.wikimedia.eventutilities.core.json.JsonLoadingException;
import org.wikimedia.eventutilities.core.json.JsonSchemaLoader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;

public class TestEventSchemaLoader {
    private EventSchemaLoader schemaLoader;

    private static final List<String> schemaBaseUris = new ArrayList<>(Arrays.asList(
        "file://" + new File("src/test/resources/event-schemas/repo1").getAbsolutePath(),
        "file://" + new File("src/test/resources/event-schemas/repo2").getAbsolutePath()
    ));

    private static final JsonNodeFactory jf = JsonNodeFactory.instance;

    private static final ObjectNode expectedTestSchema = jf.objectNode();
    private static final JsonSchema expectedTestJsonSchema;

    private static final ObjectNode testEvent = jf.objectNode();

    static {
        // Build the expected test event schema
        ObjectNode dt = jf.objectNode();
        dt.put("type", "string");
        dt.put("format", "date-time");
        dt.put("maxLength", 26);
        dt.put("description", "the time stamp of the event, in ISO8601 format");
        ObjectNode stream = jf.objectNode();
        stream.put("type", "string");
        stream.put("minLength", 1);
        stream.put("description", "The name of the stream/queue that this event belongs in.");
        ObjectNode metaProperties = jf.objectNode();
        metaProperties.set("dt", dt);
        metaProperties.set("stream", stream);
        ArrayNode metaRequired = jf.arrayNode();
        metaRequired.add(new TextNode("dt"));
        metaRequired.add(new TextNode("stream"));
        ObjectNode meta = jf.objectNode();
        meta.put("type", "object");
        meta.set("properties", metaProperties);
        meta.set("required", metaRequired);
        ObjectNode schema = jf.objectNode();
        schema.put("type", "string");
        schema.put("description", "The URI identifying the jsonschema for this event.");
        ObjectNode testField = jf.objectNode();
        testField.put("type", "string");
        testField.put("default", "default test value");
        ObjectNode expectedTestSchemaProperties = jf.objectNode();
        expectedTestSchemaProperties.set("$schema", schema);
        expectedTestSchemaProperties.set("meta", meta);
        expectedTestSchemaProperties.set("test", testField);
        expectedTestSchema.put("title", "test_event");
        expectedTestSchema.put("$id", "/test_event.schema");
        expectedTestSchema.put("$schema", "http://json-schema.org/draft-07/schema#");
        expectedTestSchema.put("type", "object");
        expectedTestSchema.set("properties", expectedTestSchemaProperties);

        try {
            expectedTestJsonSchema = JsonSchemaFactory.byDefault().getJsonSchema(expectedTestSchema);
        } catch (ProcessingException e) {
            throw new AssertionError(e);
        }

        ObjectNode eventMeta = jf.objectNode();
        eventMeta.put("dt", "2019-01-01T00:00:00Z");
        eventMeta.put("stream", "test.event");
        // Build the expected test event with $schema set to test event schema URI
        testEvent.put("$schema", "/test_event.schema.yaml");
        testEvent.set("meta", eventMeta);
        // Include unicode characters in the test event that are not allowed in yaml.
        // An event should be parsed using JsonParser instead of YAMLParser.
        // https://phabricator.wikimedia.org/T227484
        testEvent.put("test", "yoohoo \uD862\uDF4E");
    }

    @BeforeEach
    public void setUp() {
        JsonLoader loader = new JsonLoader(ResourceLoader.builder().build());
        schemaLoader = EventSchemaLoader.builder()
            .setJsonSchemaLoader(new JsonSchemaLoader(new JsonLoader(
                ResourceLoader.builder()
                    .withHttpClient()
                    .setBaseUrls(ResourceLoader.asURLs(schemaBaseUris))
                    .build()
            )))
            .build();
    }

    @Test
    public void load() throws URISyntaxException, JsonLoadingException {
        URI testSchemaUri = new URI(schemaBaseUris.get(1) + "/test_event.schema.yaml");
        JsonNode testSchema = schemaLoader.load(testSchemaUri);
        assertEquals(
            expectedTestSchema,
            testSchema
        );
    }

    @Test
    public void getEventSchema() throws JsonLoadingException {
        JsonNode testSchema = schemaLoader.getEventSchema(testEvent);
        assertEquals(
            expectedTestSchema,
            testSchema,
            "Should load schema from event $schema field"
        );
    }

    @Test
    public void getEventSchemaFromJsonString() throws JsonLoadingException {

        JsonNode testSchema = schemaLoader.getEventSchema(testEvent.toString());
        assertEquals(
            expectedTestSchema,
            testSchema,
            "Should load schema from JSON string event $schema field"
        );
    }

    @Test
    public void getLatestEventSchema() throws JsonLoadingException {
        JsonNode testSchema = schemaLoader.getLatestEventSchema(testEvent);
        assertEquals(
            expectedTestSchema,
            testSchema,
            "Should load latest schema from event $schema field"
        );
    }

    @Test
    public void getLatestEventSchemaFromJsonString() throws JsonLoadingException {
        JsonNode testSchema = schemaLoader.getLatestEventSchema(testEvent.toString());
        assertEquals(
            expectedTestSchema,
            testSchema,
            "Should load latest schema from JSON string event $schema field"
        );
    }

    @Test
    public void nonExistentSchemaUri() throws URISyntaxException {
        String schemaUri = "/non_existent_schema.yaml";
        try {
            schemaLoader.load(new URI(schemaBaseUris.get(0) + schemaUri));
            fail(
                "Expected to throw JsonSchemaLoaderException when loading non existent schema URI."
            );
        } catch (JsonLoadingException e) {
            // we should get here.
        }
    }

    @Test
    public void testGetEventJsonSchema() throws ProcessingException, JsonLoadingException {
        // JsonSchema does not implement hashCode/equals
        // so we verify that the validation report of schemaLoader.getEventJsonSchema is similar
        // to one produced by the JsonSchema we expect
        assertEquals(schemaLoader.getEventJsonSchema(testEvent).validate(testEvent).toString(),
                expectedTestJsonSchema.validate(testEvent).toString());
    }
}
