package org.wikimedia.eventutilities.core.event;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.net.URI;
import java.util.Collections;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.wikimedia.eventutilities.core.util.ResourceLoader;
import org.wikimedia.eventutilities.core.json.JsonLoader;
import org.wikimedia.eventutilities.core.json.JsonSchemaLoader;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.JsonNode;

public class TestEventLoggingSchemaLoader {

    private EventLoggingSchemaLoader schemaLoader;

    private static String schemaBaseUri = new File("src/test/resources/event-schemas/repo1").getAbsolutePath();

    private static final JsonNodeFactory jf = JsonNodeFactory.instance;

    private static final ObjectNode expectedEchoSchema = jf.objectNode();
    private static final ObjectNode expectedEncapsulatedEchoSchema;
    private static final ObjectNode expectedEventLoggingTestSchema = jf.objectNode();
    private static final ObjectNode expectedEncapsulatedEventLoggingTestSchema;

    private static final ObjectNode testEchoEvent = jf.objectNode();

    // testEvent is not used unless you uncomment the remote schema tests.
    private static final ObjectNode testEvent = jf.objectNode();

    static {
        // Build the expected encapsulated EventLogging event schema
        ObjectNode version = jf.objectNode();
        version.put("type", "string");
        version.put("required", true);
        version.put("description", "References the full specifications of the current version of Echo, example: 1.1");
        ObjectNode revisionId = jf.objectNode();
        revisionId.put("type", "integer");
        revisionId.put("description", "Revision ID of the edit that the event is for");
        ObjectNode expectedEchoEventProperties = jf.objectNode();
        expectedEchoEventProperties.set("version", version);
        expectedEchoEventProperties.set("revisionId", revisionId);

        expectedEchoSchema.put("title", "Echo");
        expectedEchoSchema.put("description", "Logs events related to the generation of notifications via the Echo extension");
        expectedEchoSchema.put("type", "object");
        expectedEchoSchema.set("properties", expectedEchoEventProperties);

        expectedEncapsulatedEchoSchema = (ObjectNode)EventLoggingSchemaLoader.buildEventLoggingCapsule();
        ((ObjectNode)expectedEncapsulatedEchoSchema.get("properties")).set("event", expectedEchoSchema);

        // Build the test Echo event
        ObjectNode testEchoEventField = jf.objectNode();
        testEchoEventField.put("version", "1.1");
        testEchoEventField.put("revisionId", 123);
        testEchoEvent.put("schema", "Echo");
        testEchoEvent.put("version", 7731316);
        testEchoEvent.set("event", testEchoEventField);

        // Build the expected encapsulated REMOTE EventLogging schema.
        // NOTE: The test that uses this is commented out.
        // That test requests the schema over HTTP at https://meta.wikimedia.org/w/api.php.
        // Uncomment if you want to test locally.
        ObjectNode otherMessage = jf.objectNode();
        otherMessage.put("type", "string");
        otherMessage.put("description", "Free-form text");
        otherMessage.put("required", true);
        ObjectNode eventLoggingTestSchemaProperties = jf.objectNode();
        eventLoggingTestSchemaProperties.set("OtherMessage", otherMessage);
        expectedEventLoggingTestSchema.put("type", "object");
        expectedEventLoggingTestSchema.put("title", "Test");
        expectedEventLoggingTestSchema.put("description", "Test schema for checking that EventLogging works");
        expectedEventLoggingTestSchema.set("properties", eventLoggingTestSchemaProperties);

        expectedEncapsulatedEventLoggingTestSchema = (ObjectNode)EventLoggingSchemaLoader.buildEventLoggingCapsule();
        ObjectNode capsuleProperties = (ObjectNode)expectedEncapsulatedEventLoggingTestSchema.get("properties");
        capsuleProperties.set("event", (JsonNode)expectedEventLoggingTestSchema);

        // build the test event with a remote schema.
        // This is only used if you uncomment remote tests.
        ObjectNode testEventField = jf.objectNode();
        testEventField.put("OtherMessage", "yoohoo");
        testEvent.put("schema", "Test");
        testEvent.set("event", testEventField);
    }


    @BeforeEach
    public void setUp() {
        schemaLoader = new EventLoggingSchemaLoader(
            new JsonSchemaLoader(new JsonLoader(
            ResourceLoader.builder()
                .setBaseUrls(ResourceLoader.asURLs(Collections.singletonList("file://" + schemaBaseUri + "/")))
                .build()
            ))
        );
    }

    @Test
    public void load() throws Exception {
        URI echoSchemaUri = new URI("file://" + schemaBaseUri + "/Echo_7731316.schema.json");
        JsonNode echoSchema = schemaLoader.load(echoSchemaUri);
        assertEquals(
            expectedEncapsulatedEchoSchema,
            echoSchema,
            "test event schema should load from json at " + echoSchemaUri
        );
    }

    @Test
    public void getEventLoggingSchemaUri() {
        URI uri = schemaLoader.eventLoggingSchemaUriFor(
            "Echo",
            7731316
        );

        assertEquals(
            "?action=jsonschema&formatversion=2&format=json&title=Echo&revid=7731316",
            uri.toString(),
            "Should return an EventLogging schema URI with revid"
        );
    }

    @Test
    public void getEventLoggingLatestSchemaUri() {
        URI uri = schemaLoader.eventLoggingSchemaUriFor(
            "Echo"
        );

        assertEquals(
            "?action=jsonschema&formatversion=2&format=json&title=Echo",
            uri.toString(),
            "Should return an EventLogging schema URI without revid"
        );
    }

    @Test
    public void schemaURIFromEvent() throws Exception {
        URI expectedSchemaUri = new URI("?action=jsonschema&formatversion=2&format=json&title=Echo");
        URI uri = schemaLoader.getEventSchemaUri(testEchoEvent);
        assertEquals(
            expectedSchemaUri,
            uri,
            "Should load schema URI from event schema field"
        );
    }

    // /**
    //  * NOTE: This test will actually perform a remote HTTP lookup to the Test EventLogging schema
    //  * hosted at https://meta.wikimedia.org/w/api.php.
    //  * @throws Exception
    //  */
    // @Test
    // public void getRemoteSchema() throws Exception {
    //     EventLoggingSchemaLoader remoteSchemaLoader = new EventLoggingSchemaLoader();
    //     JsonNode eventLoggingTestSchema = remoteSchemaLoader.getEventLoggingSchema("Test", 15047841);
    //     assertEquals(expectedEncapsulatedEventLoggingTestSchema, eventLoggingTestSchema);
    // }

    // /**
    //  * NOTE: This test will actually perform a remote HTTP lookup to the Test EventLogging schema
    //  * hosted at https://meta.wikimedia.org/w/api.php.
    //  * @throws Exception
    //  */
    // @Test
    // public void getRemoteSchemaFromEvent() throws Exception {
    //     EventLoggingSchemaLoader remoteSchemaLoader = new EventLoggingSchemaLoader();

    //     JsonNode schema = remoteSchemaLoader.getEventSchema(testEvent);
    //     assertEquals(
    //         expectedEncapsulatedEventLoggingTestSchema,
    //         schema,
    //         "Should load schema from event schema field"
    //     );
    // }

}

