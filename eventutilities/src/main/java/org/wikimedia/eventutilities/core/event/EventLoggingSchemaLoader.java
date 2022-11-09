package org.wikimedia.eventutilities.core.event;

import java.net.URI;
import java.net.URISyntaxException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikimedia.eventutilities.core.json.JsonSchemaLoader;
import org.wikimedia.eventutilities.core.json.JsonLoadingException;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Loads EventLogging schemas from schema names (and/or revisions)
 * by extending EventSchemaLoader and adding Mediawiki extension EventLogging
 * schema specific functionality. When looking up schemas for event instances,
 * this will always use the latest schema.
 *
 * Usage:
 * <pre>{@code
 * EventLoggingSchemaLoader schemaLoader = new EventLoggingSchemaLoader();
 *
 * // Load Test schema revision 123
 * schemaLoader.getEventSchema("Test", 123);
 *
 * // Load the schema for JsonNode or Json String event (schema name is at /schema in event).
 * schemaLoader.getEventSchema(event);
 * }</pre>
 */
public class EventLoggingSchemaLoader extends EventSchemaLoader {
    /**
     * EventLogging schema names are in an event's `schema` field.
     */
    protected static final String EVENTLOGGING_SCHEMA_FIELD = "/schema";

    /**
     * EventCapsule schema.
     */
    protected final JsonNode eventLoggingCapsuleSchema;

    private static final Logger LOG = LoggerFactory.getLogger(EventLoggingSchemaLoader.class.getName());

    /**
     * Returns an EventLoggingSchemaLoader that uses {@link JsonSchemaLoader} to load JSONSchemas.
     *
     * @param schemaLoader
     *  must have an underlying ResourceLoader that knows how to load
     *  relative schema URIs, which in EventLogging's case are MediaWiki action API params.
     */
    public EventLoggingSchemaLoader(JsonSchemaLoader schemaLoader) {
        super(schemaLoader, JsonPointer.compile(EVENTLOGGING_SCHEMA_FIELD));
        this.eventLoggingCapsuleSchema = buildEventLoggingCapsule();
    }

    /**
     * Adapted from https://github.com/wikimedia/eventlogging/blob/master/eventlogging/capsule.py.
     */
    protected static JsonNode buildEventLoggingCapsule() {
        JsonNodeFactory jf = JsonNodeFactory.instance;

        ObjectNode capsuleSchema = jf.objectNode();

        ObjectNode userAgentSchema = jf.objectNode();
        userAgentSchema.set("browser_family", jf.objectNode().put("type", "string"));
        userAgentSchema.set("browser_major", jf.objectNode().put("type", "string"));
        userAgentSchema.set("browser_minor", jf.objectNode().put("type", "string"));
        userAgentSchema.set("device_family", jf.objectNode().put("type", "string"));
        userAgentSchema.set("is_bot", jf.objectNode().put("type", "boolean"));
        userAgentSchema.set("is_mediawiki", jf.objectNode().put("type", "boolean"));
        userAgentSchema.set("os_family", jf.objectNode().put("type", "string"));
        userAgentSchema.set("os_major", jf.objectNode().put("type", "string"));
        userAgentSchema.set("os_minor", jf.objectNode().put("type", "string"));
        userAgentSchema.set("wmf_app_version", jf.objectNode().put("type", "string"));

        ObjectNode userAgentField = jf.objectNode();
        userAgentField.put("type", "object");
        userAgentField.set("properties", userAgentSchema);

        ObjectNode capsuleSchemaProperties = jf.objectNode();
        capsuleSchemaProperties.set("ip", jf.objectNode().put("type", "string"));
        capsuleSchemaProperties.set("userAgent", userAgentField);
        capsuleSchemaProperties.set("uuid", jf.objectNode().put("type", "string"));
        capsuleSchemaProperties.set("seqId", jf.objectNode().put("type", "integer"));
        capsuleSchemaProperties.set("dt", jf.objectNode().put("type", "string"));
        capsuleSchemaProperties.set("wiki", jf.objectNode().put("type", "string"));
        capsuleSchemaProperties.set("webHost", jf.objectNode().put("type", "string"));
        capsuleSchemaProperties.set("schema", jf.objectNode().put("type", "string"));
        capsuleSchemaProperties.set("revision", jf.objectNode().put("type", "integer"));
        capsuleSchemaProperties.set("topic", jf.objectNode().put("type", "string"));
        capsuleSchemaProperties.set("recvFrom", jf.objectNode().put("type", "string"));

        capsuleSchema = jf.objectNode();
        capsuleSchema.put("type", "object");
        capsuleSchema.set("properties", capsuleSchemaProperties);
        capsuleSchema.put("additionalProperties", false);

        return capsuleSchema;
    }

    /**
     * Given a URI to an EventLogging 'event' field (un-encapsulated) schema,
     * this will get the 'event' field schema at that URI, and then encapsulate
     * it. This will use the schemaLoader's schema cache to cache the encapsulated schema by an
     * artificial encapsulated schema URI value.
     */
    @Override
    public JsonNode load(URI schemaUri) throws JsonLoadingException {
        URI encapsulatedSchemaUriCacheKey;
        try {
            // Make make an artificial 'encapsulated' URI we can use as a cache key for the
            // encapsulated schema.
            encapsulatedSchemaUriCacheKey = new URI(schemaUri.toString() + "&encapsulated=true");
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Could not create artificial encapsulatedSchemaUri from " + schemaUri, e);
        }

        if (this.schemaLoader.isCached(encapsulatedSchemaUriCacheKey)) {
            return this.schemaLoader.cacheGet(encapsulatedSchemaUriCacheKey);
        }

        JsonNode eventFieldSchema = super.load(schemaUri);
        JsonNode encapsulatedSchema = this.encapsulateEventLoggingSchema(eventFieldSchema);
        this.schemaLoader.cachePut(encapsulatedSchemaUriCacheKey, encapsulatedSchema);
        return encapsulatedSchema;
    }

    /**
     * Returns the latest EventLogging schema URI for this event.
     * @param event should have field at schemaFieldPointer pointing at its URI.
     */
    public URI getEventSchemaUri(JsonNode event) {
        String schemaName = event.at(this.schemaFieldPointer).textValue();
        return this.eventLoggingSchemaUriFor(schemaName);
    }

    /**
     * Given an EventLogging event object, this extracts its schema name at /schema
     * and uses it to get the latest EventLogging schema.
     */
    @Override
    public JsonNode getEventSchema(JsonNode event) throws JsonLoadingException {
        URI schemaUri = this.getEventSchemaUri(event);
        return this.load(schemaUri);
    }

    /**
     * Given an EventLogging json event string, this parses it to a JsonNode and then
     * extracts its schema name at /schema and uses it to get the latest EventLogging schema.
     */
    @Override
    public JsonNode getEventSchema(String eventString) throws JsonLoadingException {
        JsonNode event = this.schemaLoader.parse(eventString);
        return this.getEventSchema(event);
    }

    // EventLoggingSchemaLoader always returns the latest schema.
    @Override
    public JsonNode getLatestEventSchema(JsonNode event) throws JsonLoadingException {
        return getEventSchema(event);
    }

    @Override
    public JsonNode getLatestEventSchema(String eventString) throws JsonLoadingException {
        return getEventSchema(eventString);
    }

    /**
     * Given an EventLogging schema name , this will get the
     * latest schema revision from EVENTLOGGING_SCHEMA_BASE_URI and encapsulate it.
     */
    public JsonNode getEventLoggingSchema(String schemaName) throws JsonLoadingException {
        URI eventFieldSchemaUri = this.eventLoggingSchemaUriFor(schemaName);
        return this.load(eventFieldSchemaUri);
    }

    /**
     * Given an EventLogging event schema name and revision, this will get the
     * schema from EVENTLOGGING_SCHEMA_BASE_URI and encapsulate it.
     * @return event schema
     */
    public JsonNode getEventLoggingSchema(String schemaName, Integer revision) throws JsonLoadingException {
        URI eventFieldSchemaUri = this.eventLoggingSchemaUriFor(schemaName, revision);
        return this.load(eventFieldSchemaUri);
    }

    /**
     * Builds an EventLogging Mediawiki API schema URI for the latest revision.
     * @param name          schema name
     * @return EventLogging schema URI
     */
    protected URI eventLoggingSchemaUriFor(String name) {
        try {
            URI schemaUri = new URI(
                "?action=jsonschema&formatversion=2&format=json" +
                "&title=" + name
            );
            LOG.debug("Built EventLogging schema URI for '{}': {}", name, schemaUri);
            return schemaUri;
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(
                "Could not build EventLogging schema URI for " + name +
                " latest revision.", e
            );
        }
    }

    /**
     * Builds an EventLogging Mediawiki API schema URI for a specific schema revision.
     * @param name          schema name
     * @param revision      schema revision
     * @return EventLogging schema URI
     */
    protected URI eventLoggingSchemaUriFor(String name, Integer revision) {
        try {
            URI schemaUri = new URI(
                "?action=jsonschema&formatversion=2&format=json" +
                "&title=" + name +
                "&revid=" + revision
            );
            LOG.debug("Built EventLogging schema URI for '{}': {}", name, schemaUri);
            return schemaUri;
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(
                "Could not build EventLogging schema URI for " + name +
                " revision " + revision + ".", e
            );
        }
    }

    /**
     * Given an EventLogging schema in an ObjectNode, 'encapsulate' it in the
     * eventLoggingCapsuleSchema the same way that EventLogging python would.
     * @param schema the event schema to be encapsulated. Its 'properties' will be set as 'event'.
     * @return encapsulated EventLogging schema
     */
    protected JsonNode encapsulateEventLoggingSchema(JsonNode schema) {
        ObjectNode schemaObject = (ObjectNode)schema;

        // EventLogging MW API doesn't return event schema with type
        // if user doesn't enter it explicitly.  This happens for most EL schemas.
        if (!schemaObject.has("type")) {
            LOG.trace("EventLogging event schema is missing type; setting type: object.");
            schemaObject.put("type", "object");
        }

        ObjectNode capsule = this.eventLoggingCapsuleSchema.deepCopy();
        ((ObjectNode)capsule.get("properties")).set("event", schemaObject);
        return capsule;
    }

    public String toString() {
        return "EventLoggingSchemaLoader(" + getResourceLoader() + ")";
    }
}
