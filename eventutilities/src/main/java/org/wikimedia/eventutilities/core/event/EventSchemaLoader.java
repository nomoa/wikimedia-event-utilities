package org.wikimedia.eventutilities.core.event;

import java.net.URI;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.wikimedia.eventutilities.core.json.JsonLoader;
import org.wikimedia.eventutilities.core.json.JsonLoadingException;
import org.wikimedia.eventutilities.core.json.JsonSchemaLoader;
import org.wikimedia.eventutilities.core.util.ResourceLoader;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import com.google.common.base.Preconditions;

/**
 * Class to load and cache JSONSchema JsonNodes from relative schema URIs and event data.
 *
 * Usage:
 * <code>
 * EventSchemaLoader schemaLoader = EventSchemaLoader.builder()
 *  .setJsonSchemaLoader(ResourceLoader.asURLs(Arrays.asList(
 *      "file:///path/to/schemas1",
 *      "http://schema.repo.org/path/to/schemas"
 *  )))
 *  .build();
 *
 * // Load the JSONSchema at file:///path/to/schemas/test/event/0.0.2
 * schemaLoader.getEventSchema("/test/event/0.0.2");
 *
 * // Load the JsonNode or JSON String event's JSONSchema at /$schema
 * schemaLoader.getEventSchema(event);
 * </code>
 */
public class EventSchemaLoader {

    /**
     * When looking up latest schema versions, this will be used instead of a semver string.
     */
    protected static final String LATEST_FILE_NAME = "latest";

    /**
     * Field in an event from which to extract the schema URI.
     */
    protected final JsonPointer schemaFieldPointer;

    /**
     * JsonSchemaLoader used to load schemas from URIs.
     */
    protected final JsonSchemaLoader schemaLoader;

    /**
     * Used to parse a {@link JsonNode} into a {@link JsonSchema} that can be used for validation.
     */
    private static final JsonSchemaFactory jsonSchemaFactory = JsonSchemaFactory.byDefault();

    private static final Logger log = LogManager.getLogger(EventSchemaLoader.class.getName());

    /**
     * Constructs a EventSchemaLoader that prefixes URIs with baseURI and
     * extracts schema URIs from the schemaField in events.
     * This is protected; use {@link EventSchemaLoader.Builder}.
     */
    protected EventSchemaLoader(JsonSchemaLoader loader, JsonPointer schemaFieldPointer) {
        this.schemaFieldPointer = schemaFieldPointer;
        this.schemaLoader = loader;
    }

    public static class Builder {
        private static final String SCHEMA_FIELD_DEFAULT = "/$schema";

        private JsonSchemaLoader jsonSchemaLoader;
        private String schemaField = SCHEMA_FIELD_DEFAULT;

        /**
         * Sets the {@link JsonSchemaLoader}.
         */
        public Builder setJsonSchemaLoader(JsonSchemaLoader schemaLoader) {
            this.jsonSchemaLoader = schemaLoader;
            return this;
        }

        /**
         * Sets the schema field from which a schema URI will be extracted from events.
         */
        public Builder setSchemaField(String schemaField) {
            this.schemaField = schemaField;
            return this;
        }

        /**
         * Retuns a new EventSchemaLoader.
         */
        public EventSchemaLoader build() {
            Preconditions.checkState(
                    jsonSchemaLoader != null,
                    "Must call setJsonSchemaLoader() before calling build().");

            return new EventSchemaLoader(jsonSchemaLoader, JsonPointer.compile(schemaField));
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns the content at schemaURI as a JsonNode JSONSchema.
     *
     * @return the jsonschema at schemaURI.
     */
    public JsonNode load(URI schemaUri) throws JsonLoadingException {
        log.debug("Loading event schema at {}", schemaUri);
        return schemaLoader.load(schemaUri);
    }

    /**
     * Extracts the value at schemaFieldPointer from the event as a URI.
     */
    public URI extractSchemaUri(JsonNode event) {
        String uriString = event.at(schemaFieldPointer).textValue();
        Preconditions.checkNotNull(
                uriString,
                "Could not extract %s field from event, field does not exist", schemaFieldPointer
        );
        try {
            return new URI(uriString);
        } catch (java.net.URISyntaxException e) {
            throw new RuntimeException(
                    "Failed building new URI from " + uriString + ". " + e.getMessage(), e
            );
        }
    }

    /**
     * Converts the given schemaUri to a 'latest' schema URI.  E.g.
     * "/my/schema/1.0.0" returns "/my/schema/latest".
     */
    public URI getLatestSchemaUri(URI schemaUri) {
        return schemaUri.resolve(LATEST_FILE_NAME);
    }

    /**
     * Extracts the event's schema URI and converts it to a latest schema URI.
     * @return 'latest' version of this event's schema URI
     */
    public URI getLatestSchemaUri(JsonNode event) {
        return getLatestSchemaUri(extractSchemaUri(event));
    }

    /**
     * Get a JsonSchema at schemaUri looking in all baseUris.
     */
    public JsonNode getSchema(URI schemaUri) throws JsonLoadingException {
        return this.load(schemaUri);
    }

    /**
     * Get a 'latest' JsonSchema given a schema URI looking in all baseUris.
     */
    public JsonNode getLatestSchema(URI schemaUri) throws JsonLoadingException {
        return this.load(getLatestSchemaUri(schemaUri));
    }


//    GET SCHEMA FROM EVENT or EVENT STRING

    /**
     * Given an event object, this extracts its schema URI at schemaField
     * (prefixed with baseURI) and returns the schema there.
     */
    public JsonNode getEventSchema(JsonNode event) throws JsonLoadingException {
        return this.getSchema(extractSchemaUri(event));
    }

    /**
     * Given a JSON event string, get its schema URI,
     * and load and return schema for the event.
     */
    public JsonNode getEventSchema(String eventString) throws JsonLoadingException {
        JsonNode event = this.schemaLoader.parse(eventString);
        return getEventSchema(event);
    }

    /**
     * Given a JSON event, get its schema URI,
     * and load and return schema for the event as a JsonSchema (suited for validation).
     */
    public JsonSchema getEventJsonSchema(String eventString) throws ProcessingException, JsonLoadingException {
        return getEventJsonSchema(this.schemaLoader.parse(eventString));
    }

    /**
     * Given a JSON event, get its schema URI,
     * and load and return schema for the event as a JsonSchema (suited for validation).
     */
    public JsonSchema getEventJsonSchema(JsonNode event) throws ProcessingException, JsonLoadingException {
        JsonNode schemaAsJson = this.getEventSchema(event);
        return getJsonSchema(schemaAsJson);
    }

    /**
     * Given a json schema parsed as a JsonNode materialize JsonSchema suited for validation.
     */
    public JsonSchema getJsonSchema(JsonNode schema) throws ProcessingException {
        return jsonSchemaFactory.getJsonSchema(schema);
    }

    /**
     * Given an event object, this extracts its schema URI at schemaField
     * (prefixed with baseURI) and resolves it to the latest schema URI and returns
     * the schema there.
     */
    public JsonNode getLatestEventSchema(JsonNode event) throws JsonLoadingException {
        return getSchema(getLatestSchemaUri(event));
    }

    /**
     * Given a JSON event string, get its schema URI,
     * and load and return latest schema for the event.
     */
    public JsonNode getLatestEventSchema(String eventString) throws JsonLoadingException {
        JsonNode event = schemaLoader.parse(eventString);
        return getLatestEventSchema(event);
    }

    public JsonLoader getJsonLoader() {
        return schemaLoader.getJsonLoader();
    }

    public ResourceLoader getResourceLoader() {
        return getJsonLoader().getResourceLoader();
    }

    public String getLatestVersionFileName() {
        return LATEST_FILE_NAME;
    }

    public String toString() {
        return "EventSchemaLoader(" + getResourceLoader() + ", " + schemaFieldPointer + ")";
    }
}
