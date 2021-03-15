package org.wikimedia.eventutilities.core.json;

import java.net.URI;
import java.util.concurrent.ConcurrentHashMap;

import org.wikimedia.eventutilities.core.util.ResourceLoader;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jsonschema.core.load.SchemaLoader;

/**
 * Uses a {@link JsonLoader} to fetch JSON schemas from URIs and cache them.
 * Usage:
 *
 * <code>
 * JsonSchemaLoader schemaLoader = JsonSchemaLoader(schemaBaseURLs);
 * JsonNode schema = schemaLoader.load("http://my.schemas.org/schemas/test/event/schema/0.0.2")
 * </code>
 */
public class JsonSchemaLoader {

    /**
     * Cache of schema URI to JsonNode JSON schemas.
     */
    private final ConcurrentHashMap<URI, com.fasterxml.jackson.databind.JsonNode> cache = new ConcurrentHashMap<>();

    /**
     * fge SchemaLoader, used to resolve JSON $ref pointers.
     */
    private final SchemaLoader schemaLoader = new SchemaLoader();

    /**
     * Underlying JsonLoader.
     */
    private final JsonLoader jsonLoader;

    /**
     * Make a new JsonSchemaLoader using {@link JsonLoader}.
     */
    public JsonSchemaLoader(JsonLoader loader) {
        this.jsonLoader = loader;
    }

    /**
     * Creates a new JsonLoader using resourceLoader and uses that to create a new JsonSchemaLoader.
     */
    public static JsonSchemaLoader build(ResourceLoader resourceLoader) {
        return new JsonSchemaLoader(new JsonLoader(resourceLoader));
    }

    /**
     * Given a schemaUri, this will request the JSON or YAML content at that URI and
     * parse it into a JsonNode.  $refs will be resolved.
     * The compiled schema will be cached by schemaURI, and only looked up once per schemaURI.
     *
     * @return the jsonschema at schemaURI.
     */
    public JsonNode load(URI schemaUri) throws JsonLoadingException {
        if (this.cache.containsKey(schemaUri)) {
            return this.cache.get(schemaUri);
        }

        // Use SchemaLoader so we resolve any JsonRefs in the JSONSchema.
        JsonNode schema = this.schemaLoader.load(jsonLoader.load(schemaUri)).getBaseNode();
        this.cache.put(schemaUri, schema);
        return schema;
    }

    /**
     * Parses the JSON or YAML string into a JsonNode.
     * @param data JSON or YAML string to parse into a JsonNode.
     */
    public JsonNode parse(String data) throws JsonLoadingException {
        return jsonLoader.parse(data);
    }

    /**
     * Proxy method to see if the schemaUri is currently cached.
     */
    public boolean isCached(URI schemaUri) {
        return this.cache.containsKey(schemaUri);
    }

    /**
     * Proxy method to get a schema by schemaUri directly from the local cache.
     */
    public JsonNode cacheGet(URI schemaUri) {
        return this.cache.get(schemaUri);
    }

    /**
     * Proxy method to put a schema by schemaUri directly in the local cache.
     */
    public JsonNode cachePut(URI schemaUri, JsonNode schema) {
        return this.cache.put(schemaUri, schema);
    }

    /**
     * Return the underlying JsonLoader.
     */
    public JsonLoader getJsonLoader() {
        return jsonLoader;
    }

}
