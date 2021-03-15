package org.wikimedia.eventutilities.core.json;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;

import org.wikimedia.eventutilities.core.util.ResourceLoader;
import org.wikimedia.eventutilities.core.util.ResourceLoadingException;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 * Uses a {@link ResourceLoader} to load content at URIs and parse it into YAML or JSON.
 *
 * If the data fetched fromo a URI starts with a { or [
 * character, it will be assumed to be JSON and JsonParser will be used.
 * Otherwise YAMLParser will be used. JSON data can contain certain unicode
 * characters that YAML cannot, so it is best to use JsonParser when we can.
 */
public class JsonLoader {

    final YAMLFactory  yamlFactory  = new YAMLFactory();
    final JsonFactory  jsonFactory  = new JsonFactory();

    // Make sure to reuse, expensive to create.
    // This should be thread safe.
    final ObjectMapper objectMapper = new ObjectMapper();

    final ResourceLoader resourceLoader;

    public JsonLoader(ResourceLoader resourceLoader) {
        this.resourceLoader = resourceLoader;
    }

    /**
     * Given a schemaURI, this will request the JSON or YAML content at that URI and
     * parse it into a JsonNode.  $refs will be resolved.
     * The compiled schema will be cached by schemaURI, and only looked up once per schemaURI.
     *
     * @return the jsonschema at schemaURI.
     */
    public JsonNode load(URI uri) throws JsonLoadingException {

        JsonParser parser;
        try {
            parser = this.getParser(uri);
        } catch (IOException | IllegalArgumentException | UncheckedIOException | ResourceLoadingException e) {
            throw new JsonLoadingException("Failed reading JSON/YAML data from " + uri, e);
        }

        try {
            return this.parse(parser);
        } catch (IOException | IllegalArgumentException e) {
            throw new JsonLoadingException("Failed loading JSON/YAML data from " + uri, e);
        }
    }

    /**
     * Parses the JSON or YAML string into a JsonNode.
     *
     * @param data JSON or YAML string to parse into a JsonNode.
     */
    public JsonNode parse(String data) throws JsonLoadingException {
        try {
            return this.parse(this.getParser(data));
        } catch (IOException | IllegalArgumentException e) {
            throw new JsonLoadingException(
                "Failed parsing JSON/YAML data from string '" + data + '"', e
            );
        }
    }

    /**
     * Convenience method to reuse our ObjectMapper to serialize a JsonNode
     * to a JSON String.
     */
    public String asString(JsonNode jsonNode) throws JsonProcessingException {
        return objectMapper.writeValueAsString(jsonNode);
    }

    /**
     * Convenience method to reuse our ObjectMapper to convert a JsonNode to a Java Class.
     * See:
     * https://fasterxml.github.io/jackson-databind/javadoc/2.10/com/fasterxml/jackson/databind/ObjectMapper.html#convertValue-java.lang.Object-java.lang.Class
     */
    public <T> T convertValue(JsonNode jsonNode, Class<T> t) {
        return objectMapper.convertValue(jsonNode, t);
    }

    /**
     * Returns the underlying {@link ResourceLoader}.
     */
    public ResourceLoader getResourceLoader() {
        return resourceLoader;
    }

    /**
     * Given a {@link JsonParser}, reads it in as a JsonNode.
     */
    private JsonNode parse(JsonParser parser) throws IOException {
        return objectMapper.readTree(parser);
    }

    /**
     * Gets a {@link JsonParser} for String data.
     * If the data starts with a { or [, this will be parsed as JSON,
     * else it will be parsed as YAML.
     */
    private JsonParser getParser(String data) throws IOException {
        // If the first character is { or [, assume this is
        // JSON data and use a JsonParser.  Otherwise assume
        // YAML and use a YAMLParser.
        char firstChar = data.charAt(0);
        if (firstChar == '{' || firstChar == '[') {
            return jsonFactory.createParser(data);
        } else {
            return yamlFactory.createParser(data);
        }
    }

    /**
     * Gets either a YAMLParser or a JsonParser for the data at uri.
     * If the data starts with a { or [, this will be parsed as JSON,
     * else it will be parsed as YAML.
     */
    private JsonParser getParser(URI uri) throws IOException, ResourceLoadingException {
        return this.getParser(new String(resourceLoader.load(uri), UTF_8));
    }

    public String toString() {
        return "JsonLoader(" + getResourceLoader() + ")";
    }

}
