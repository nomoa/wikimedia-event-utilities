package org.wikimedia.eventutilities.core.event;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import org.wikimedia.eventutilities.core.json.JsonLoadingException;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchema;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;

/**
 * Helper class suited to assist java clients producing json events directly to kafka.
 * This object must be constructed using {@link JsonEventGenerator#builder()}.
 *
 * It provides:
 * - initialization of default values
 *   - meta.dt as the kafka ingestion time with the provided clock
 *   - meta.stream
 *   - dt as a yet optional event time provided via a param
 *   - $schema as the schema that can validate the generated event
 * - verification of schema vs stream
 * - validation of the event against its schema
 */
public final class JsonEventGenerator {
    public static final String SCHEMA_FIELD = "$schema";
    public static final String EVENT_TIME_FIELD = "dt";
    public static final String META_FIELD = "meta";
    public static final String META_STREAM_FIELD = "stream";
    public static final String META_INGESTION_TIME_FIELD = "dt";
    public static final String META_ID_FIELD = "id";

    private final EventSchemaLoader schemaLoader;
    private final EventStreamConfig eventStreamConfig;
    private final Supplier<Instant> ingestionTimeClock;
    private final ObjectMapper jsonMapper;
    private final Supplier<UUID> uuidSupplier;
    /**
     * cache of the JsonSchema whose schema/stream pair was already verified against the stream configuration.
     */
    private final Cache<SchemaStreamNamesPair, JsonSchema> schemaCache = CacheBuilder.newBuilder().build();

    private JsonEventGenerator(
        EventSchemaLoader schemaLoader,
        EventStreamConfig eventStreamConfig,
        Supplier<Instant> ingestionTimeClock,
        ObjectMapper jsonMapper,
        Supplier<UUID> uuidSupplier
    ) {
        this.schemaLoader = Objects.requireNonNull(schemaLoader);
        this.eventStreamConfig = Objects.requireNonNull(eventStreamConfig);
        this.ingestionTimeClock = Objects.requireNonNull(ingestionTimeClock);
        this.jsonMapper = Objects.requireNonNull(jsonMapper);
        this.uuidSupplier = uuidSupplier;
    }

    /**
     * Generates an json event calling the supplier eventData.
     *
     * @param stream the stream this event will be pushed to
     * @param schema the schema this event is build against
     * @param eventData consumer receiving an empty ObjectNode to attach data to
     * @param eventTime the optional eventTime to be set to the dt field (might become mandatory)
     * @throws IllegalArgumentException if the schema does not match what is expected from the stream configuration
     * @throws IllegalArgumentException if the schema cannot be found/loaded
     * @throws IllegalArgumentException if the event is not valid against the provided schema
     */
    public ObjectNode generateEvent(
        String stream,
        String schema,
        Consumer<ObjectNode> eventData,
        @Nullable Instant eventTime
    ) {
        return generateEvent(stream, schema, eventData, eventTime, true);
    }

    /**
     * Generates a json event calling the supplier eventData.
     *
     * @param stream the stream this event will be pushed to
     * @param schema the schema this event is build against
     * @param eventData consumer receiving an empty ObjectNode to attach data to
     * @param eventTime the optional eventTime to be set to the dt field (might become mandatory)
     * @param validate true to validate the event against its schema
     * @throws IllegalArgumentException if the schema does not match what is expected from the stream configuration
     * @throws IllegalArgumentException if the schema cannot be found/loaded
     * @throws IllegalArgumentException if the event is not valid against the provided schema
     */
    public ObjectNode generateEvent(
        String stream,
        String schema,
        Consumer<ObjectNode> eventData,
        @Nullable Instant eventTime,
        boolean validate
    ) {
        Objects.requireNonNull(stream, "stream must not be null");
        Objects.requireNonNull(schema, "schema must not be null");
        ObjectNode root = jsonMapper.createObjectNode();

        eventData.accept(root);
        // attach dt as eventTime if provided and not already set in the json object
        if (eventTime != null && !root.has(EVENT_TIME_FIELD)) {
            // Should event time be mandatory?
            root.put(EVENT_TIME_FIELD, eventTime.toString());
        }
        // schema is always overridden with the one passed as param
        root.put(SCHEMA_FIELD, schema);

        JsonNode metaAsJsonNode = root.get(META_FIELD);
        ObjectNode meta;
        if (metaAsJsonNode == null) {
            meta = root.putObject(META_FIELD);
        } else if (!metaAsJsonNode.isObject()) {
            throw new IllegalArgumentException("The field [meta] must be an object. [" + metaAsJsonNode.getNodeType() + "] found.");
        } else {
            meta = (ObjectNode) metaAsJsonNode;
        }
        // set meta.dt only if not already provided, some users prefer to provide their own "processing-time" rather than
        // using this object clock.
        if (!meta.has(META_INGESTION_TIME_FIELD)) {
            // ingestionTimeClock cannot be null
            meta.put(META_INGESTION_TIME_FIELD, ingestionTimeClock.get().toString());
        }
        if (!meta.has(META_ID_FIELD)) {
            meta.put(META_ID_FIELD, uuidSupplier.get().toString());
        }
        // Like the schema field the stream field is always overridden
        meta.put(META_STREAM_FIELD, stream);

        if (validate) {
            JsonSchema eventSchema = loadAndVerifyJsonSchema(stream, schema, root);
            validateEvent(root, eventSchema);
        }
        return root;
    }

    private void validateEvent(ObjectNode root, JsonSchema eventSchema) {
        ProcessingReport report;
        try {
            report = eventSchema.validate(root);
        } catch (ProcessingException e) {
            throw new IllegalArgumentException("Cannot validate the generated event", e);
        }
        if (!report.isSuccess()) {
            throw new IllegalArgumentException("Cannot validate the generated event");
        }
    }

    private JsonSchema loadAndVerifyJsonSchema(String stream, String schema, ObjectNode root) {
        JsonSchema eventSchema;
        try {
            eventSchema = schemaCache.get(new SchemaStreamNamesPair(stream, schema), () -> {
                JsonNode eventSchemaAsJson;
                try {
                    eventSchemaAsJson = schemaLoader.getEventSchema(root);
                } catch (JsonLoadingException e) {
                    throw new IllegalArgumentException("Cannot load schema for event", e);
                }

                checkSchemaTitleAndStream(eventSchemaAsJson, stream, schema);
                try {
                    return schemaLoader.getJsonSchema(eventSchemaAsJson);
                } catch (ProcessingException e) {
                    throw new IllegalArgumentException("Cannot obtain schema [" + schema + "]", e);
                }
            });
        } catch (ExecutionException | UncheckedExecutionException e) {
            throw new IllegalArgumentException(e.getCause().getMessage(), e.getCause());
        }
        return eventSchema;
    }

    private static final class SchemaStreamNamesPair {
        private final String stream;
        private final String schema;

        private SchemaStreamNamesPair(String stream, String schema) {
            this.stream = stream;
            this.schema = schema;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SchemaStreamNamesPair that = (SchemaStreamNamesPair) o;
            return Objects.equals(stream, that.stream) && Objects.equals(schema, that.schema);
        }

        @Override
        public int hashCode() {
            return Objects.hash(stream, schema);
        }
    }

    private void checkSchemaTitleAndStream(
        JsonNode eventSchema,
        String stream,
        String schemaName
    ) {
        JsonNode schemaTitle = eventSchema.get("title");
        if (schemaTitle == null || schemaTitle.getNodeType() != JsonNodeType.STRING) {
            throw new IllegalArgumentException("Missing or invalid title for schema [" + schemaName + "]");
        }

        List<String> allowedTitles = eventStreamConfig.collectSettingAsString(
            stream,
            EventStreamConfig.SCHEMA_TITLE_SETTING
        );
        if (allowedTitles.isEmpty()) {
            throw new IllegalArgumentException("Cannot find any schema titles for stream [" + stream + "]");
        }
        if (!allowedTitles.contains(schemaTitle.textValue())) {
            throw new IllegalArgumentException("Schema [" + schemaName + "] with title " +
                    "[" + schemaTitle.asText() + "] does not match allowed titles for stream " +
                    "[" + stream + "], allowed titles are: " +
                    "[" + String.join(",", allowedTitles) + "]");
        }
    }

    /**
     * Helper method to serialize the event as bytes.
     */
    public byte[] serializeAsBytes(ObjectNode root) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        JsonGenerator generator = jsonMapper.getFactory().createGenerator(
            byteArrayOutputStream,
            JsonEncoding.UTF8
        );
        root.serialize(generator, jsonMapper.getSerializerProviderInstance());
        generator.flush();
        generator.close();
        return byteArrayOutputStream.toByteArray();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private EventSchemaLoader schemaLoader;
        private EventStreamConfig eventStreamConfig;
        private Supplier<Instant> ingestionTimeClock;
        private ObjectMapper jsonMapper;
        private Supplier<UUID> uuidSupplier;

        private Builder() {}

        public Builder ingestionTimeClock(Supplier<Instant> ingestionTimeClock) {
            this.ingestionTimeClock = Objects.requireNonNull(ingestionTimeClock);
            return this;
        }

        public Builder jsonMapper(ObjectMapper jsonMapper) {
            this.jsonMapper = Objects.requireNonNull(jsonMapper);
            return this;
        }

        public Builder schemaLoader(EventSchemaLoader schemaLoader) {
            this.schemaLoader = Objects.requireNonNull(schemaLoader);
            return this;
        }

        public Builder eventStreamConfig(EventStreamConfig eventStreamConfig) {
            this.eventStreamConfig = Objects.requireNonNull(eventStreamConfig);
            return this;
        }

        public Builder withUuidSupplier(Supplier<UUID> uuidSupplier) {
            this.uuidSupplier = uuidSupplier;
            return this;
        }

        public JsonEventGenerator build() {
            if (schemaLoader == null) {
                throw new IllegalArgumentException(
                    "Must call schemaLoader() before calling build()."
                );
            }

            if (eventStreamConfig == null) {
                throw new IllegalArgumentException(
                    "Must call eventStreamConfig() before calling build()."
                );
            }

            if (ingestionTimeClock == null) {
                Clock clock = Clock.systemUTC();
                ingestionTimeClock = clock::instant;
            }

            return new JsonEventGenerator(
                schemaLoader,
                eventStreamConfig,
                ingestionTimeClock,
                jsonMapper != null ? jsonMapper : new ObjectMapper(),
                this.uuidSupplier != null ? this.uuidSupplier : UUID::randomUUID
            );
        }
    }
}
