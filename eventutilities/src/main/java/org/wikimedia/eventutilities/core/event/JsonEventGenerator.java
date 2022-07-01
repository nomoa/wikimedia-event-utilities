package org.wikimedia.eventutilities.core.event;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import org.wikimedia.eventutilities.core.SerializableClock;
import org.wikimedia.eventutilities.core.json.JsonLoadingException;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.UncheckedExecutionException;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

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
@ParametersAreNonnullByDefault
public final class JsonEventGenerator {
    public static final String SCHEMA_FIELD = "$schema";
    public static final String EVENT_TIME_FIELD = "dt";
    public static final String META_FIELD = "meta";
    public static final String META_STREAM_FIELD = "stream";
    public static final String META_INGESTION_TIME_FIELD = "dt";
    public static final String META_ID_FIELD = "id";

    private final EventSchemaLoader schemaLoader;
    private final EventStreamConfig eventStreamConfig;
    private final SerializableClock ingestionTimeClock;
    private final ObjectMapper jsonMapper;
    private final Supplier<UUID> uuidSupplier;
    /**
     * cache of the JsonSchema whose schema/stream pair was already verified against the stream configuration.
     */
    private final Cache<SchemaStreamNamesPair, EventNormalizer> validatingGeneratorCache = CacheBuilder.newBuilder().build();
    private final Cache<SchemaStreamNamesPair, EventNormalizer> nonValidatingGeneratorCache = CacheBuilder.newBuilder().build();

    private JsonEventGenerator(
        EventSchemaLoader schemaLoader,
        EventStreamConfig eventStreamConfig,
        SerializableClock ingestionTimeClock,
        ObjectMapper jsonMapper,
        Supplier<UUID> uuidSupplier
    ) {
        this.schemaLoader = schemaLoader;
        this.eventStreamConfig = eventStreamConfig;
        this.ingestionTimeClock = ingestionTimeClock;
        this.jsonMapper = jsonMapper;
        this.uuidSupplier = uuidSupplier;
    }

    /**
     * Generates an json event calling the supplier eventData.
     *
     * @param streamName the stream this event will be pushed to
     * @param schemaUri the schema this event is build against
     * @param eventData consumer receiving an empty ObjectNode to attach data to
     * @param eventTime the optional eventTime to be set to the dt field (might become mandatory)
     * @throws IllegalArgumentException if the schema does not match what is expected from the stream configuration
     * @throws IllegalArgumentException if the schema cannot be found/loaded
     * @throws IllegalArgumentException if the event is not valid against the provided schema
     */
    public ObjectNode generateEvent(
        String streamName,
        String schemaUri,
        Consumer<ObjectNode> eventData,
        @Nullable Instant eventTime
    ) {
        return createEventStreamEventGenerator(streamName, schemaUri).generateEvent(eventData, eventTime);

    }

    /**
     * Generates a json event calling the supplier eventData.
     *
     * Using this method with validate to false is highly discouraged and should only be used when shipping events
     * to event-gate that will take care of the schema validation.
     *
     * @param streamName the stream this event will be pushed to
     * @param schemaUri the schema this event is build against
     * @param eventData consumer receiving an empty ObjectNode to attach data to
     * @param eventTime the optional eventTime to be set to the dt field (might become mandatory)
     * @param validate true to validate the event against its schema
     *
     * @throws IllegalArgumentException if the schema does not match what is expected from the stream configuration
     * @throws IllegalArgumentException if the schema cannot be found/loaded
     * @throws IllegalArgumentException if the event is not valid against the provided schema
     */
    public ObjectNode generateEvent(
        String streamName,
        String schemaUri,
        Consumer<ObjectNode> eventData,
        @Nullable Instant eventTime,
        boolean validate
    ) {
        Objects.requireNonNull(streamName, "stream must not be null");
        Objects.requireNonNull(schemaUri, "schema must not be null");
        return createEventStreamEventGenerator(streamName, schemaUri, validate).generateEvent(eventData, eventTime);
    }

    /**
     * Create a validating event generator.
     * Suited to ship events directly to kafka
     */
    public EventNormalizer createEventStreamEventGenerator(String streamName, String schemaUri) {
        return createEventStreamEventGenerator(streamName, schemaUri, true);
    }

    /**
     * Create a non-validating event generator.
     * Use with care, this should ONLY be used to send event via event-gate which will perform schema validation.
     * Using this to ship events directly to kafka must never be used.
     */
    public EventNormalizer createNonValidatingEventStreamEventGenerator(String streamName, String schemaUri) {
        return createEventStreamEventGenerator(streamName, schemaUri, false);
    }

    private EventNormalizer createEventStreamEventGenerator(String streamName, String schemaUri, boolean validating) {
        Cache<SchemaStreamNamesPair, EventNormalizer> generatorCache = validating ? validatingGeneratorCache : nonValidatingGeneratorCache;
        try {
            return generatorCache.get(new SchemaStreamNamesPair(streamName, schemaUri), () -> {
                List<EventNormalizationStep> steps = validating ?
                        normalizeAndValidationSteps(streamName, schemaUri) : normalizationStep(streamName, schemaUri);
                return new StepBasedEventNormalizer(jsonMapper, steps);
            });
        } catch (ExecutionException | UncheckedExecutionException e) {
            throw new IllegalArgumentException(e.getCause().getMessage(), e.getCause());
        }
    }

    /**
     * Non-validating normalization steps.
     * - meta.dt
     * - meta.id
     * - $schema and meta.stream
     */
    private List<EventNormalizationStep> normalizationStep(String streamName, String schemaUri) {
        return ImmutableList.of(
                EventNormalizationStep.streamAndSchema(streamName, schemaUri),
                EventNormalizationStep.ingestionTime(ingestionTimeClock),
                EventNormalizationStep.eventId(uuidSupplier)
        );
    }

    /**
     * Validating and normalization steps.
     * - meta.dt
     * - meta.id
     * - $schema and meta.stream
     * - schema validation
     */
    private List<EventNormalizationStep> normalizeAndValidationSteps(String streamName, String schemaUri) {
        ObjectNode schemaAsJson = loadAndVerifyJsonSchema(streamName, schemaUri);
        return ImmutableList.<EventNormalizationStep>builder()
                .addAll(normalizationStep(streamName, schemaUri))
                .add(EventNormalizationStep.schemaValidation(schemaAsJson, streamName))
                .build();
    }

    private ObjectNode loadAndVerifyJsonSchema(String streamName, String schemaUri) {
        JsonNode eventSchemaAsJson;
        try {
            eventSchemaAsJson = schemaLoader.getSchema(URI.create(schemaUri));
        } catch (JsonLoadingException e) {
            throw new IllegalArgumentException("Cannot load schema for event", e);
        }

        checkSchemaTitleAndStream(eventSchemaAsJson, streamName, schemaUri);
        try {
            // check that the schema is valid
            schemaLoader.getJsonSchema(eventSchemaAsJson);
        } catch (ProcessingException e) {
            throw new IllegalArgumentException("Cannot obtain schema [" + schemaUri + "]", e);
        }
        if (!(eventSchemaAsJson instanceof ObjectNode)) {
            throw new IllegalArgumentException("schemaLoader must return an ObjectNode");
        }

        return (ObjectNode) eventSchemaAsJson;
    }

    private static final class SchemaStreamNamesPair {
        private final String streamName;
        private final String schemaUri;

        private SchemaStreamNamesPair(String streamName, String schemaUri) {
            this.streamName = streamName;
            this.schemaUri = schemaUri;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SchemaStreamNamesPair that = (SchemaStreamNamesPair) o;
            return Objects.equals(streamName, that.streamName) && Objects.equals(schemaUri, that.schemaUri);
        }

        @Override
        public int hashCode() {
            return Objects.hash(streamName, schemaUri);
        }
    }

    private void checkSchemaTitleAndStream(
        JsonNode eventSchema,
        String streamName,
        String schemaUri
    ) {
        JsonNode schemaTitle = eventSchema.get("title");
        if (schemaTitle == null || schemaTitle.getNodeType() != JsonNodeType.STRING) {
            throw new IllegalArgumentException("Missing or invalid title for schema [" + schemaUri + "]");
        }

        List<String> allowedTitles = eventStreamConfig.collectSettingAsString(
            streamName,
            EventStreamConfig.SCHEMA_TITLE_SETTING
        );
        if (allowedTitles.isEmpty()) {
            throw new IllegalArgumentException("Cannot find any schema titles for stream [" + streamName + "]");
        }
        if (!allowedTitles.contains(schemaTitle.textValue())) {
            throw new IllegalArgumentException("Schema [" + schemaUri + "] with title " +
                    "[" + schemaTitle.asText() + "] does not match allowed titles for stream " +
                    "[" + streamName + "], allowed titles are: " +
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
        private SerializableClock ingestionTimeClock;
        private ObjectMapper jsonMapper;
        private Supplier<UUID> uuidSupplier;

        private Builder() {}

        public Builder ingestionTimeClock(SerializableClock ingestionTimeClock) {
            this.ingestionTimeClock = Objects.requireNonNull(ingestionTimeClock);
            return this;
        }

        public Builder jsonMapper(ObjectMapper mapper) {
            this.jsonMapper = mapper;
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
                ingestionTimeClock = Instant::now;
            }

            return new JsonEventGenerator(
                schemaLoader,
                eventStreamConfig,
                ingestionTimeClock,
                jsonMapper != null ? jsonMapper : new ObjectMapper(),
                this.uuidSupplier != null ? this.uuidSupplier : (Supplier<UUID> & Serializable) UUID::randomUUID
            );
        }
    }


    public ObjectMapper getJsonMapper() {
        return jsonMapper;
    }

    /**
     * A component that applies various normalization and verification steps to the events it receives.
     * This object is serializable and is suited for use with frameworks like spark or flink.
     */
    public interface EventNormalizer extends Serializable, Function<Consumer<ObjectNode>, ObjectNode> {
        ObjectNode generateEvent(
                Consumer<ObjectNode> eventData,
                @Nullable Instant eventTime
        );

        default ObjectNode apply(
                Consumer<ObjectNode> eventData
        ) {
            return generateEvent(eventData, null);
        }

        /**
         * The object mapper this normalizer will use to generate json nodes.
         */
        ObjectMapper getObjectMapper();
    }

    @Getter
    @AllArgsConstructor
    @ToString(exclude = {"objectMapper"})
    @SuppressFBWarnings({"SE_NO_SERIALVERSIONID"})
    private static class StepBasedEventNormalizer implements Serializable, EventNormalizer {
        private final ObjectMapper objectMapper;
        private final List<EventNormalizationStep> steps;

        @Override
        public ObjectNode generateEvent(Consumer<ObjectNode> eventData, @Nullable Instant eventTime) {
            ObjectNode root = objectMapper.createObjectNode();

            eventData.accept(root);
            if (eventTime != null) {
                EventNormalizationStep.eventTime(eventTime).accept(root);
            }
            steps.forEach(e -> e.accept(root));
            return root;
        }
    }
}
