package org.wikimedia.eventutilities.core.event;

import static org.wikimedia.eventutilities.core.event.JsonEventGenerator.EVENT_TIME_FIELD;
import static org.wikimedia.eventutilities.core.event.JsonEventGenerator.META_ID_FIELD;
import static org.wikimedia.eventutilities.core.event.JsonEventGenerator.META_STREAM_FIELD;
import static org.wikimedia.eventutilities.core.event.JsonEventGenerator.SCHEMA_FIELD;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.wikimedia.eventutilities.core.SerializableClock;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;


/**
 * Define the steps to "normalize" an event before sending it to kafka.
 * Normalize here stands for "standardization" to make outgoing events compliant to the WMF event platform recommendations.
 */
@FunctionalInterface
public interface EventNormalizationStep extends Serializable, Consumer<ObjectNode> {

    /**
     * Set the ingestionTime (meta.dt) if not already set.
     */
    static EventNormalizationStep ingestionTime(SerializableClock clock) {
        return node -> {
            ObjectNode meta = getOrSetMeta(node);
            // set meta.dt only if not already provided, some users prefer to provide their own "processing-time" rather than
            // using this object clock.
            if (!meta.has(JsonEventGenerator.META_INGESTION_TIME_FIELD)) {
                // ingestionTimeClock cannot be null
                meta.put(JsonEventGenerator.META_INGESTION_TIME_FIELD, clock.get().toString());
            }
        };
    }

    /**
     * Set the event time (dt) if not already set.
     */
    static EventNormalizationStep eventTime(Instant eventTime) {
        return node -> {
            if (!node.has(EVENT_TIME_FIELD)) {
                // Should event time be mandatory?
                node.put(EVENT_TIME_FIELD, eventTime.toString());
            }
        };
    }

    /**
     * Set the event if (meta.id) if not already set.
     */
    static EventNormalizationStep eventId(Supplier<UUID> uuidSupplier) {
        return node -> {
            ObjectNode meta = getOrSetMeta(node);
            if (!meta.has(META_ID_FIELD)) {
                meta.put(META_ID_FIELD, uuidSupplier.get().toString());
            }
        };
    }

    /**
     * Forcibly set the stream (meta.stream) and the schema URI.
     */
    static EventNormalizationStep streamAndSchema(String streamName, String schemaUri) {
        return node -> {
            node.put(SCHEMA_FIELD, schemaUri);
            getOrSetMeta(node).put(META_STREAM_FIELD, streamName);
        };
    }

    /**
     * Validates the event against the provided schema (must be the last step to apply).
     */
    static EventNormalizationStep schemaValidation(ObjectNode jsonSchema, String schemaUri) {
        return new Validation(jsonSchema, schemaUri);
    }

    static ObjectNode getOrSetMeta(ObjectNode node) {
        JsonNode metaAsJsonNode = node.get(JsonEventGenerator.META_FIELD);
        ObjectNode meta;
        if (metaAsJsonNode == null) {
            meta = node.putObject(JsonEventGenerator.META_FIELD);
        } else if (!metaAsJsonNode.isObject()) {
            throw new IllegalArgumentException("The field [meta] must be an object. [" + metaAsJsonNode.getNodeType() + "] found.");
        } else {
            meta = (ObjectNode) metaAsJsonNode;
        }
        return meta;
    }

    @SuppressFBWarnings("SE_NO_SERIALVERSIONID")
    class Validation implements EventNormalizationStep {
        private final ObjectNode jsonSchema;
        private final String schemaUri;

        /**
         * This object is not serializable and will be managed manually in constructor and readObject.
         */
        private transient JsonSchema schemaValidator;

        private Validation(ObjectNode jsonSchema, String schemaUri) {
            this.jsonSchema = jsonSchema;
            this.schemaUri = schemaUri;
            this.schemaValidator = parseSchema(jsonSchema, schemaUri);
        }

        /**
         * Parse the schema when deserializing to fail early in case this stream
         * is rarely pushing events.
         */
        private void readObject(ObjectInputStream in) throws ClassNotFoundException, IOException {
            // Default serialization mechanism do not call the constructor, so we have to re-initialize
            // this transient field explicitly when it happens
            // see https://docs.oracle.com/javase/7/docs/api/java/io/Serializable.html
            in.defaultReadObject();
            schemaValidator = parseSchema(jsonSchema, schemaUri);
        }

        private static JsonSchema parseSchema(JsonNode jsonSchema, String schemaUri) {
            JsonSchemaFactory schemaFactory = JsonSchemaFactory.byDefault();
            try {
                return schemaFactory.getJsonSchema(jsonSchema);
            } catch (ProcessingException e) {
                throw new IllegalArgumentException("The schema [" + schemaUri + "] is invalid", e);
            }
        }
        @Override
        public void accept(ObjectNode root) {
            validateEvent(root, schemaValidator);
        }

        private void validateEvent(ObjectNode root, JsonSchema eventSchema) {
            ProcessingReport report;
            try {
                report = eventSchema.validate(root);
            } catch (ProcessingException e) {
                throw new IllegalArgumentException("Cannot validate the generated event", e);
            }
            if (!report.isSuccess()) {
                throw new IllegalArgumentException("Cannot validate the generated event: " + report);
            }
        }
    }

}

