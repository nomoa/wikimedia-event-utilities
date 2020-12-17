package org.wikimedia.eventutilities.core.event;

import org.wikimedia.eventutilities.core.json.JsonLoader;
import org.wikimedia.eventutilities.core.json.JsonLoadingException;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchema;

/**
 * Utility class to validate events against their schema.
 */
public class EventSchemaValidator {
    private final EventSchemaLoader schemaLoader;

    public EventSchemaValidator(EventSchemaLoader schemaLoader) {
        this.schemaLoader = schemaLoader;
    }

    public ProcessingReport validate(String eventData) throws JsonLoadingException, ProcessingException {
        return validate(JsonLoader.getInstance().parse(eventData));
    }

    public ProcessingReport validate(JsonNode eventData) throws JsonLoadingException, ProcessingException {
        JsonSchema schema = schemaLoader.getEventJsonSchema(eventData);
        return schema.validate(eventData);
    }
}
