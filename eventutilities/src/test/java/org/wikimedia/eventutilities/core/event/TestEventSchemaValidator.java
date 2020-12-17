package org.wikimedia.eventutilities.core.event;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.wikimedia.eventutilities.core.json.JsonLoadingException;

import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.ProcessingReport;

public class TestEventSchemaValidator {
    private final EventSchemaLoader loader = new EventSchemaLoader(WikimediaDefaults.SCHEMA_BASE_URIS);
    private final EventSchemaValidator validator = new EventSchemaValidator(loader);

    @Test
    public void testValidEventMustSucceedValidation() throws ProcessingException, JsonLoadingException {
        String simpleEvent = "{\"$schema\": \"/fragment/common/1.0.0\", \"meta\": {\"dt\": \"2020-12-20T20:00:00Z\", \"stream\": \"test.stream\"}}";
        Assertions.assertTrue(validator.validate(simpleEvent).isSuccess());
    }

    @Test
    public void testInvalidEventMustFailsValidation() throws ProcessingException, JsonLoadingException {
        String brokenEvent = "{\"$schema\": \"/fragment/common/1.0.0\"}";
        ProcessingReport report = validator.validate(brokenEvent);
        Assertions.assertTrue(report.toString().contains("missing: [\"meta\"]"));
    }
}
