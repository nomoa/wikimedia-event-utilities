package org.wikimedia.eventutilities.core.event;

import static java.util.Collections.singletonList;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.wikimedia.eventutilities.core.json.JsonLoadingException;
import org.wikimedia.eventutilities.core.json.JsonSchemaLoader;
import org.wikimedia.eventutilities.core.util.ResourceLoader;

import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.ProcessingReport;


public class TestEventSchemaValidator {
    private final ResourceLoader resLoader = ResourceLoader.builder()
            .setBaseUrls(singletonList(this.getClass().getResource("/event-schemas/repo4")))
            .build();

    private final EventSchemaLoader loader = EventSchemaLoader.builder()
        .setJsonSchemaLoader(JsonSchemaLoader.build(resLoader)).build();

    private final EventSchemaValidator validator = new EventSchemaValidator(loader);

    @Test
    public void testValidEventMustSucceedValidation() throws ProcessingException, JsonLoadingException {
        String simpleEvent = "{\"$schema\": \"/test/event/1.0.0\", \"meta\": { \"stream\": " +
            "\"test.event.example\", \"dt\": \"2019-01-01T00:00:00Z\" }, \"dt\": " +
            "\"2019-01-01T00:00:00Z\", \"test\": \"specific test value\", \"test_map\": " +
            "{ \"key1\": \"val1\", \"key2\": \"val2\" }}";
        Assertions.assertTrue(validator.validate(simpleEvent).isSuccess());
    }

    @Test
    public void testInvalidEventMustFailsValidation() throws ProcessingException, JsonLoadingException {
        String invalidEvent = "{\"$schema\": \"/test/event/1.0.0\", \"dt\": \"2019-01-01T00:00:00Z\"}";
        ProcessingReport report = validator.validate(invalidEvent);
        Assertions.assertTrue(report.toString().contains("missing: [\"meta\"]"));
    }
}
