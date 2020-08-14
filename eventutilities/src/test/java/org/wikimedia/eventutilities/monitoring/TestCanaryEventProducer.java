package org.wikimedia.eventutilities.monitoring;


import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static java.net.HttpURLConnection.HTTP_CREATED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.wikimedia.eventutilities.core.event.EventStreamConfigFactory.EVENT_SERVICE_TO_URI_MAP_DEFAULT;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.wikimedia.eventutilities.core.event.EventSchemaLoader;
import org.wikimedia.eventutilities.core.event.EventStreamConfig;
import org.wikimedia.eventutilities.core.event.EventStreamConfigFactory;
import org.wikimedia.eventutilities.core.event.EventStreamFactory;
import org.wikimedia.eventutilities.core.http.HttpResult;
import org.wikimedia.eventutilities.core.json.JsonLoader;
import org.wikimedia.eventutilities.core.json.JsonLoadingException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.Options;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.extension.responsetemplating.ResponseTemplateTransformer;

public class TestCanaryEventProducer {

    WireMockServer wireMockServer;

    private static String testStreamConfigsFile =
        "file://" + new File("src/test/resources/event_stream_configs.json")
            .getAbsolutePath();

    private static List<String> schemaBaseUris = new ArrayList<>(Arrays.asList(
        "file://" + new File("src/test/resources/event-schemas/repo3").getAbsolutePath()
    ));

    private static CanaryEventProducer canaryEventProducer;

    private static JsonNode pageCreateSchema;
    private static JsonNode searchSatisfactionSchema;

    @BeforeAll
    public static void setUp() throws JsonLoadingException {
        EventSchemaLoader eventSchemaLoader = new EventSchemaLoader(schemaBaseUris);
        EventStreamConfig eventStreamConfig = EventStreamConfigFactory.createStaticEventStreamConfig(testStreamConfigsFile, EVENT_SERVICE_TO_URI_MAP_DEFAULT);
        EventStreamFactory eventStreamFactory = new EventStreamFactory(eventSchemaLoader, eventStreamConfig);
        canaryEventProducer = new CanaryEventProducer(eventStreamFactory);

        // Read expected some data in for assertions
        pageCreateSchema = JsonLoader.getInstance().load(
            URI.create(schemaBaseUris.get(0) + "/mediawiki/revision/create/latest")
        );
        searchSatisfactionSchema = JsonLoader.getInstance().load(
            URI.create(schemaBaseUris.get(0) + "/analytics/legacy/searchsatisfaction/latest")
        );
    }

    @BeforeEach
    public void startWireMock() {
        Options options = new WireMockConfiguration()
                .dynamicHttpsPort()
                .extensions(new ResponseTemplateTransformer(false));
        wireMockServer = new WireMockServer(options);
        wireMockServer.start();

        wireMockServer.stubFor(post(urlPathMatching("/v1/events.*"))
                .willReturn(aResponse()
                        // FIXME: it looks like we don't care about the body of the response for these tests
                        //        we might want to remove this complexity
                        .withBody("\"{\"success\": true, \"body\": \"{{request.body}}\"}\"")
                        .withTransformers("response-template")
                        .withStatus(HTTP_CREATED)
                ));
    }

    @AfterEach
    public void stopWireMock() {
        wireMockServer.stop();
    }

    @Test
    public void testCanaryEvent() {
        ObjectNode canaryEvent = canaryEventProducer.canaryEvent(
            "eventlogging_SearchSatisfaction"
        );

        assertEquals(
            "canary",
            canaryEvent.get("meta").get("domain").asText(),
            "Should set meta.domain in canary event"
        );

        assertEquals(
            "eventlogging_SearchSatisfaction",
            canaryEvent.get("meta").get("stream").asText(),
            "Should set meta.stream in canary event"
        );
    }

    @Test
    public void testGetCanaryEventsToPost() {
        Map<URI, List<ObjectNode>> canaryEventsToPost = canaryEventProducer.getCanaryEventsToPost(
            Arrays.asList("mediawiki.page-create", "eventlogging_SearchSatisfaction")
        );

        ObjectNode pageCreateCanaryEvent = canaryEventProducer.canaryEvent(
            "mediawiki.page-create"
        );
        ObjectNode searchSatisfactionCanaryEvent = canaryEventProducer.canaryEvent(
            "eventlogging_SearchSatisfaction"
        );

        Map<URI, List<ObjectNode>> expected = new HashMap<URI, List<ObjectNode>>() {{
            put(
                URI.create("https://eventgate-main.svc.eqiad.wmnet:4492/v1/events"),
                Arrays.asList(pageCreateCanaryEvent)
            );
            put(
                URI.create("https://eventgate-main.svc.codfw.wmnet:4492/v1/events"),
                Arrays.asList(pageCreateCanaryEvent)
            );
            put(
                URI.create("https://eventgate-analytics-external.svc.eqiad.wmnet:4692/v1/events"),
                Arrays.asList(searchSatisfactionCanaryEvent)
            );
            put(
                URI.create("https://eventgate-analytics-external.svc.codfw.wmnet:4692/v1/events"),
                Arrays.asList(searchSatisfactionCanaryEvent)
            );
        }};

        List<URI> expectedKeys = expected.keySet().stream().sorted().collect(Collectors.toList());
        List<URI> actualKeys = canaryEventsToPost.keySet().stream().sorted().collect(Collectors.toList());
        assertEquals(
            expectedKeys,
            actualKeys,
            "Should get canary events to POST grouped by event service datacenter url"
        );

        for (URI key : expectedKeys) {
            List<ObjectNode> expectedEvents = expected.get(key);
            List<ObjectNode> actualEvents = canaryEventsToPost.get(key);

            for (ObjectNode expectedEvent : expectedEvents) {
                ObjectNode matchedActualEvent = null;

                for (ObjectNode actualEvent : actualEvents) {
                    if (actualEvent.equals(expectedEvent)) {
                        matchedActualEvent = actualEvent;
                        break;
                    }
                }

                assertEquals(
                    expectedEvent,
                    matchedActualEvent,
                    "Did not find expected canary event for event service url " + key
                );
            }
        }

        assertEquals(expected, canaryEventsToPost, "Should get canary events to POST");
    }

    @Test
    public void testPostEvents() {
        ObjectNode canaryEvent = canaryEventProducer.canaryEvent(
            "mediawiki.page-create"
        );

        URI url = URI.create(String.format(
                Locale.ROOT,
                "http://localhost:%d/v1/events", wireMockServer.port()
        ));

        HttpResult result = CanaryEventProducer.postEvents(
            url,
            Collections.singletonList(canaryEvent)
        );
        assertTrue(result.getSuccess(), "Should post events");
    }

    @Test
    public void testPostEventsFailure() {
        ObjectNode canaryEvent = canaryEventProducer.canaryEvent(
            "mediawiki.page-create"
        );

        URI url = URI.create(String.format(
                Locale.ROOT,
                "http://localhost:%d/bad_url", wireMockServer.port()
        ));

        HttpResult result = CanaryEventProducer.postEvents(
            url,
            Collections.singletonList(canaryEvent)
        );
        assertFalse(result.getSuccess(), "Should fail post events");
    }

}
