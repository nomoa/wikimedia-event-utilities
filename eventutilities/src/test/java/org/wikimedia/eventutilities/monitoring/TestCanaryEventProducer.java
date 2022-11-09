package org.wikimedia.eventutilities.monitoring;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.notFound;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static java.net.HttpURLConnection.HTTP_CREATED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
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
import org.wikimedia.eventutilities.core.event.EventStreamFactory;
import org.wikimedia.eventutilities.core.http.BasicHttpClient;
import org.wikimedia.eventutilities.core.http.BasicHttpResult;
import org.wikimedia.eventutilities.core.json.JsonLoader;
import org.wikimedia.eventutilities.core.json.JsonSchemaLoader;
import org.wikimedia.eventutilities.core.util.ResourceLoader;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.Options;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.extension.responsetemplating.ResponseTemplateTransformer;
import com.google.common.collect.ImmutableMap;

class TestCanaryEventProducer {

    WireMockServer wireMockServer;

    private static final String testStreamConfigsFile =
        "file://" + new File("src/test/resources/event_stream_configs.json")
            .getAbsolutePath();

    private static final List<URL> schemaBaseUrls = ResourceLoader.asURLs(Arrays.asList(
        "file://" + new File("src/test/resources/event-schemas/repo3").getAbsolutePath()
    ));

    private static final Map<String, URI> eventServiceToUriMap = ImmutableMap.<String, URI>builder()
            .put("eventgate-main", URI.create("https://eventgate-main.discovery.test:4492/v1/events"))
            .put("eventgate-main-eqiad", URI.create("https://eventgate-main.svc.eqiad.test:4492/v1/events"))
            .put("eventgate-main-codfw", URI.create("https://eventgate-main.svc.codfw.test:4492/v1/events"))

            .put("eventgate-analytics", URI.create("https://eventgate-analytics.discovery.test:4592/v1/events"))
            .put("eventgate-analytics-eqiad", URI.create("https://eventgate-analytics.svc.eqiad.test:4592/v1/events"))
            .put("eventgate-analytics-codfw", URI.create("https://eventgate-analytics.svc.codfw.test:4592/v1/events"))

            .put("eventgate-analytics-external", URI.create("https://eventgate-analytics-external.discovery.test:4692/v1/events"))
            .put("eventgate-analytics-external-eqiad", URI.create("https://eventgate-analytics-external.svc.eqiad.test:4692/v1/events"))
            .put("eventgate-analytics-external-codfw", URI.create("https://eventgate-analytics-external.svc.codfw.test:4692/v1/events"))

            .put("eventgate-logging-external", URI.create("https://eventgate-logging-external.discovery.test:4392/v1/events"))
            .put("eventgate-logging-external-eqiad", URI.create("https://eventgate-logging-external.svc.eqiad.test:4392/v1/events"))
            .put("eventgate-logging-external-codfw", URI.create("https://eventgate-logging-external.svc.codfw.test:4392/v1/events"))
            .build();


    private static CanaryEventProducer canaryEventProducer;


    @BeforeAll
    public static void setUp() {
        BasicHttpClient client = BasicHttpClient.builder().build();
        ResourceLoader resLoader = ResourceLoader.builder()
                .setBaseUrls(schemaBaseUrls)
                .withHttpClient(client)
                .build();
        EventStreamConfig eventStreamConfig = EventStreamConfig.builder()
            .setEventStreamConfigLoader(testStreamConfigsFile)
            .setEventServiceToUriMap(eventServiceToUriMap)
            .setJsonLoader(new JsonLoader(resLoader))
            .build();

        EventSchemaLoader evtSchemaLoader = EventSchemaLoader.builder()
                .setJsonSchemaLoader(JsonSchemaLoader.build(resLoader))
                .build();

        canaryEventProducer = new CanaryEventProducer(
            EventStreamFactory.builder()
                .setEventSchemaLoader(evtSchemaLoader)
                .setEventStreamConfig(eventStreamConfig)
                .build(),
            client
        );
    }

    @BeforeEach
    public void startWireMock() {
        Options options = new WireMockConfiguration()
                .dynamicPort()
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
        wireMockServer.stubFor(post(urlPathMatching("/bad_url")).willReturn(notFound()));
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

        assertNull(
            canaryEvent.get("meta").get("dt"),
            "Should unset meta.dt in canary event"
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

        Map<URI, List<ObjectNode>> expected = ImmutableMap.of(
                URI.create("https://eventgate-main.svc.eqiad.test:4492/v1/events"),
                Arrays.asList(pageCreateCanaryEvent),

                URI.create("https://eventgate-main.svc.codfw.test:4492/v1/events"),
                Arrays.asList(pageCreateCanaryEvent),

                URI.create("https://eventgate-analytics-external.svc.eqiad.test:4692/v1/events"),
                Arrays.asList(searchSatisfactionCanaryEvent),

                URI.create("https://eventgate-analytics-external.svc.codfw.test:4692/v1/events"),
                Arrays.asList(searchSatisfactionCanaryEvent)
        );
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

        BasicHttpResult result = canaryEventProducer.postEvents(
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

        BasicHttpResult result = canaryEventProducer.postEvents(
            url,
            Collections.singletonList(canaryEvent)
        );
        assertFalse(result.getSuccess(), "Should fail post events");
    }

}
