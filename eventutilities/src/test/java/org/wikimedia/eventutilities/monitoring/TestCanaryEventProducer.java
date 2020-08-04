package org.wikimedia.eventutilities.monitoring;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.apache.commons.io.IOUtils;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import org.wikimedia.eventutilities.core.event.EventStreamConfigFactory;
import org.wikimedia.eventutilities.core.event.EventStreamFactory;
import org.wikimedia.eventutilities.core.event.EventStreamConfig;
import org.wikimedia.eventutilities.core.event.EventSchemaLoader;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.wikimedia.eventutilities.core.http.HttpResult;
import org.wikimedia.eventutilities.core.json.JsonLoader;
import org.wikimedia.eventutilities.core.json.JsonLoadingException;

import static org.junit.jupiter.api.Assertions.*;

public class TestCanaryEventProducer {

    private static String testStreamConfigsFile =
        "file://" + new File("src/test/resources/event_stream_configs.json")
            .getAbsolutePath();

    private static List<String> schemaBaseUris = new ArrayList<>(Arrays.asList(
        "file://" + new File("src/test/resources/event-schemas/repo3").getAbsolutePath()
    ));

    private static CanaryEventProducer canaryEventProducer;

    private static JsonNode pageCreateSchema;
    private static JsonNode searchSatisfactionSchema;

    private static HttpServer httpServer;
    private static InetSocketAddress httpServerAddress;

    private static HttpServer createTestHttpServer() throws IOException {
        HttpServer httpServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);

        httpServer.createContext("/v1/events", new HttpHandler() {
            public String readString(HttpExchange exchange) throws IOException {
                String body;
                try (InputStream in = exchange.getRequestBody()) {
                    body = IOUtils.toString(in, StandardCharsets.UTF_8);
                }
                return body;
            }

            public void handle(HttpExchange exchange) throws IOException {
                String requestBody;
                try (InputStream in = exchange.getRequestBody()) {
                    requestBody = IOUtils.toString(in, StandardCharsets.UTF_8);
                }

                byte[] response = ("{\"success\": true, \"body\": " + requestBody + "}").getBytes();
                // HTTP_CREATED is 201.
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_CREATED, response.length);
                exchange.getResponseBody().write(response);
                exchange.close();
            }
        });

        return httpServer;
    }

    @BeforeAll
    public static void setUp() throws RuntimeException, IOException {
        EventSchemaLoader eventSchemaLoader = new EventSchemaLoader(schemaBaseUris);
        EventStreamConfig eventStreamConfig = EventStreamConfigFactory.createStaticEventStreamConfig(testStreamConfigsFile);
        EventStreamFactory eventStreamFactory = new EventStreamFactory(eventSchemaLoader, eventStreamConfig);
        canaryEventProducer = new CanaryEventProducer(eventStreamFactory);

        // Read expected some data in for assertions
        try {
            pageCreateSchema = JsonLoader.getInstance().load(
                URI.create(schemaBaseUris.get(0) + "/mediawiki/revision/create/latest")
            );
            searchSatisfactionSchema = JsonLoader.getInstance().load(
                URI.create(schemaBaseUris.get(0) + "/analytics/legacy/searchsatisfaction/latest")
            );
        } catch (JsonLoadingException e) {
            throw new RuntimeException(e);
        }

        httpServer = createTestHttpServer();
        httpServerAddress = httpServer.getAddress();

        httpServer.start();
    }

    @AfterAll
    public static void tearDown() {
        httpServer.stop(0);
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
            "http://%s:%d/v1/events", httpServerAddress.getHostString(), httpServerAddress.getPort()
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
            "http://%s:%d/bad_url", httpServerAddress.getHostString(), httpServerAddress.getPort()
        ));

        HttpResult result = CanaryEventProducer.postEvents(
            url,
            Collections.singletonList(canaryEvent)
        );
        assertFalse(result.getSuccess(), "Should fail post events");
    }

}