package org.wikimedia.eventutilities.core.event;


import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.BeforeAll;
import org.wikimedia.eventutilities.core.json.JsonLoader;
import org.wikimedia.eventutilities.core.json.JsonLoadingException;

import java.io.File;
import java.net.URI;
import java.util.*;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;


public class TestEventStream {

    private static final String testStreamConfigsFile =
        "file://" + new File("src/test/resources/event_stream_configs.json")
                .getAbsolutePath();

    private static final List<String> schemaBaseUris = Collections.singletonList(
        "file://" + new File("src/test/resources/event-schemas/repo3").getAbsolutePath()
    );

    private static final HashMap<String, URI> eventServiceToUriMap =
        new HashMap<String, URI>() {{
            put("eventgate-main", URI.create("https://eventgate-main.discovery.wmnet:4492/v1/events"));
            put("eventgate-main-eqiad", URI.create("https://eventgate-main.svc.eqiad.wmnet:4492/v1/events"));
            put("eventgate-main-codfw", URI.create("https://eventgate-main.svc.codfw.wmnet:4492/v1/events"));

            put("eventgate-analytics", URI.create("https://eventgate-analytics.discovery.wmnet:4592/v1/events"));
            put("eventgate-analytics-eqiad", URI.create("https://eventgate-analytics.svc.eqiad.wmnet:4592/v1/events"));
            put("eventgate-analytics-codfw", URI.create("https://eventgate-analytics.svc.codfw.wmnet:4592/v1/events"));

            put("eventgate-analytics-external", URI.create("https://eventgate-analytics-external.discovery.wmnet:4692/v1/events"));
            put("eventgate-analytics-external-eqiad", URI.create("https://eventgate-analytics-external.svc.eqiad.wmnet:4692/v1/events"));
            put("eventgate-analytics-external-codfw", URI.create("https://eventgate-analytics-external.svc.codfw.wmnet:4692/v1/events"));

            put("eventgate-logging-external", URI.create("https://eventgate-logging-external.discovery.wmnet:4392/v1/events"));
            put("eventgate-logging-external-eqiad", URI.create("https://eventgate-logging-external.svc.eqiad.wmnet:4392/v1/events"));
            put("eventgate-logging-external-codfw", URI.create("https://eventgate-logging-external.svc.codfw.wmnet:4392/v1/events"));
        }};


    private static final EventStreamFactory eventStreamFactory = EventStreamFactory.createStaticConfigEventStreamFactory(
        schemaBaseUris,
        testStreamConfigsFile,
        eventServiceToUriMap
    );

    private static JsonNode pageCreateSchema;
    private static JsonNode searchSatisfactionSchema;

    @BeforeAll
    public static void setUp() throws RuntimeException {
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
    }

    @Test
    public void createEventStream() {
        String streamName = "mediawiki.page-create";
        EventStream es = eventStreamFactory.createEventStream(streamName);
        assertEquals(
            streamName,
            es.streamName(),
            "Should create an EventStream"
        );
    }

    @Test
    public void createEventStreams() {
        List<String> streamNames = Arrays.asList(
            "mediawiki.page-create", "eventlogging_SearchSatisfaction"
        );

        List<EventStream> eventStreams = eventStreamFactory.createEventStreams(streamNames);
        assertEquals(
            streamNames.get(0),
            eventStreams.get(0).streamName(),
            "Should create multiple EventStreams (1)"
        );
        assertEquals(
            streamNames.get(1),
            eventStreams.get(1).streamName(),
            "Should create multiple EventStreams (2)"
        );
    }

    @Test
    public void streamName() {
        EventStream es = eventStreamFactory.createEventStream("mediawiki.page-create");
        String expected = "mediawiki.page-create";
        assertEquals(expected, es.streamName(), "Should get stream name");
    }

    @Test
    public void topics() {
        EventStream es = eventStreamFactory.createEventStream("mediawiki.page-create");
        List<String> topics = es.topics();
        List<String> expected = new ArrayList<>(Arrays.asList(
            "eqiad.mediawiki.page-create", "codfw.mediawiki.page-create"
        ));
        assertEquals(expected, topics, "Should get topics for stream");
    }

    @Test
    public void eventServiceName() {
        EventStream es = eventStreamFactory.createEventStream("mediawiki.page-create");
        String eventServiceName = es.eventServiceName();
        String expected = "eventgate-main";
        assertEquals(expected, eventServiceName, "Should get event service name for stream");
    }

    @Test
    public void eventServiceUri() {
        EventStream es = eventStreamFactory.createEventStream("mediawiki.page-create");
        URI eventServiceUri = es.eventServiceUri();
        URI expected = URI.create("https://eventgate-main.discovery.wmnet:4492/v1/events");
        assertEquals(expected, eventServiceUri, "Should get event service URI for stream out of eventServiceUriMap");
    }

    @Test
    public void eventServiceDatacenterSpecificUri() {
        EventStream es = eventStreamFactory.createEventStream("mediawiki.page-create");
        URI eventServiceUrl = es.eventServiceUri("eqiad");
        URI expected = URI.create("https://eventgate-main.svc.eqiad.wmnet:4492/v1/events");
        assertEquals(expected, eventServiceUrl, "Should get event service datacenter URI for stream");
    }

    @Test
    public void schemaUri() {
        EventStream es = eventStreamFactory.createEventStream("mediawiki.page-create");
        URI schemaUri = es.schemaUri();
        URI expected = URI.create("/mediawiki/revision/create/latest");
        assertEquals(
            expected, schemaUri, "Should build latest schema URI for stream"
        );
    }

    @Test
    public void exampleEvent() {
        EventStream es = eventStreamFactory.createEventStream("eventlogging_SearchSatisfaction");
        JsonNode example = es.exampleEvent();
        JsonNode expected = searchSatisfactionSchema.get("examples").get(0);
        assertEquals(
            expected, example, "Should read example event from schema for stream"
        );
    }

}