package org.wikimedia.eventutilities.core.event;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.wikimedia.eventutilities.core.json.JsonLoader;
import org.wikimedia.eventutilities.core.json.JsonLoadingException;
import org.wikimedia.eventutilities.core.json.JsonSchemaLoader;
import org.wikimedia.eventutilities.core.util.ResourceLoader;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;

public class TestEventStream {

    private static final String testStreamConfigsFile =
        "file://" + new File("src/test/resources/event_stream_configs.json").getAbsolutePath();

    private static final List<URL> schemaBaseUrls = ResourceLoader.asURLs(Collections.singletonList(
        "file://" + new File("src/test/resources/event-schemas/repo3").getAbsolutePath()
    ));

    private static final Map<String, URI> eventServiceToUriMap = ImmutableMap.<String, URI>builder()
            .put("eventgate-main", URI.create("https://eventgate-main.discovery.wmnet:4492/v1/events"))
            .put("eventgate-main-eqiad", URI.create("https://eventgate-main.svc.eqiad.wmnet:4492/v1/events"))
            .put("eventgate-main-codfw", URI.create("https://eventgate-main.svc.codfw.wmnet:4492/v1/events"))

            .put("eventgate-analytics", URI.create("https://eventgate-analytics.discovery.wmnet:4592/v1/events"))
            .put("eventgate-analytics-eqiad", URI.create("https://eventgate-analytics.svc.eqiad.wmnet:4592/v1/events"))
            .put("eventgate-analytics-codfw", URI.create("https://eventgate-analytics.svc.codfw.wmnet:4592/v1/events"))

            .put("eventgate-analytics-external", URI.create("https://eventgate-analytics-external.discovery.wmnet:4692/v1/events"))
            .put("eventgate-analytics-external-eqiad", URI.create("https://eventgate-analytics-external.svc.eqiad.wmnet:4692/v1/events"))
            .put("eventgate-analytics-external-codfw", URI.create("https://eventgate-analytics-external.svc.codfw.wmnet:4692/v1/events"))

            .put("eventgate-logging-external", URI.create("https://eventgate-logging-external.discovery.wmnet:4392/v1/events"))
            .put("eventgate-logging-external-eqiad", URI.create("https://eventgate-logging-external.svc.eqiad.wmnet:4392/v1/events"))
            .put("eventgate-logging-external-codfw", URI.create("https://eventgate-logging-external.svc.codfw.wmnet:4392/v1/events"))
            .build();

    private static final String eventServiceToUriMapFile =
        "file://" + new File("src/test/resources/event_service_to_uri.yaml").getAbsolutePath();




    private static final JsonLoader jsonLoader = new JsonLoader(
        ResourceLoader.builder()
        .setBaseUrls(schemaBaseUrls)
        .build()
    );

    private static final EventStreamFactory eventStreamFactory = EventStreamFactory.builder()
        .setEventSchemaLoader(EventSchemaLoader.builder().setJsonSchemaLoader(new JsonSchemaLoader(jsonLoader)).build())
        .setEventStreamConfig(
            EventStreamConfig.builder()
                .setEventStreamConfigLoader(testStreamConfigsFile)
                .setEventServiceToUriMap(eventServiceToUriMap)
                .build()
        )
        .build();

    private static JsonNode searchSatisfactionSchema;

    @BeforeAll
    public static void setUp() throws JsonLoadingException {
        // Read expected some data in for assertions
        searchSatisfactionSchema = jsonLoader.load(
            URI.create(schemaBaseUrls.get(0) + "/analytics/legacy/searchsatisfaction/latest")
        );
    }

    @Test
    void eventStreamFactoryFrom() {
        // Simple test of the from factory method.
        EventStreamFactory eventStreamFactoryForTest = EventStreamFactory.from(
            schemaBaseUrls.stream().map(URL::toString).collect(Collectors.toList()),
            testStreamConfigsFile,
            null,
            eventServiceToUriMapFile
        );

        assertEquals(
            eventStreamFactory.createEventStream("mediawiki.page-create").toString(),
            eventStreamFactoryForTest.createEventStream("mediawiki.page-create").toString(),
            "EventStream created by EventStreamFactory via of method should be the " +
            "same as the one created by EventStreamFactory builder"
        );
    }

    @Test
    void createEventStream() {
        String streamName = "mediawiki.page-create";
        EventStream es = eventStreamFactory.createEventStream(streamName);
        assertEquals(
            streamName,
            es.streamName(),
            "Should create an EventStream"
        );
    }

    @Test
    void createEventStreams() {
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
    void createEventStreamWithRegexName() {
        String streamName = "/mediawiki\\.job\\..+/";
        assertThrows(
            RuntimeException.class, () -> eventStreamFactory.createEventStream(streamName),
            "Should throw RuntimeException if attempt to create EventStream with regex stream name"
        );
    }

    @Test
    void createAllCachedEventStreams() {
        // /^mediawiki\\.job\\..+/ should not be included.
        List<String> expectedStreamNames = Arrays.asList(
            "mediawiki.page-create", "eventlogging_SearchSatisfaction", "no_settings"
        );

        List<EventStream> eventStreams = eventStreamFactory.createAllCachedEventStreams();

        Assertions.assertThat(eventStreams)
            .withFailMessage("Should create " + expectedStreamNames.size() + "streams")
            .hasSameSizeAs(expectedStreamNames);


        for (String streamName : expectedStreamNames) {
            EventStream eventStream = eventStreams.stream()
                .filter((es) -> es.streamName() == streamName)
                .findAny()
                .orElse(null);

            assertNotNull(eventStream, "Should create event stream " + streamName);
        }
    }

    @Test
    void createEventStreamsMatchingSettings() {
        // /^mediawiki\\.job\\..+/ should not be included.
        List<String> expectedStreamNames = Arrays.asList(
            "eventlogging_SearchSatisfaction"
        );

        List<EventStream> eventStreams = eventStreamFactory.createEventStreamsMatchingSettings(
            null,
            Collections.singletonMap("canary_events_enabled", "true")
        );

        Assertions.assertThat(eventStreams)
            .withFailMessage("Should create " + expectedStreamNames.size() + "streams")
            .hasSameSizeAs(expectedStreamNames);


        for (String streamName : expectedStreamNames) {
            EventStream eventStream = eventStreams.stream()
                .filter((es) -> es.streamName() == streamName)
                .findAny()
                .orElse(null);

            assertNotNull(eventStream, "Should match settings and create event " + streamName);
        }
    }

    @Test
    void createEventStreamsMatchingSettingsJsonPointer() {
        List<String> expectedStreamNames = Arrays.asList(
                "eventlogging_SearchSatisfaction", "mediawiki.page-create"
        );

        List<EventStream> eventStreams = eventStreamFactory.createEventStreamsMatchingSettings(
                null,
                Collections.singletonMap("/consumers/client_name/job_name", "general")
        );

        Assertions.assertThat(eventStreams)
            .withFailMessage("Should create " + expectedStreamNames.size() + "streams")
            .hasSameSizeAs(expectedStreamNames);



        for (String streamName : expectedStreamNames) {
            EventStream eventStream = eventStreams.stream()
                .filter((es) -> es.streamName() == streamName)
                .findAny()
                .orElse(null);

            assertNotNull(eventStream, "Should match settings and create event " + streamName);
        }
    }

    @Test
    void streamName() {
        EventStream es = eventStreamFactory.createEventStream("mediawiki.page-create");
        String expected = "mediawiki.page-create";
        assertEquals(expected, es.streamName(), "Should get stream name");
    }

    @Test
    void topics() {
        EventStream es = eventStreamFactory.createEventStream("mediawiki.page-create");
        List<String> topics = es.topics();
        List<String> expected = new ArrayList<>(Arrays.asList(
            "eqiad.mediawiki.page-create", "codfw.mediawiki.page-create"
        ));
        assertEquals(expected, topics, "Should get topics for stream");
    }

    @Test
    void eventServiceName() {
        EventStream es = eventStreamFactory.createEventStream("mediawiki.page-create");
        String eventServiceName = es.eventServiceName();
        String expected = "eventgate-main";
        assertEquals(expected, eventServiceName, "Should get event service name for stream");
    }

    @Test
    void eventServiceUri() {
        EventStream es = eventStreamFactory.createEventStream("mediawiki.page-create");
        URI eventServiceUri = es.eventServiceUri();
        URI expected = URI.create("https://eventgate-main.discovery.wmnet:4492/v1/events");
        assertEquals(expected, eventServiceUri, "Should get event service URI for stream out of eventServiceUriMap");
    }

    @Test
    void eventServiceDatacenterSpecificUri() {
        EventStream es = eventStreamFactory.createEventStream("mediawiki.page-create");
        URI eventServiceUrl = es.eventServiceUri("eqiad");
        URI expected = URI.create("https://eventgate-main.svc.eqiad.wmnet:4492/v1/events");
        assertEquals(expected, eventServiceUrl, "Should get event service datacenter URI for stream");
    }

    @Test
    void schemaTitle() {
        EventStream es = eventStreamFactory.createEventStream("mediawiki.page-create");
        String schemaTitle = es.schemaTitle();
        String expected = "mediawiki/revision/create";
        assertEquals(
            expected, schemaTitle, "Should get schema title"
        );
    }

    @Test
    void schemaUri() {
        EventStream es = eventStreamFactory.createEventStream("mediawiki.page-create");
        URI schemaUri = es.schemaUri();
        URI expected = URI.create("/mediawiki/revision/create/latest");
        assertEquals(
            expected, schemaUri, "Should build latest schema URI for stream"
        );
    }

    @Test
    void exampleEvent() {
        EventStream es = eventStreamFactory.createEventStream("eventlogging_SearchSatisfaction");
        JsonNode example = es.exampleEvent();
        JsonNode expected = searchSatisfactionSchema.get("examples").get(0);
        assertEquals(
            expected, example, "Should read example event from schema for stream"
        );
    }

}
