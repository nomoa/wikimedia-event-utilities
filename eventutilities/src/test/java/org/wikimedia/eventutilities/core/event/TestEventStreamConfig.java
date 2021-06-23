package org.wikimedia.eventutilities.core.event;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.wikimedia.eventutilities.core.json.JsonLoader;
import org.wikimedia.eventutilities.core.json.JsonLoadingException;
import org.wikimedia.eventutilities.core.util.ResourceLoader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

public class TestEventStreamConfig {

    private static final String testStreamConfigsFile =
        "file://" + new File("src/test/resources/event_stream_configs.json")
            .getAbsolutePath();
    private static final String testEventServiceConfigFile =
        "file://" + new File("src/test/resources/event_service_to_uri.yaml")
            .getAbsolutePath();

    private final JsonLoader jsonLoader = new JsonLoader(ResourceLoader.builder().build());
    private EventStreamConfig streamConfigs;
    private ObjectNode testStreamConfigsContent;

    @BeforeEach
    public void setUp() throws JsonLoadingException {
        streamConfigs = EventStreamConfig.builder()
            .setEventStreamConfigLoader(testStreamConfigsFile)
            .setEventServiceToUriMap(testEventServiceConfigFile)
            .build();

        // Read this in for test assertions
        testStreamConfigsContent = (ObjectNode)jsonLoader.load(
            URI.create(testStreamConfigsFile)
        );
    }

    @Test
    void cachedStreamConfigs() {
        JsonNode configs = streamConfigs.cachedStreamConfigs();
        assertEquals(
                testStreamConfigsContent, configs, "Should read and return all stream configs"
        );
    }

    @Test
    void getStreamConfig() {
        JsonNode config = streamConfigs.getStreamConfig("mediawiki.page-create");
        JsonNode expected = testStreamConfigsContent.retain("mediawiki.page-create");
        assertEquals(expected, config, "Should read and return a single stream config");
    }

    @Test
    void getStreamConfigs() {
        JsonNode configs = streamConfigs.getStreamConfigs(
            Arrays.asList("mediawiki.page-create", "eventlogging_SearchSatisfaction")
        );
        JsonNode expected = testStreamConfigsContent.retain(
            "mediawiki.page-create", "eventlogging_SearchSatisfaction"
        );
        assertEquals(expected, configs, "Should read and return a multiple stream configs");
    }

    @Test
    void cachedStreamNames() {
        List<String> streams = streamConfigs.cachedStreamNames();
        Assertions.assertThat(streams)
                .withFailMessage("Should return all known stream names")
                .containsExactlyInAnyOrder(
                        "mediawiki.page-create",
                        "eventlogging_SearchSatisfaction",
                        "/^mediawiki\\.job\\..+/",
                        "no_settings"
                );

    }

    @Test
    void getSetting() {
        String settingValue = streamConfigs.getSetting(
            "mediawiki.page-create", "destination_event_service"
        ).asText();
        String expected = "eventgate-main";
        assertEquals(expected, settingValue, "Should get a single stream config setting");
    }

    @Test
    void getSettingJsonPointer() {
        String settingValue = streamConfigs.getSetting(
                "mediawiki.page-create", "/consumers/client_name/job_name"
        ).asText();
        String expected = "general";
        assertEquals(expected, settingValue, "Should get a single stream config setting using JsonPointer");
    }

    @Test
    void getSettingAsString() {
        String settingValue = streamConfigs.getSettingAsString(
            "mediawiki.page-create", "destination_event_service"
        );
        String expected = "eventgate-main";
        assertEquals(expected, settingValue, "Should get a single stream config setting as a string");
    }

    @Test
    void getSettingForNonExistentStream() {
        JsonNode settingValue = streamConfigs.getSetting(
            "nonexistent-stream", "destination_event_service"
        );
        assertNull(settingValue, "Should return null for a non existent stream.");
    }

    @Test
    void getSettingForNonExistentSetting() {
        JsonNode settingValue = streamConfigs.getSetting(
            "mediawiki.page-create", "non-existent-setting"
        );
        assertNull(settingValue, "Should return null for a non existent setting.");
    }

    @Test
    void filterStreamConfigs() {
        List<String> expectedStreams = Arrays.asList(
            "eventlogging_SearchSatisfaction"
        );
        HashMap<String, String> filters = new HashMap<>();
        filters.put("destination_event_service", "eventgate-analytics-external");
        ObjectNode result = streamConfigs.filterStreamConfigs(null, filters);
        List<String> resultStreamNames = ImmutableList.copyOf(result.fieldNames());
        assertEquals(expectedStreams, resultStreamNames, "Should filter stream configs on setting name and values");
    }

    @Test
    void filterStreamConfigsJsonPointer() {
        List<String> expectedStreams = Arrays.asList(
                "mediawiki.page-create", "eventlogging_SearchSatisfaction"
        );
        HashMap<String, String> filters = new HashMap<>();
        filters.put("/consumers/client_name/job_name", "general");
        ObjectNode result = streamConfigs.filterStreamConfigs(null, filters);
        List<String> resultStreamNames = ImmutableList.copyOf(result.fieldNames());
        assertEquals(expectedStreams, resultStreamNames, "Should filter stream configs on setting name as JsonPointer and values");
    }

    @Test
    void collectSetting() {
        List<JsonNode> settingValues = streamConfigs.collectSetting(
            "mediawiki.page-create", "topics"
        );
        List<JsonNode> expected = Arrays.asList(
            JsonNodeFactory.instance.textNode("eqiad.mediawiki.page-create"),
            JsonNodeFactory.instance.textNode("codfw.mediawiki.page-create")
        );
        assertEquals(expected, settingValues, "Should collect a stream setting as a List of JsonNodes");
    }

    @Test
    void collectSettingAsString() {
        List<String> settingValues = streamConfigs.collectSettingAsString(
                "mediawiki.page-create", "topics"
        );
        List<String> expected = Arrays.asList(
            "eqiad.mediawiki.page-create",
            "codfw.mediawiki.page-create"
        );
        assertEquals(expected, settingValues, "Should collect a stream setting as a List of Strings");
    }


    @Test
    void collectSettings() {
        List<JsonNode> settingValues = streamConfigs.collectSettings(
            Arrays.asList("mediawiki.page-create", "eventlogging_SearchSatisfaction"), "topics"
        );
        List<JsonNode> expected = Arrays.asList(
            JsonNodeFactory.instance.textNode("eqiad.mediawiki.page-create"),
            JsonNodeFactory.instance.textNode("codfw.mediawiki.page-create"),
            JsonNodeFactory.instance.textNode("eventlogging_SearchSatisfaction")
        );
        assertEquals(expected, settingValues, "Should collect all settings for target streams as a List of JsonNodes");
    }

    @Test
    void collectSettingsAsString() {
        List<String> settingValues = streamConfigs.collectSettingsAsString(
            Arrays.asList("mediawiki.page-create", "eventlogging_SearchSatisfaction"), "topics"
        );
        List<String> expected = Arrays.asList(
            "eqiad.mediawiki.page-create",
            "codfw.mediawiki.page-create",
            "eventlogging_SearchSatisfaction"
        );
        assertEquals(expected, settingValues, "Should collect all settings for target streams as a List of Strings");
    }

    @Test
    void collectAllCachedSettings() {
        List<JsonNode> settingValues = streamConfigs.collectAllCachedSettings("topics");

        List<JsonNode> expected = Arrays.asList(
            JsonNodeFactory.instance.textNode("/^(eqiad\\.|codfw\\.)mediawiki\\.job\\..+/"),
            JsonNodeFactory.instance.textNode("eqiad.mediawiki.page-create"),
            JsonNodeFactory.instance.textNode("codfw.mediawiki.page-create"),
            JsonNodeFactory.instance.textNode("eventlogging_SearchSatisfaction")
        );
        assertEquals(expected, settingValues, "Should collect all cached settings for all streams as a List of JsonNodes");
    }

    @Test
    void collectAllCachedSettingsAsString() {
        List<String> settingValues = streamConfigs.collectAllCachedSettingsAsString("topics");

        List<String> expected = Arrays.asList(
            "/^(eqiad\\.|codfw\\.)mediawiki\\.job\\..+/",
            "eqiad.mediawiki.page-create",
            "codfw.mediawiki.page-create",
            "eventlogging_SearchSatisfaction"
        );
        assertEquals(expected, settingValues, "Should collect all cached settings for all streams as a List of Strings");
    }

    @Test
    void collectTopicMatchingSettings() {
        List<String> topics = streamConfigs.collectTopicsMatchingSettings(
            Arrays.asList("mediawiki.page-create", "eventlogging_SearchSatisfaction"),
            Collections.singletonMap("destination_event_service", "eventgate-main")
        );

        // /^mediawiki\\.job\\..+/ should not be included.
        List<String> expectedTopics = Arrays.asList(
            "eqiad.mediawiki.page-create",
            "codfw.mediawiki.page-create"
        );

        assertEquals(
            expectedTopics.size(),
            topics.size(),
            "Should collect " + expectedTopics.size() + " topics matching settings"
        );

        for (String expectedTopic : expectedTopics) {
            assertTrue(
                topics.contains(expectedTopic),
                "Should collect topics matching settings and get topic " + expectedTopic
            );
        }
    }

    @Test
    void eventServiceToUriMap() {
        assertEquals(
            URI.create("https://eventgate-analytics-external.example.org:4692/v1/events"),
            streamConfigs.getEventServiceUri("eventlogging_SearchSatisfaction"),
            "Should read main event service to URI map from config file"
        );

        assertEquals(
            URI.create("https://eventgate-analytics-external.svc.eqiad.example.org:4692/v1/events"),
            streamConfigs.getEventServiceUri("eventlogging_SearchSatisfaction", "eqiad"),
            "Should read datacenter specific service to URI map from config file"
        );
    }

    @Test
    void toRegex() {
        List<String> strings = Arrays.asList("a", "/^b.+/", "c");
        String expected = "(a|^b.+|c)";

        String result = EventStreamConfig.toRegex(strings);
        assertEquals(expected, result, "Should convert a list of strings to a single or-ed regex.");
    }
}
