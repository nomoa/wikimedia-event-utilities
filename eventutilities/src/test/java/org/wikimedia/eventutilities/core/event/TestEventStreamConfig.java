package org.wikimedia.eventutilities.core.event;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.wikimedia.eventutilities.core.json.JsonLoader;
import org.wikimedia.eventutilities.core.json.JsonLoadingException;

import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.node.ObjectNode;


public class TestEventStreamConfig {

    private static String testStreamConfigsFile =
        "file://" + new File("src/test/resources/event_stream_configs.json")
            .getAbsolutePath();

    private EventStreamConfig streamConfigs;
    private ObjectNode testStreamConfigsContent;

    @BeforeEach
    public void setUp() throws RuntimeException {
        streamConfigs = EventStreamConfigFactory.createStaticEventStreamConfig(testStreamConfigsFile);

        try {
            // Read this in for test assertions
            testStreamConfigsContent = (ObjectNode)JsonLoader.getInstance().load(
                URI.create(testStreamConfigsFile)
            );
        } catch (JsonLoadingException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void cachedStreamConfigs() {
        JsonNode configs = streamConfigs.cachedStreamConfigs();
        assertEquals(
            testStreamConfigsContent, configs, "Should read and return all stream configs"
                );
    }

    @Test
    public void getStreamConfig() {
        JsonNode config = streamConfigs.getStreamConfig("mediawiki.page-create");
        JsonNode expected = testStreamConfigsContent.retain("mediawiki.page-create");
        assertEquals(expected, config, "Should read and return a single stream config");
    }

    @Test
    public void getStreamConfigs() {
        JsonNode configs = streamConfigs.getStreamConfigs(
            Arrays.asList("mediawiki.page-create", "eventlogging_SearchSatisfaction")
        );
        JsonNode expected = testStreamConfigsContent.retain(
            "mediawiki.page-create", "eventlogging_SearchSatisfaction"
        );
        assertEquals(expected, configs, "Should read and return a multiple stream configs");
    }

    @Test
    public void cachedStreamNames() {
        List<String> streams = streamConfigs.cachedStreamNames();
        Collections.sort(streams);
        List<String> expected = Arrays.asList(
            "mediawiki.page-create", "eventlogging_SearchSatisfaction", "/^mediawiki\\.job\\..+/", "no_settings"
        );
        Collections.sort(expected);
        assertEquals(expected, streams, "Should return all known stream names");
    }

    @Test
    public void getSetting() {
        String settingValue = streamConfigs.getSetting(
            "mediawiki.page-create", "destination_event_service"
        ).asText();
        String expected = "eventgate-main";
        assertEquals(expected, settingValue, "Should get a single stream config setting");
    }

    @Test
    public void getSettingAsString() {
        String settingValue = streamConfigs.getSettingAsString(
            "mediawiki.page-create", "destination_event_service"
        );
        String expected = "eventgate-main";
        assertEquals(expected, settingValue, "Should get a single stream config setting as a string");
    }

    @Test
    public void getSettingForNonExistentStream() {
        JsonNode settingValue = streamConfigs.getSetting(
            "nonexistent-stream", "destination_event_service"
        );
        assertNull(settingValue, "Should return null for a non existent stream.");
    }

    @Test
    public void getSettingForNonExistentSetting() {
        JsonNode settingValue = streamConfigs.getSetting(
            "mediawiki.page-create", "non-existent-setting"
        );
        assertNull(settingValue, "Should return null for a non existent setting.");
    }

    @Test
    public void collectSetting() {
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
    public void collectSettingAsString() {
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
    public void collectSettings() {
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
    public void collectSettingsAsString() {
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
    public void collectAllCachedSettings() {
        List<JsonNode> settingValues = streamConfigs.collectAllCachedSettings( "topics");

        List<JsonNode> expected = Arrays.asList(
            JsonNodeFactory.instance.textNode("/^(eqiad\\.|codfw\\.)mediawiki\\.job\\..+/"),
            JsonNodeFactory.instance.textNode("eqiad.mediawiki.page-create"),
            JsonNodeFactory.instance.textNode("codfw.mediawiki.page-create"),
            JsonNodeFactory.instance.textNode("eventlogging_SearchSatisfaction")
        );
        assertEquals(expected, settingValues, "Should collect all cached settings for all streams as a List of JsonNodes");
    }

    @Test
    public void collectAllCachedSettingsAsString() {
        List<String> settingValues = streamConfigs.collectAllCachedSettingsAsString("topics");

        List<String> expected = Arrays.asList(
            "/^(eqiad\\.|codfw\\.)mediawiki\\.job\\..+/",
            "eqiad.mediawiki.page-create",
            "codfw.mediawiki.page-create",
            "eventlogging_SearchSatisfaction"
        );
        assertEquals(expected, settingValues, "Should collect all cached settings for all streams as a List of Strings");
    }
}
