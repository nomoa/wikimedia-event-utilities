package org.wikimedia.eventutilities.core.event;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.net.URI;

import org.wikimedia.eventutilities.core.json.JsonLoader;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Class to fetch and work with stream configuration from the a URI.
 * Upon instantiation, this will attempt to pre fetch and cache all stream config from
 * streamConfigsUri.  Further accesses of uncached stream names will cause
 * a fetch from the result of makeStreamConfigsUriForStreams for the uncached stream names only.
 */
public class EventStreamConfig {

    /**
     * Stream config topics setting.  Used to get a full list of topics for streams.
     */
    public static final String TOPICS_SETTING = "topics";

    /**
     * Stream Config setting name for schema title.
     */
    public static final String SCHEMA_TITLE_SETTING = "schema_title";

    /**
     * Stream Config setting name for destination event service name.
     */
    public static final String EVENT_SERVICE_SETTING = "destination_event_service";

    /**
     * Maps event service name to a service URL.
     */
    protected Map<String, URI> eventServiceToUriMap;

    /**
     * Used to load stream config at instantiation and on demand.
     */
    protected EventStreamConfigLoader eventStreamConfigLoader;

    /**
     * Cached stream configurations. This maps stream name (or stream pattern regex)
     * to stream config settings.
     */
    protected ObjectNode streamConfigsCache;

    /**
     * EventStreamConfig constructor.
     * This is protected; use EventStreamConfig.builder()
     * to create your EventStreamConfig instance.
     */
    protected EventStreamConfig(
        EventStreamConfigLoader eventStreamConfigLoader,
        Map<String, URI> eventServiceToUriMap
    ) {
        if (eventStreamConfigLoader == null) {
            throw new RuntimeException(
                "Cannot instantiate EventStreamConfig; eventStreamConfigLoader must not be null."
            );
        }
        if (eventServiceToUriMap == null) {
            throw new RuntimeException(
                "Cannot instantiate EventStreamConfig; eventServiceToUriMap must not be null."
            );
        }

        this.eventStreamConfigLoader = eventStreamConfigLoader;
        this.eventServiceToUriMap = eventServiceToUriMap;

        // Get and store all known stream configs.
        // The stream configs endpoint should return an object mapping
        // stream names (or regex patterns) to stream config entries.
        this.reset();
    }

    /**
     * EventStreamConfigBuilder, builder pattern to construct
     * EventStreamConfig instances.  Usage:
     *
     *  EventStreamConfig c = EventStreamConfig.builder()
     *      .setEventStreamConfigLoader("https://meta.wikimedia.org/api.php")
     *      .setEventServiceToUriMap("file:///path/to/event_service_map.yaml")
     *      .build()
     */
    public static class EventStreamConfigBuilder {
        private EventStreamConfigLoader eventStreamConfigLoader;
        private Map<String, URI> eventServiceToUriMap;

        public EventStreamConfigBuilder setEventStreamConfigLoader(
            EventStreamConfigLoader loader
        ) {
            this.eventStreamConfigLoader = loader;
            return this;
        }

        public EventStreamConfigBuilder setEventStreamConfigLoader(String streamConfigUri) {
            if (streamConfigUri.contains("/api.php")) {
                return setEventStreamConfigLoader(
                    new MediawikiEventStreamConfigLoader(streamConfigUri)
                );
            } else {
                return setEventStreamConfigLoader(
                    new StaticEventStreamConfigLoader(streamConfigUri)
                );
            }
        }

        public EventStreamConfigBuilder setEventServiceToUriMap(
            Map<String, URI> eventServiceToUriMap
        ) {
            this.eventServiceToUriMap = eventServiceToUriMap;
            return this;
        }

        public EventStreamConfigBuilder setEventServiceToUriMap(String eventServiceConfigUri) {
            return setEventServiceToUriMap(loadEventServiceConfig(eventServiceConfigUri));
        }

        /**
         * Returns a new EventStreamConfig.  If eventStreamConfigLoader or
         * eventServiceToUriMap are not yet set, defaults suitable for use in
         * WMF production will be used, as specified in WikimediaDefaults.
         */
        public EventStreamConfig build() {
            if (this.eventStreamConfigLoader == null) {
                setEventStreamConfigLoader(WikimediaDefaults.EVENT_STREAM_CONFIG_URI);
            }
            if (this.eventServiceToUriMap == null) {
                setEventServiceToUriMap(WikimediaDefaults.EVENT_SERVICE_TO_URI_MAP);
            }

            return new EventStreamConfig(
                this.eventStreamConfigLoader,
                this.eventServiceToUriMap
            );
        }

        /**
         * Loads the YAML or JSON at eventServiceConfigUri into a Map.
         * This expects that the JSON object simply maps an event intake service name to
         * an event intake service URI.
         * @param eventServiceConfigUri
         *  http://, file:// or other loadable URI.
         */
        protected Map<String, URI> loadEventServiceConfig(
            String eventServiceConfigUri
        ) {
            Map<String, String> loadedConfig;
            try {
                // Load eventServiceConfigUri and convert it to a HashMap.
                loadedConfig = JsonLoader.getInstance().convertValue(
                    JsonLoader.get(URI.create(eventServiceConfigUri)), HashMap.class
                );
            } catch (JsonProcessingException e) {
                throw new RuntimeException(
                    "Failed loading event service config from " + eventServiceConfigUri,
                    e
                );
            }

            // Convert our String -> String HashMap into String -> URI.
            return new HashMap<>(loadedConfig.entrySet().stream()
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> URI.create(e.getValue()))
                )
            );
        }
    }

    /**
     * Returns an EventStreamConfigBuilder instance.
     */
    public static EventStreamConfigBuilder builder() {
        return new EventStreamConfigBuilder();
    }

    /**
     * Re-fetches the content for all stream configs and saves it in the local
     * stream configs cache.  This should fetch and cache all stream configs.
     */
    public final void reset() {
        streamConfigsCache = eventStreamConfigLoader.load();
    }

    /**
     * Returns a Java Stream iterator over the stream config entries.
     * Useful for e.g. map, filter, reduce etc.
     */
    public Stream<JsonNode> elementsStream() {
        return StreamSupport.stream(streamConfigsCache.spliterator(), false);
    }

    /**
     * Returns a Java Stream iterator over the Map.Entry of stream name to stream config entries.
     * Useful for e.g. map, filter, reduce etc.
     */
    public Stream<Map.Entry<String, JsonNode>> fieldsStream() {
        return StreamSupport.stream(
            Spliterators.spliterator(
                streamConfigsCache.fields(),
                streamConfigsCache.size(),
                Spliterator.SIZED | Spliterator.IMMUTABLE
            ),
            false
        );
    }

    /**
     * Filter stream configs for streamNames that match the settingsFilters.
     * If streamNames is null, it is assumed you don't want to match on stream names,
     * and only setttingsFilters will be considered.
     *
     * Since settingsFilters must all be strings, this only allows filtering
     * on string stream config settings, or at least ones for which JsonNode.asText()
     * returns something sane (which is true for most primitive types).
     */
    public ObjectNode filterStreamConfigs(
        List<String> streamNames,
        Map<String, String> settingsFilters
    ) {
        List<Map.Entry<String, JsonNode>> filteredEntries = fieldsStream()
            .filter(entry -> {
                String streamName = entry.getKey();
                JsonNode settings = entry.getValue();

                // If streamNames were given but this isn't one of our target streamNames,
                // filter it out by returning false.
                if (streamNames != null && !streamNames.contains(streamName)) {
                    return false;
                }

                // Return true if all settingsFilters match for this streamConfigEntry.
                return settingsFilters.entrySet().stream()
                    .allMatch(targetSetting -> {
                        JsonNode streamSettingValue = settings.get(targetSetting.getKey());
                        return streamSettingValue != null && streamSettingValue.asText().equals(targetSetting.getValue());
                    });
            })
            .collect(Collectors.toList());

        // Rebuild an ObjectNode containing the matched stream configs.
        ObjectNode filteredStreamConfigs = JsonNodeFactory.instance.objectNode();
        for (Map.Entry<String, JsonNode> entry : filteredEntries) {
            filteredStreamConfigs.put(entry.getKey(), entry.getValue());
        }
        return filteredStreamConfigs;
    }

    /**
     * Returns all cached stream configs.
     */
    public ObjectNode cachedStreamConfigs() {
        return streamConfigsCache.deepCopy();
    }

    /**
     * Returns all cached stream name keys.
     */
    public List<String> cachedStreamNames() {
        List<String> streamNames = new ArrayList<>();
        streamConfigsCache.fieldNames().forEachRemaining(streamNames::add);
        return streamNames;
    }

    /**
     * Returns the stream config entry for a specific stream.
     * This will still return an ObjectNode mapping the
     * stream name to the stream config entry. E.g.
     *
     * <pre>
     *   getStreamConfigs(my_stream)
     *      returns
     *   { my_stream: { schema_title: my/schema, ... } }
     * </pre>
     */
    public ObjectNode getStreamConfig(String streamName) {
        return getStreamConfigs(Collections.singletonList(streamName));
    }

    /**
     * Gets the stream config entries for the desired stream names.
     * Returns a JsonNode map of stream names to stream config entries.
     */
    public ObjectNode getStreamConfigs(List<String> streamNames) {
        // If any of the desired streams are not cached, try to fetch them now and cache them.
        List<String> unfetchedStreams = streamNames.stream()
            .filter(streamName -> !streamConfigsCache.has(streamName))
            .collect(Collectors.toList());

        if (!unfetchedStreams.isEmpty()) {
            ObjectNode fetchedStreamConfigs = eventStreamConfigLoader.load(unfetchedStreams);
            streamConfigsCache.setAll(fetchedStreamConfigs);
        }

        // Return only desired stream configs.
        return streamConfigsCache.deepCopy().retain(streamNames);
    }

    /**
     * Gets a stream config setting for a specific stream.  E.g.
     *
     * <pre>
     * JsonNode setting = getSetting("mediawiki.revision-create", "destination_event_service")
     *  returns
     * TextNode("eventgate-main")
     * </pre>
     * You'll still have to pull the value out of the JsonNode wrapper yourself.
     * E.g. setting.asText() or setting.asDouble()
     *
     * If either this streamName does not have a stream config entry, or
     * the stream config entry does not have setting, this returns null.
     */
    public JsonNode getSetting(String streamName, String settingName) {
        JsonNode streamConfigEntry = getStreamConfig(streamName).get(streamName);
        if (streamConfigEntry == null) {
            return null;
        } else {
            return streamConfigEntry.get(settingName);
        }
    }

    /**
     * Gets the stream config setting for a specific stream as a string.
     * If either this streamName does not have a stream config entry, or
     * the stream config entry does not have setting, this returns null.
     *
     * <pre>
     * JsonNode setting = getSettingAsString("mediawiki.revision-create", "destination_event_service")
     *  returns
     * "eventgate-main"
     * </pre>
     *
     * If either this streamName does not have a stream config entry, or
     * the stream config entry does not have setting, this returns null.
     */
    public String getSettingAsString(String streamName, String settingName) {
        JsonNode settingNode = getSetting(streamName, settingName);
        if (settingNode == null) {
            return null;
        } else {
            return settingNode.asText();
        }
    }

    /**
     * Collects the settingName value for streamName.  If the setting value is an array,
     * it will be flattened into a list of JsonNode values.
     * <pre>
     *  { stream1: { setting1: [val1, val2] }, stream2: { setting1: [val3, val4] } returns [val1, val2, val3, val4]
     *  collectSetting("stream2", "setting1") returns [JsonNode("val3"), JsonNode("val4")]
     * </pre>
     */
    public List<JsonNode> collectSetting(String streamName, String settingName) {
        return collectSettings(Collections.singletonList(streamName), settingName);
    }

    /**
     * Collects the settingName value as a String for streamName.  If the setting value is an array,
     * it will be flattened into a list of String values.
     * <pre>
     *  { stream1: { setting1: [val1, val2] }, stream2: { setting1: [val3, val4] } returns [val1, val2, val3, val4]
     *  collectSettingAsString("stream2", "setting1") returns ["val3", "val4"]
     * </pre>
     */
    public List<String> collectSettingAsString(String streamName, String settingName) {
        return jsonNodesAsText(collectSetting(streamName, settingName));
    }

    /**
     * Collects all settingName values for each of the listed streamNames.  If any
     * encountered setting values is an array, it will be flattened.
     * <pre>
     *  { stream1: { setting1: [val1, val2] }, stream2: { setting1: [val3, val4] } returns [val1, val2, val3, val4]
     *  collectSettings(["stream1", "stream2"], "setting1") returns [JsonNode("val1"), JsonNode("val2"), JsonNode("val3"), JsonNode("val4")]
     * </pre>
     */
    public List<JsonNode> collectSettings(List<String> streamNames, String settingName) {
        return objectNodeCollectValues(getStreamConfigs(streamNames), settingName);
    }

    /**
     * Collects all settingName values as a String for each of the listed streamNames.  If any
     * encountered setting values is an array, it will be flattened.
     * <pre>
     *   { stream1: { setting1: [val1, val2] }, stream2: { setting1: [val3, val4] } returns [val1, val2, val3, val4]
     *  collectSettingsAsString(["stream1", "stream2"], "setting1") returns ["val1", "val2", "val3", "val4"]
     * </pre>
     */
    public List<String> collectSettingsAsString(List<String> streamNames, String settingName) {
        return jsonNodesAsText(collectSettings(streamNames, settingName));
    }

    /**
     * Collects all settingName values of every cached stream config entry.
     * If the value is an array, its contents will be flattened.
     * <pre>
     *  { stream1: { setting1: [val1, val2] }, stream2: { setting1: [val3, val4] } returns [val1, val2, val3, val4]
     *  collectAllCachedSettings("setting1") returns [JsonNode("val1"), JsonNode("val2"), JsonNode("val3"), JsonNode("val4")]
     * </pre>
     */
    public List<JsonNode> collectAllCachedSettings(String settingName) {
        return objectNodeCollectValues(streamConfigsCache, settingName);
    }

    /**
     * Collects all settingName values of every cached stream config entry as a String
     * If the value is an array, its contents will be flattened.
     * <pre>
     *   { stream1: { setting1: [val1, val2] }, stream2: { setting1: [val3, val4] } returns [val1, val2, val3, val4]
     *  collectAllCachedSettingsAsString(setting1") returns ["val1", "val2", "val3", "val4"]
     * </pre>
     */
    public List<String> collectAllCachedSettingsAsString(String settingName) {
        return jsonNodesAsText(collectAllCachedSettings(settingName));
    }

    /**
     * Collect all settingName values for the list of specified streams
     * with settings that match the provided settingsFilters.
     * If streamNames null, it is assumed you don't want to match on stream names,
     * and only setttingsFilters will be considered.
     *
     * Since settingsFilters must all be strings, this only allows filtering
     * on string stream config settings, or at least ones for which JsonNode.asText()
     * returns something sane (which is true for most primitive types).
     */
    public List<JsonNode> collectSettingMatchingSettings(
        String settingName,
        List<String> streamNames,
        Map<String, String> settingsFilters
    ) {
        ObjectNode filteredStreamConfigs = filterStreamConfigs(streamNames, settingsFilters);
        return objectNodeCollectValues(filteredStreamConfigs, settingName);
    }

    /**
     * Collect all settingName values as Strings for the list of specified streams
     * with settings that match the provided settingsFilters.
     * If streamNames is null, it is assumed you don't want to match on stream names,
     * and only setttingsFilters will be considered.
     *
     * Since settingsFilters must all be strings, this only allows filtering
     * on string stream config settings, or at least ones for which JsonNode.asText()
     * returns something sane (which is true for most primitive types).
     */
    public List<String> collectSettingMatchingSettingsAsString(
        String settingName,
        List<String> streamNames,
        Map<String, String> settingsFilters
    ) {
        return jsonNodesAsText(collectSettingMatchingSettings(
            settingName,
            streamNames,
            settingsFilters
        ));
    }

    /**
     * Get all topics settings for the a single stream.
     */
    public String getSchemaTitle(String streamName) {
        return getSettingAsString(streamName, SCHEMA_TITLE_SETTING);
    }

    /**
     * Get all topics settings for all known streams.
     */
    public List<String> getAllCachedTopics() {
        return collectAllCachedSettingsAsString(TOPICS_SETTING);
    }

    /**
     * Get all topics settings for the a single stream.
     */
    public List<String> getTopics(String streamName) {
        return collectSettingAsString(streamName, TOPICS_SETTING);
    }

    /**
     * Get all topics settings for the list of specified streams.
     */
    public List<String> collectTopics(List<String> streamNames) {
        return collectSettingsAsString(streamNames, TOPICS_SETTING);
    }

    /**
     * Get all topics settings for the list of specified streams
     * with settings that match the provided settingsFiilters.
     * If streamNames is null, it is assumed you don't want to match on stream names,
     * and only setttingsFilters will be considered.
     *
     * Since settingsFilters must all be strings, this only allows filtering
     * on string stream config settings, or at least ones for which JsonNode.asText()
     * returns something sane (which is true for most primitive types).
     */
    public List<String> collectTopicsMatchingSettings(
        List<String> streamNames,
        Map<String, String> settingsFilters
    ) {
        return collectSettingMatchingSettingsAsString(
            TOPICS_SETTING,
            streamNames,
            settingsFilters
        );
    }

    /**
     * Gets the destination_event_service name for the specified stream.
     */
    public String getEventServiceName(String streamName) {
        return getSettingAsString(streamName, EVENT_SERVICE_SETTING);
    }

    /**
     * Converts a list of strings to a regex that will match
     * any of the strings.  If any of the strings looks like a regex,
     * that is, it starts and ends with a "/" character, the "/" will be
     * removed from the beginning and end of the string before joining into a regex.
     *
     * Example:
     *   ("a", "/^b.+/", "c") returns "(a|^b.+|c)"
     *
     * Use this for converting a list of topics to a regex like:
     *  EventStreamConfig.toRegex(
     *      eventStreamConfig.getAllCachedTopics()
     *  );
     */
    public static String toRegex(Collection<String> strings) {
        List<String> stringsForRegex = strings.stream()
            .map((s -> {
                if (s.startsWith("/") && s.endsWith("/")) {
                    return s.substring(1, s.length() - 1);
                } else {
                    return s;
                }
            }))
            .collect(Collectors.toList());
        return "(" + String.join("|", stringsForRegex) + ")";
    }

    /**
     * Gets the event service POST URI of an event service.
     * The URI is looked up in the eventServiceToUriMap.
     * If this the eventServiceName is not defined there, returns null.
     *
     * Note that this function is not looking up anything in the source stream
     * configuration; this is a static configuration provided to this EventStreamConfig
     * class constructor that maps from destination_event_service to a URI.
     *
     * If eventServiceName is not set in eventServiceToUriMap, this throws a RuntimeException.
     */
    public URI getEventServiceUriByServiceName(String eventServiceName) {
        URI uri = eventServiceToUriMap.get(eventServiceName);
        if (uri == null) {
            throw new RuntimeException(
                "Cannot get event service URI. " + eventServiceName +
                ", is not configured in the eventServiceToUriMap"
            );
        }
        return uri;
    }

    /**
     * Gets the default event service URI for this stream via the EVENT_SERVICE_SETTING.
     */
    public URI getEventServiceUri(String streamName) {
        return getEventServiceUriByServiceName(getEventServiceName(streamName));
    }

    /**
     * Gets a datacenter specific destination event service URI for this stream
     * via the EVENT_SERVICE_SETTING + the datacenter name.
     */
    public URI getEventServiceUri(String streamName, String datacenter) {
        String defaultEventServiceName = getEventServiceName(streamName);
        String datacenterSpecificEventServiceName = defaultEventServiceName + "-" + datacenter;
        return getEventServiceUriByServiceName(datacenterSpecificEventServiceName);
    }

    public String toString() {
        return this.getClass() + " using " + eventStreamConfigLoader +
            " with event service URI mapping:\n  " +
            eventServiceToUriMap.entrySet().stream()
                .map(k -> k + " -> " + eventServiceToUriMap.get(k))
                .collect(Collectors.joining("\n  "));
    }

    /**
     * Finds all values of fieldName of each element in objectNode.
     * If the found value is an array, its contents will be flattened.
     *
     * E.g.
     * <pre>
     * { key1: { targetField: val1 }, key2: { targetField: val2 } } returns [val1, val2]
     * { key1: { targetField: [val1, val2] }, key2: { targetField: [val3, val4] } returns [val1, val2, val3, val4]
     * </pre>
     */
    protected static List<JsonNode> objectNodeCollectValues(
        ObjectNode objectNode,
        String fieldName
    ) {
        List<JsonNode> results = new ArrayList<>();

        for (JsonNode jsonNode : objectNode.findValues(fieldName)) {
            if (jsonNode.isArray()) {
                jsonNode.forEach(results::add);
            } else {
                results.add(jsonNode);
            }
        }
        return results;
    }

    /**
     * Converts a List of JsonNodes to a List of Strings using JsonNode::asText.
     */
    protected static List<String> jsonNodesAsText(Collection<JsonNode> jsonNodes) {
        return jsonNodes.stream()
            .map(JsonNode::asText)
            .collect(Collectors.toList());
    }

}
