package org.wikimedia.eventutilities.core.event;


import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.stream.Collectors.joining;

import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.wikimedia.eventutilities.core.json.JsonLoader;
import org.wikimedia.eventutilities.core.json.JsonLoadingException;
import org.wikimedia.eventutilities.core.util.ResourceLoader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Class to fetch and work with stream configuration from the a URI.
 * Upon instantiation, this will attempt to pre fetch and cache all stream config from
 * streamConfigsUri.  Further accesses of uncached stream names will cause
 * a fetch from the result of makeStreamConfigsUriForStreams for the uncached stream names only.
 *
 * Wherever a settingName parameter is used, if it begins with a '/', it will be assumed
 * the settingName is a JsonPointer path, instead of a top level key.
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
    protected ImmutableMap<String, URI> eventServiceToUriMap;

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
        Preconditions.checkArgument(
                eventStreamConfigLoader != null,
                "Cannot instantiate EventStreamConfig; eventStreamConfigLoader must not be null.");

        Preconditions.checkArgument(
                eventServiceToUriMap != null,
                "Cannot instantiate EventStreamConfig; eventServiceToUriMap must not be null.");

        this.eventStreamConfigLoader = eventStreamConfigLoader;
        this.eventServiceToUriMap = ImmutableMap.copyOf(eventServiceToUriMap);

        // Get and store all known stream configs.
        // The stream configs endpoint should return an object mapping
        // stream names (or regex patterns) to stream config entries.
        this.reset();
    }

    /**
     * Builder, builder pattern to construct
     * EventStreamConfig instances.  Usage:
     * <pre>{@code
     *  EventStreamConfig c = EventStreamConfig.builder()
     *      .setEventStreamConfigLoader("https://meta.wikimedia.org/w/api.php")
     *      .setEventServiceToUriMap("file:///path/to/event_service_map.yaml")
     *      .build()
     * }</pre>
     */
    public static class Builder {
        private EventStreamConfigLoader eventStreamConfigLoader;
        private String eventStreamConfigLoaderUri;
        private Map<String, URI> eventServiceToUriMap;
        private String eventServiceToUriMapUri;
        private JsonLoader jsonLoader;

        public Builder setEventStreamConfigLoader(EventStreamConfigLoader eventStreamConfigLoader) {
            this.eventStreamConfigLoader = eventStreamConfigLoader;
            this.eventStreamConfigLoaderUri = null;
            return this;
        }

        public Builder setEventStreamConfigLoader(String streamConfigUri) {
            this.eventStreamConfigLoaderUri = streamConfigUri;
            this.eventStreamConfigLoader = null;
            return this;
        }

        public Builder setEventServiceToUriMap(Map<String, URI> eventServiceToUriMap) {
            this.eventServiceToUriMap = ImmutableMap.copyOf(eventServiceToUriMap);
            this.eventServiceToUriMapUri = null;
            return this;
        }

        public Builder setEventServiceToUriMap(String eventServiceToUriMapUri) {
            this.eventServiceToUriMapUri = eventServiceToUriMapUri;
            this.eventServiceToUriMap = null;
            return this;
        }

        public Builder setJsonLoader(JsonLoader jsonLoader) {
            this.jsonLoader = jsonLoader;
            return this;
        }

        /**
         * Returns a new EventStreamConfig.  setEventStreamConfigLoader and setEventServiceToUriMap must
         * have been called before calling build or an IllegalArgumentException will be thrown.
         */
        public EventStreamConfig build() {
            Preconditions.checkState(
                    eventStreamConfigLoader != null || this.eventStreamConfigLoaderUri != null,
                    "Must call setEventStreamConfigLoader() before calling build()");

            if (this.eventStreamConfigLoader == null) {
                this.eventStreamConfigLoader = buildEventStreamConfigLoader();
            }

            if (this.eventServiceToUriMap == null) {
                if (eventServiceToUriMapUri != null) {
                    this.eventServiceToUriMap = loadEventServiceConfig(
                        eventServiceToUriMapUri, getJsonLoader()
                    );
                } else {
                    this.eventServiceToUriMap = new HashMap<String, URI>();
                }
            }

            return new EventStreamConfig(
                this.eventStreamConfigLoader,
                this.eventServiceToUriMap
            );
        }

        private EventStreamConfigLoader buildEventStreamConfigLoader() {
            // If we get here we know that eventStreamConfigLoaderUri is not null.
            if (eventStreamConfigLoaderUri.contains("/api.php")) {
                // Assume eventStreamConfigLoaderUri is a MediawikiEventStreamConfigLoader
                return new MediawikiEventStreamConfigLoader(
                    eventStreamConfigLoaderUri, getJsonLoader()
                );
            } else {
                // Else assume eventStreamConfigLoaderUri is a StaticEventStreamConfigLoader
                return new StaticEventStreamConfigLoader(
                    URI.create(eventStreamConfigLoaderUri), getJsonLoader()
                );
            }
        }

        private JsonLoader getJsonLoader() {
            if (jsonLoader == null) {
                jsonLoader = new JsonLoader(ResourceLoader.builder().build());
            }
            return jsonLoader;
        }

        /**
         * Loads the YAML or JSON at eventServiceConfigUri into a Map.
         * This expects that the JSON object simply maps an event intake service name to
         * an event intake service URI.
         * @param eventServiceConfigUri
         *  http://, file:// or other URI that the JsonLoader can load.
         */
        private static Map<String, URI> loadEventServiceConfig(
            String eventServiceConfigUri,
            JsonLoader jsonLoader
        ) {
            Map<String, String> loadedConfig;
            try {
                // Load eventServiceConfigUri and convert it to a HashMap.
                loadedConfig = jsonLoader.convertValue(
                    jsonLoader.load(URI.create(eventServiceConfigUri)), HashMap.class
                );
            } catch (JsonLoadingException e) {
                throw new IllegalArgumentException(
                    "Failed loading event service config from " + eventServiceConfigUri, e
                );
            }

            // Convert our String -> String HashMap into String -> URI.
            return new HashMap<>(loadedConfig.entrySet().stream()
                .collect(toImmutableMap(
                    Map.Entry::getKey,
                    e -> URI.create(e.getValue()))
                )
            );
        }
    }

    /**
     * Returns an Builder instance.
     */
    public static Builder builder() {
        return new Builder();
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
     * The keys in settingsFilters are assumed to top level field names, unless
     * the key starts with a '/', in which case it will be assumed to be a JsonPointer.
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
                        String settingName = targetSetting.getKey();

                        JsonNode streamSettingValue;
                        if (settingName.startsWith("/")) {
                            streamSettingValue = settings.at(settingName);
                        } else {
                            streamSettingValue = settings.get(settingName);
                        }
                        return streamSettingValue != null &&
                            !streamSettingValue.isMissingNode() &&
                            streamSettingValue.asText().equals(targetSetting.getValue());
                    });
            })
            .collect(toImmutableList());

        // Rebuild an ObjectNode containing the matched stream configs.
        ObjectNode filteredStreamConfigs = JsonNodeFactory.instance.objectNode();
        for (Map.Entry<String, JsonNode> entry : filteredEntries) {
            filteredStreamConfigs.set(entry.getKey(), entry.getValue());
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
        ImmutableList.Builder<String> streamNames = ImmutableList.builder();
        streamConfigsCache.fieldNames().forEachRemaining(streamNames::add);
        return streamNames.build();
    }

    /**
     * Returns the stream config entry for a specific stream.
     * This will still return an ObjectNode mapping the
     * stream name to the stream config entry. E.g.
     *
     * <pre>{@code
     *   getStreamConfigs(my_stream)
     *      returns
     *   { my_stream: { schema_title: my/schema, ... } }
     * }</pre>
     */
    public ObjectNode getStreamConfig(String streamName) {
        return getStreamConfigs(ImmutableList.of(streamName));
    }

    /**
     * Gets the stream config entries for the desired stream names.
     * Returns a JsonNode map of stream names to stream config entries.
     */
    public ObjectNode getStreamConfigs(List<String> streamNames) {
        // If any of the desired streams are not cached, try to fetch them now and cache them.
        List<String> unfetchedStreams = streamNames.stream()
            .filter(streamName -> !streamConfigsCache.has(streamName))
            .collect(toImmutableList());

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
     * <pre>{@code
     * JsonNode setting = getSetting("mediawiki.revision-create", "destination_event_service")
     *  returns
     * TextNode("eventgate-main")
     * }</pre>
     * You'll still have to pull the value out of the JsonNode wrapper yourself.
     * E.g. setting.asText() or setting.asDouble()
     *
     * If either this streamName does not have a stream config entry, or
     * the stream config entry does not have setting, this returns null.
     *
     * @param streamName name of stream in stream config
     * @param settingName setting name to get, either top level field name, or JsonPointer
     *                    starting with '/'.
     */
    public JsonNode getSetting(String streamName, String settingName) {
        JsonNode streamConfigEntry = getStreamConfig(streamName).get(streamName);

        if (streamConfigEntry == null) {
            return null;
        } else {
            if (settingName.startsWith("/")) {
                JsonNode result = streamConfigEntry.at(settingName);
                if (result.isMissingNode()) {
                    return null;
                } else {
                    return result;
                }
            } else {
                return streamConfigEntry.get(settingName);
            }
        }
    }

    /**
     * Gets the stream config setting for a specific stream as a string.
     * If either this streamName does not have a stream config entry, or
     * the stream config entry does not have setting, this returns null.
     *
     * {@code JsonNode setting = getSettingAsString("mediawiki.revision-create", "destination_event_service")}
     * returns "eventgate-main"
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
        return collectSettings(ImmutableList.of(streamName), settingName);
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
     * Get all topics settings for a single stream.
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
     * <pre>
     *   ("a", "/^b.+/", "c") returns "(a|^b.+|c)"
     * </pre>
     *
     * Use this for converting a list of topics to a regex like:
     *
     * <pre>{@code
     *  EventStreamConfig.toRegex(
     *      eventStreamConfig.getAllCachedTopics()
     *  );
     * }</pre>
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
            .collect(toImmutableList());
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
            throw new IllegalArgumentException(
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
                .collect(joining("\n  "));
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
        ImmutableList.Builder<JsonNode> results = ImmutableList.builder();

        for (JsonNode jsonNode : objectNode.findValues(fieldName)) {
            if (jsonNode.isArray()) {
                jsonNode.forEach(results::add);
            } else {
                results.add(jsonNode);
            }
        }
        return results.build();
    }

    /**
     * Converts a List of JsonNodes to a List of Strings using JsonNode::asText.
     */
    protected static List<String> jsonNodesAsText(Collection<JsonNode> jsonNodes) {
        return jsonNodes.stream()
            .map(JsonNode::asText)
            .collect(toImmutableList());
    }

}
