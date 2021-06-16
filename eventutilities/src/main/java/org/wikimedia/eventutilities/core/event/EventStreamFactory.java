package org.wikimedia.eventutilities.core.event;

import static com.google.common.collect.ImmutableList.toImmutableList;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Class to aide in constructing {@link EventStream} instances and
 * working with groups of event streams using
 * {@link EventStreamConfig} and {@link EventSchemaLoader}.
 */
public class EventStreamFactory {
    /**
     * EventSchemaLoader instance used when constructing EventStreams.
     */
    protected final EventSchemaLoader eventSchemaLoader;

    /**
     * EventStreamConfig instance used when constructing EventStreams and
     * looking up stream configs.
     */
    protected EventStreamConfig eventStreamConfig;

    /**
     * Constructs a new instance of EventStreamFactory.
     * This is protected; use EventStreamFactory.builder()
     * to create your EventStreamFactory instance.
     */
    protected EventStreamFactory(
        EventSchemaLoader eventSchemaLoader,
        EventStreamConfig eventStreamConfig
    ) {
        this.eventSchemaLoader = eventSchemaLoader;
        this.eventStreamConfig = eventStreamConfig;
    }

    /**
     * EventStreamFactoryBuilder builder pattern to construct
     * EventStreamFactory instance.  Usage:
     * <code>
     *  EventStreamFactory f = EventStreamFactory.builder()
     *      .setEventSchemaLoader(Arrays.asList("file:///path/to/schema/repo"))
     *      .setEventStreamConfig(
     *          "https://meta.wikimedia.org/w/api.php", // MW Stream Config URL, will use MediawikiEventStreamConfigLoader
     *          "file:///path/to/event_service_map.yaml" // event service to URI map config file.
     *      )
     *      .build()
     * </code>
     *
     * The default EventStreamFactory returned by .build() will be suitable for use
     * in WMF production, connected to MediaWiki EventStreamConfig API using
     * event schemas hosted schema.wikimedia.org, as specified in WikimediaDefaults.
     *
     * <pre>
     *  EventStreamFactory wmfEventStreamFactory = EventStreamFactory.builder().build()
     * </pre>
     */
    public static class EventStreamFactoryBuilder {
        private EventSchemaLoader eventSchemaLoader;
        private EventStreamConfig eventStreamConfig;

        /**
         * Set the EventSchemaLoader.
         */
        public EventStreamFactoryBuilder setEventSchemaLoader(EventSchemaLoader eventSchemaLoader) {
            this.eventSchemaLoader = eventSchemaLoader;
            return this;
        }

        public EventStreamFactoryBuilder setEventStreamConfig(EventStreamConfig eventStreamConfig) {
            this.eventStreamConfig = eventStreamConfig;
            return this;
        }

        /**
         * Returns a new EventStreamFactory.  If
         * eventSchemaLoader or eventStreamConfig are not yet set,
         * an IllegalArgumentException will be thrown.
         */
        public EventStreamFactory build() {
            if (this.eventSchemaLoader == null) {
                throw new IllegalArgumentException(
                    "Must call setEventSchemaLoader() before calling build()."
                );
            }

            if (this.eventStreamConfig == null) {
                throw new IllegalArgumentException(
                    "Must call eventStreamConfig() before calling build()."
                );
            }

            return new EventStreamFactory(
                this.eventSchemaLoader,
                this.eventStreamConfig
            );
        }
    }

    public static EventStreamFactoryBuilder builder() {
        return new EventStreamFactoryBuilder();
    }

    /**
     * Creates EventStreams for all streams in our EventStreamConfig's cache.
     * This will exclude any stream name that looks like a regex in the config,
     * as it doesn't make sense to construct an EventStream without a concrete stream name.
     */
    public List<EventStream> createAllCachedEventStreams() {
        return createEventStreams(
            eventStreamConfig.cachedStreamNames().stream()
                .filter(streamName -> !streamName.startsWith("/"))
                .collect(toImmutableList())
        );
    }

    /**
     * Returns a new EventStream for this streamName using eventSchemaLoader and
     * eventStreamConfig.
     */
    public EventStream createEventStream(String streamName) {
        return new EventStream(streamName, eventSchemaLoader, eventStreamConfig);
    }

    /**
     * Returns a List of new EventStreams using eventSchemaLoader and
     * eventStreamConfig.
     */
    public List<EventStream> createEventStreams(Collection<String> streamNames) {
        return streamNames.stream()
            .map(this::createEventStream)
            .collect(toImmutableList());
    }

    /**
     * Creates EventStreams for the list of specified streams
     * with settings that match the provided settingsFilters.
     * If streamNames is null, it is assumed you don't want to match on stream names,
     * and only setttingsFilters will be considered.
     *
     * Since settingsFilters must all be strings, this only allows filtering
     * on string stream config settings, or at least ones for which JsonNode.asText()
     * returns something sane (which is true for most primitive types).
     */
    public List<EventStream> createEventStreamsMatchingSettings(
        Collection<String> streamNames,
        Map<String, String> settingsFilters
    ) {
        List<EventStream> eventStreams;
        if (streamNames != null) {
            eventStreams = createEventStreams(streamNames);
        } else {
            eventStreams = createAllCachedEventStreams();
        }

        return eventStreams.stream()
            .filter(eventStream -> settingsFilters.entrySet().stream()
                .allMatch(settingEntry -> {
                    JsonNode streamSetting = eventStream.getSetting(settingEntry.getKey());
                    return streamSetting != null && streamSetting.asText().equals(settingEntry.getValue());
                }))
            .collect(toImmutableList());
    }

    /**
     * Returns the EventStreamConfig instance this EventStreamFactory is using.
     */
    public EventStreamConfig getEventStreamConfig() {
        return eventStreamConfig;
    }

    /**
     * Returns the EventSchemaLoader instance this EventStreamFactory is using.
     */
    public EventSchemaLoader getEventSchemaLoader() {
        return eventSchemaLoader;
    }

}






