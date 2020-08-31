package org.wikimedia.eventutilities.core.event;

import java.net.URI;
import java.util.List;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Class to aide in constructing EventStream instances and
 * working with groups of event streams using
 * EventStreamConfig and EventSchemaLoader.
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
        if (eventSchemaLoader == null) {
            throw new RuntimeException(
                "Cannot instantiate EventStreamFactory; eventSchemaLoader must not be null."
            );
        }
        if (eventStreamConfig == null) {
            throw new RuntimeException(
                "Cannot instantiate EventStreamFactory; eventStreamConfig must not be null."
            );
        }

        this.eventSchemaLoader = eventSchemaLoader;
        this.eventStreamConfig = eventStreamConfig;
    }

    /**
     * EventStreamFactoryBuilder builder pattern to construct
     * EventStreamFactory instance.  Usage:
     * <pre>
     *  EventStreamFactory f = EventStreamFactory.builder()
     *      .setEventSchemaLoader(Arrays.asList("file:///path/to/schema/repo"))
     *      .setEventStreamConfig(
     *          "https://meta.wikimedia.org/api.php",
     *          "file:///path/to/event_service_map.yaml"
     *      )
     *      .build()
     * </pre>
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

        public EventStreamFactoryBuilder setEventSchemaLoader(EventSchemaLoader eventSchemaLoader) {
            this.eventSchemaLoader = eventSchemaLoader;
            return this;
        }

        public EventStreamFactoryBuilder setEventSchemaLoader(List<String> schemaBaseUris) {
            return this.setEventSchemaLoader(new EventSchemaLoader(schemaBaseUris));
        }

        public EventStreamFactoryBuilder setEventStreamConfig(EventStreamConfig eventStreamConfig) {
            this.eventStreamConfig = eventStreamConfig;
            return this;
        }

        public EventStreamFactoryBuilder setEventStreamConfig(String streamConfigUri) {
            return this.setEventStreamConfig(EventStreamConfig.builder()
                .setEventStreamConfigLoader(streamConfigUri)
                .build()
            );
        }

        public EventStreamFactoryBuilder setEventStreamConfig(
            String streamConfigUri,
            Map<String, URI> eventServiceToUriMap
        ) {
            return this.setEventStreamConfig(
                EventStreamConfig.builder()
                .setEventStreamConfigLoader(streamConfigUri)
                .setEventServiceToUriMap(eventServiceToUriMap)
                .build()
            );
        }

        public EventStreamFactoryBuilder setEventStreamConfig(
            String streamConfigUri,
            String eventServiceConfigUri
        ) {
            return this.setEventStreamConfig(
                EventStreamConfig.builder()
                    .setEventStreamConfigLoader(streamConfigUri)
                    .setEventServiceToUriMap(eventServiceConfigUri)
                    .build()
            );
        }

        /**
         * Returns a new EventStreamFactory.  If
         * eventSchemaLoader or eventStreamConfig are not yet set,
         * new ones will be created using defaults suitable for use
         * in WMF production.
         */
        public EventStreamFactory build() {
            if (this.eventSchemaLoader == null) {
                setEventSchemaLoader(WikimediaDefaults.SCHEMA_BASE_URIS);
            }
            if (this.eventStreamConfig == null) {
                setEventStreamConfig(
                    WikimediaDefaults.EVENT_STREAM_CONFIG_URI,
                    WikimediaDefaults.EVENT_SERVICE_TO_URI_MAP
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
                .collect(Collectors.toList())
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
            .collect(Collectors.toList());
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






