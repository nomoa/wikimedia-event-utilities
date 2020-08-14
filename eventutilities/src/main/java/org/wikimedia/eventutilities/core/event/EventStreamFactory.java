package org.wikimedia.eventutilities.core.event;

import static org.wikimedia.eventutilities.core.event.EventStreamConfigFactory.EVENT_SERVICE_TO_URI_MAP_DEFAULT;

import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * Class to aide in constructing EventStream instances and
 * working with groups of event streams using
 * EventStreamConfig API and event schema repositories.
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
     * Constructs a new EventStreamFactory with new instances of
     * EventSchemaLoader and EventStreamConfig with the given URIs.
     */
    public EventStreamFactory(
        List<String> schemaBaseUris,
        EventStreamConfig eventStreamConfig
    ) {
        this(
            new EventSchemaLoader(schemaBaseUris),
            eventStreamConfig
        );
    }

    /**
     * Constructs a new instance of EventStreamFactory.
     */
    public EventStreamFactory(
        EventSchemaLoader eventSchemaLoader,
        EventStreamConfig eventStreamConfig
    ) {
        this.eventSchemaLoader = eventSchemaLoader;
        this.eventStreamConfig = eventStreamConfig;
    }

    /**
     * Creates an EventStreamFactory that creates EventStreams
     * using the Mediawiki EventStreamConfig API at mediawikiApiEndpoint.
     */
    public static EventStreamFactory createMediawikiConfigEventStreamFactory(
        List<String> schemaBaseUris,
        String mediawikiApiEndpoint,
        Map<String, URI> eventServiceToUriMap
    ) {
        return new EventStreamFactory(
            schemaBaseUris,
            EventStreamConfigFactory.createMediawikiEventStreamConfig(
                mediawikiApiEndpoint,
                eventServiceToUriMap
            )
        );
    }

    /**
     * Creates an EventStreamFactory that creates EventStreams
     * using the Mediawiki EventStreamConfig API at mediawikiApiEndpoint
     * and the default eventServiceToUriMap.
     */
    public static EventStreamFactory createMediawikiConfigEventStreamFactory(
        List<String> schemaBaseUris,
        String mediawikiApiEndpoint
    ) {
        return new EventStreamFactory(
            schemaBaseUris,
            EventStreamConfigFactory.createMediawikiEventStreamConfig(
                mediawikiApiEndpoint
            )
        );
    }

    /**
     * Creates an EventStreamFactory that creates EventStreams
     * using the Mediawiki EventStreamConfig API at mediawikiApiEndpoint
     * and the default mediawikiApiEndpoint and eventServiceToUriMap.
     */
    public static EventStreamFactory createMediawikiConfigEventStreamFactory(
        List<String> schemaBaseUris
    ) {
        return new EventStreamFactory(
            schemaBaseUris,
            EventStreamConfigFactory.createMediawikiEventStreamConfig()
        );
    }


    /**
     * Creates an EventStreamFactory that creates EventStreams
     * using a static stream config local or remote file.
     */
    public static EventStreamFactory createStaticConfigEventStreamFactory(
        List<String> schemaBaseUris,
        String streamConfigsUriString,
        Map<String, URI> eventServiceToUriMap
    ) {
        return new EventStreamFactory(
            schemaBaseUris,
            EventStreamConfigFactory.createStaticEventStreamConfig(
                streamConfigsUriString,
                eventServiceToUriMap
            )
        );
    }

    /**
     * Creates an EventStreamFactory that creates EventStream
     * using a static stream config local or remote file
     * and the default eventServiceToUriMap.
     */
    public static EventStreamFactory createStaticConfigEventStreamFactory(
        List<String> schemaBaseUris,
        String streamConfigsUriString
    ) {
        return new EventStreamFactory(
            schemaBaseUris,
            EventStreamConfigFactory.createStaticEventStreamConfig(
                streamConfigsUriString,
                    EVENT_SERVICE_TO_URI_MAP_DEFAULT)
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






