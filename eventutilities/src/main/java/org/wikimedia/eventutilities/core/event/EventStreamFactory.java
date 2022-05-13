package org.wikimedia.eventutilities.core.event;

import static com.google.common.collect.ImmutableList.toImmutableList;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.wikimedia.eventutilities.core.http.BasicHttpClient;
import org.wikimedia.eventutilities.core.json.JsonLoader;
import org.wikimedia.eventutilities.core.json.JsonSchemaLoader;
import org.wikimedia.eventutilities.core.util.ResourceLoader;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;

/**
 * Class to aide in constructing {@link EventStream} instances and
 * working with groups of event streams using
 * {@link EventStreamConfig} and {@link EventSchemaLoader}.
 *
 * Use EventStreamFactory.builder() or one of the EventStreamFactory.from()
 * methods to construct an EventStreamFactory.
 *
 * Example using builder():
 * <code>
 *  EventStreamFactory f = EventStreamFactory.builder()
 *      .setEventSchemaLoader(Arrays.asList("file:///path/to/schema/repo"))
 *      // MW Stream Config URL, will use MediawikiEventStreamConfigLoader
 *      .setEventStreamConfig(
 *          "https://meta.wikimedia.org/w/api.php",
 *      )
 *       // in prod networks, we need special routing for MW API, optional.
 *      .setHttpRoute("https://meta.wikimedia.org", "https://api-ro.wikimedia.org")
 *      // event service to URI map config file, optional.
 *      .setEventServiceToUriMap(
 *          "file:///path/to/event_service_map.yaml"
 *       )
 *      .build()
 * </code>
 *
 * Example using from():
 * <code>
 *     EventStreamFactory f = EventStreamFactory.from(
 *          Arrays.asList(
 *              "https://schema.wikimedia.org/repositories/primary",
 *              "https://schema.wikimedia.org/repositories/secondary",
 *          ),
 *          "https://meta.wikimedia.org/w/api.php/",
 *     );
 * </code>
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
     * or one of the EventStreamFactory.of(...) factories.
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
     * Helper function to quickly instantiate EventStreamFactory.
     */
    public static EventStreamFactory from(
        List<String> eventSchemaBaseUris,
        String eventStreamConfigUri
    ) {
        return from(
            eventSchemaBaseUris,
            eventStreamConfigUri,
            null,
            null
        );
    }

    /**
     * Helper function to quickly instantiate EventStreamFactory.
     */
    public static EventStreamFactory from(
        List<String> eventSchemaBaseUris,
        String eventStreamConfigUri,
        Map<String, String> httpRoutes
    ) {
        return from(
            eventSchemaBaseUris,
            eventStreamConfigUri,
            httpRoutes,
            null
        );
    }

    /**
     * Helper function to quickly instantiate EventStreamFactory.
     */
    public static EventStreamFactory from(
        List<String> eventSchemaBaseUris,
        String eventStreamConfigUri,
        Map<String, String> httpRoutes,
        String eventServiceToUriMapUrl
    ) {
        Builder builder = EventStreamFactory.builder();

        builder.setEventSchemaLoader(eventSchemaBaseUris);
        builder.setEventStreamConfig(eventStreamConfigUri);

        if (eventServiceToUriMapUrl != null) {
            builder.setEventServiceToUriMap(eventServiceToUriMapUrl);
        }

        if (httpRoutes != null) {
            builder.setHttpRoutes(httpRoutes);
        }

        return builder.build();
    }

    /**
     * Builder builder pattern to construct EventStreamFactory instance.
     */
    public static class Builder {
        private List<String> eventSchemaBaseUris;
        private EventSchemaLoader eventSchemaLoader;

        private String eventStreamConfigUri;
        private String eventServiceToUriMapUrl;
        private Map<String, String> eventServiceToUriMap;
        private EventStreamConfig eventStreamConfig;

        private Map<String, String> httpRoutes = new HashMap<>();


        // setEventSchemaLoader by URIs
        public Builder setEventSchemaLoader(List<String> eventSchemaBaseUris) {
            this.eventSchemaBaseUris = eventSchemaBaseUris;
            return this;
        }
        // setEventSchemaLoader by instantiated eventSchemaLoader.
        // If you use this, any provided eventSchemaBaseUris
        // or httpRoutes will be ignored.
        public Builder setEventSchemaLoader(EventSchemaLoader eventSchemaLoader) {
            this.eventSchemaLoader = eventSchemaLoader;
            return this;
        }

        // setEventServiceToUriMap by config file
        public Builder setEventServiceToUriMap(String eventServiceToUriMapUrl) {
            this.eventServiceToUriMapUrl = eventServiceToUriMapUrl;
            return this;
        }
        // setEventServiceToUriMap by instantiated Map
        public Builder setEventServiceToUriMap(Map<String, String> eventServiceToUriMap) {
            this.eventServiceToUriMap = eventServiceToUriMap;
            return this;
        }

        // Set setEventStreamConfig by URI
        public Builder setEventStreamConfig(String eventStreamConfigUri) {
            this.eventStreamConfigUri = eventStreamConfigUri;
            return this;
        }
        // setEventStreamConfig by instantiated eventStreamConfig.
        // If you use this, any provided httpRoutes or setEventServiceToUriMap
        // will be ignored.
        public Builder setEventStreamConfig(EventStreamConfig eventStreamConfig) {
            this.eventStreamConfig = eventStreamConfig;
            return this;
        }

        // Add an httpRoute by source and dest url
        public Builder setHttpRoute(String sourceUrl, String destUrl) {
            this.httpRoutes.put(sourceUrl, destUrl);
            return this;
        }
        // Set the httpRoutes, if you use this, any previously set
        // http routes will be ignored.
        public Builder setHttpRoutes(Map<String, String> httpRoutes) {
            this.httpRoutes = httpRoutes;
            return this;
        }

        /**
         * Returns a new EventStreamFactory.
         * If not enough has been provided to build an EventStreamFactory
         * an IllegalArgumentException will be thrown.
         */
        public EventStreamFactory build() {
            Preconditions.checkState(
                !(eventSchemaLoader == null && eventSchemaBaseUris == null),
                "Must call setEventSchemaLoader() before calling build().");

            Preconditions.checkState(
                !(eventStreamConfig == null && eventStreamConfigUri == null),
                "Must call eventStreamConfig() before calling build().");

            // Build a BasicHttpClient with configured routes
            // that will be used for one or both of our eventSchemaLoader
            // and eventStreamConfig.
            BasicHttpClient.Builder httpClientBuilder = BasicHttpClient.builder();
            httpRoutes.forEach((source, dest) -> {
                try {
                    httpClientBuilder.addRoute(source, dest);
                } catch (MalformedURLException e) {
                    throw new IllegalArgumentException(e);
                }
            });
            BasicHttpClient httpClient = httpClientBuilder.build();

            // Build the eventSchemaLoader from eventSchemaBaseUris.
            if (this.eventSchemaLoader == null) {
                List<URL> baseUrls = eventSchemaBaseUris.stream().map((s) -> {
                    try {
                        return URI.create(s).toURL();
                    } catch (MalformedURLException e) {
                        throw new IllegalArgumentException(e);
                    }
                }).collect(Collectors.toList());

                JsonLoader jsonLoader = new JsonLoader(ResourceLoader.builder()
                    .withHttpClient(httpClient)
                    .setBaseUrls(baseUrls)
                    .build()
                );

                this.eventSchemaLoader = EventSchemaLoader.builder()
                    .setJsonSchemaLoader(new JsonSchemaLoader(jsonLoader))
                    .build();
            }

            // Build the eventStreamConfig from eventStreamConfigUri
            if (this.eventStreamConfig == null) {
                JsonLoader jsonLoader = new JsonLoader(ResourceLoader.builder()
                        .withHttpClient(httpClient)
                        .build()
                );
                EventStreamConfig.Builder eventStreamConfigBuilder = EventStreamConfig.builder()
                    .setJsonLoader(jsonLoader)
                    .setEventStreamConfigLoader(eventStreamConfigUri);

                // If either eventServiceToUriMap or eventServiceToUriMapUrl are set,
                // then call setEventServiceToUriMap on the builder with one of them.
                if (this.eventServiceToUriMap != null) {
                    // Convert our String, String eventServiceToUriMap to String, URI.
                    Map<String, URI> uriMap = new HashMap<>();
                    for (Map.Entry<String, String> entry : this.eventServiceToUriMap.entrySet()) {
                        uriMap.put(entry.getKey(), URI.create(entry.getValue()));
                    }
                    eventStreamConfigBuilder.setEventServiceToUriMap(uriMap);
                } else if (this.eventServiceToUriMapUrl != null) {
                    eventStreamConfigBuilder.setEventServiceToUriMap(this.eventServiceToUriMapUrl);
                }

                this.eventStreamConfig = eventStreamConfigBuilder.build();
            }

            // Finally, return the new EventStreamFactory
            return new EventStreamFactory(
                this.eventSchemaLoader,
                this.eventStreamConfig
            );
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    // --- actual instance methods below ---

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






