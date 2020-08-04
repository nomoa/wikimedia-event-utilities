package org.wikimedia.eventutilities.core.event;

import java.net.URI;
import java.util.HashMap;

public class EventStreamConfigFactory {

    /**
     * Default Mediawiki API endpoint used to construct MediawikiEventStreamConfigLoader.
     */
    private static final String MEDIAWIKI_API_ENDPOINT_DEFAULT = "https://meta.wikimedia.org/w/api.php";

    /**
     * TODO: We should be able to get puppet to render a
     * config file with a mapping of event service name to
     * event service urls.  That would be less fragile
     * than this hardcoded list.
     * See also https://wikitech.wikimedia.org/wiki/Service_ports
     */
    public static final HashMap<String, URI> EVENT_SERVICE_TO_URI_MAP_DEFAULT =
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

    /**
     * Creates an EventStreamConfig instance loading config from the Mediawiki EventStreamConfig
     * extension API.
     * Uses MEDIAWIKI_API_ENDPOINT_DEFAULT and EVENT_SERVICE_TO_URI_MAP_DEFAULT.
     * @return
     */
    public static EventStreamConfig createMediawikiEventStreamConfig() {
        return createMediawikiEventStreamConfig(
            MEDIAWIKI_API_ENDPOINT_DEFAULT,
            EVENT_SERVICE_TO_URI_MAP_DEFAULT
        );
    }

    /**
     * Creates an EventStreamConfig instance loading config from the Mediawiki EventStreamConfig
     * extension.
     * Uses EVENT_SERVICE_TO_URI_MAP_DEFAULT.
     * @param mediawikiApiEndpoint
     * @return
     */
    public static EventStreamConfig createMediawikiEventStreamConfig(
        String mediawikiApiEndpoint
    ) {
        return createMediawikiEventStreamConfig(
            mediawikiApiEndpoint,
            EVENT_SERVICE_TO_URI_MAP_DEFAULT
        );
    }

    /**
     * Creates an EventStreamConfig instance loading config from the Mediawiki EventStreamConfig
     * extension API.
     * @param mediawikiApiEndpoint
     * @param eventServiceToUriMap
     * @return
     */
    public static EventStreamConfig createMediawikiEventStreamConfig(
        String mediawikiApiEndpoint,
        HashMap<String, URI> eventServiceToUriMap
    ) {
        return new EventStreamConfig(
            new MediawikiEventStreamConfigLoader(mediawikiApiEndpoint),
            eventServiceToUriMap
        );
    }

    /**
     * Constructs a simple EventStreamConfig instance that does not support
     * per stream config lookups.  This will just load the content at the
     * streamConfigsUriString on instantiation.
     * Uses EVENT_SERVICE_TO_URI_MAP_DEFAULT.
     *
     * @param streamConfigsUriString
     *  http://, file:// or other loadable URI.
     */
    public static EventStreamConfig createStaticEventStreamConfig(
        String streamConfigsUriString
    ) {
        return createStaticEventStreamConfig(
            streamConfigsUriString,
            EVENT_SERVICE_TO_URI_MAP_DEFAULT
        );
    }

    /**
     * Constructs a simple EventStreamConfig instance that does not support
     * per stream config lookups.  This will just load the content at the
     * streamConfigsUriString on instantiation.
     *
     * @param streamConfigsUriString
     *  http://, file:// or other loadable URI.
     *
     * @param eventServiceToUriMap
     *  Maps event service names to event service URIs.
     */
    public static EventStreamConfig createStaticEventStreamConfig(
        String streamConfigsUriString,
        HashMap<String, URI> eventServiceToUriMap
    ) {
        return new EventStreamConfig(
            new StaticEventStreamConfigLoader(streamConfigsUriString),
            eventServiceToUriMap
        );
    }
}
