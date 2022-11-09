package org.wikimedia.eventutilities.core.event;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.wikimedia.eventutilities.core.http.BasicHttpClient;
import org.wikimedia.eventutilities.core.json.JsonLoader;
import org.wikimedia.eventutilities.core.json.JsonSchemaLoader;
import org.wikimedia.eventutilities.core.util.ResourceLoader;

import com.google.common.collect.ImmutableMap;

/**
 * Default values and instances to aide working with Event Streams in
 * Wikimedia outside of production networks, e.g. schema.wikimedia.org
 * instead of schema.discovery.wmnet.
 *
 * It will not be able to use non external services like eventgate-main.
 *
 * You should only use this class for troubleshooting and debugging.
 * If you are writing code for a production job you should properly
 * configure your code externally with non hard-coded values.
 */
public final class WikimediaExternalDefaults {

    /**
     * Hostname to use when interacting with the MediaWiki action API.
     */
    public static final String MEDIAWIKI_API_HOST = "meta.wikimedia.org";

    /**
     * List of event schema base URIs used in WMF production.
     */
    public static final List<String> EVENT_SCHEMA_BASE_URIS = Collections.unmodifiableList(
        Arrays.asList(
            "https://schema.wikimedia.org/repositories/primary/jsonschema",
            "https://schema.wikimedia.org/repositories/secondary/jsonschema"
        )
    );

    /**
     * Wikimedia EventSchemaLoader default instance.
     */
    public static final EventSchemaLoader EVENT_SCHEMA_LOADER;

    /**
     * URI from which to get legacy EventLogging on wiki schemas.
     */
    public static final String EVENTLOGGING_SCHEMA_BASE_URI = "https://" + MEDIAWIKI_API_HOST + "/w/api.php";

    /**
     * Wikimedia legacy EventLoggingSchemaLoader instance.
     */
    public static final EventLoggingSchemaLoader EVENTLOGGING_SCHEMA_LOADER;

    /**
     * MediaWiki EventStreamConfig API used in WMF production to use to fetch stream configs.
     */
    public static final String EVENT_STREAM_CONFIG_URI = "https://" + MEDIAWIKI_API_HOST + "/w/api.php";

    /**
     * This default is suitable for using in WMF production networks, but
     * may become outdated.  For production jobs, you should provide this
     * yourself, or provide a URI path to a config file that specifies this.
     * See also https://wikitech.wikimedia.org/wiki/Service_ports.
     */
    public static final Map<String, URI> EVENT_SERVICE_TO_URI_MAP = ImmutableMap.<String, URI>builder()
            // eventgate-main and eventgate-analytics are not accessible from external networks.
            .put("eventgate-analytics-external", URI.create("https://intake-analytics.wikimedia.org/v1/events"))
            .put("eventgate-analytics-external-eqiad", URI.create("https://intake-analytics.wikimedia.org/v1/events"))
            .put("eventgate-analytics-external-codfw", URI.create("https://intake-analytics.wikimedia.org/v1/events"))

            .put("eventgate-logging-external", URI.create("https://intake-logging.wikimedia.org/v1/events"))
            .put("eventgate-logging-external-eqiad", URI.create("https://intake-logging.wikimedia.org/v1/events"))
            .put("eventgate-logging-external-codfw", URI.create("https://intake-logging.wikimedia.org/v1/events"))
            .build();

    /**
     * Http client with routes set to work on the internal wikimedia network.
     */
    public static final BasicHttpClient WIKIMEDIA_HTTP_CLIENT;

    /**
     * Wikimedia EventStreamConfig default instance.
     */
    public static final EventStreamConfig EVENT_STREAM_CONFIG;
    static {
        WIKIMEDIA_HTTP_CLIENT = BasicHttpClient.builder().build();

        EVENTLOGGING_SCHEMA_LOADER = new EventLoggingSchemaLoader(
            JsonSchemaLoader.build(ResourceLoader.builder()
                .withHttpClient(WIKIMEDIA_HTTP_CLIENT)
                .setBaseUrls(ResourceLoader.asURLs(Collections.singletonList(EVENTLOGGING_SCHEMA_BASE_URI)))
                .build()
            )
        );

        MediawikiEventStreamConfigLoader eventStreamConfigLoader = new MediawikiEventStreamConfigLoader(
            EVENT_STREAM_CONFIG_URI,
            new JsonLoader(ResourceLoader.builder().withHttpClient(WIKIMEDIA_HTTP_CLIENT).build())
        );

        EVENT_STREAM_CONFIG = EventStreamConfig.builder()
            .setEventStreamConfigLoader(eventStreamConfigLoader)
            .setEventServiceToUriMap(EVENT_SERVICE_TO_URI_MAP)
            .build();

        EVENT_SCHEMA_LOADER = EventSchemaLoader.builder()
            .setJsonSchemaLoader(JsonSchemaLoader.build(
                ResourceLoader.builder()
                    .withHttpClient(WIKIMEDIA_HTTP_CLIENT)
                    .setBaseUrls(ResourceLoader.asURLs(EVENT_SCHEMA_BASE_URIS))
                    .build()
            ))
            .build();
    }

    /**
     * Wikimedia EventStreamFactory default instance.
     */
    public static final EventStreamFactory EVENT_STREAM_FACTORY = EventStreamFactory.builder()
        .setEventSchemaLoader(EVENT_SCHEMA_LOADER)
        .setEventStreamConfig(EVENT_STREAM_CONFIG)
        .build();

    /**
     * Wikimedia EventSchemaValidator default instance.
     */
    public static final EventSchemaValidator EVENT_SCHEMA_VALIDATOR = new EventSchemaValidator(EVENT_SCHEMA_LOADER);

    /**
     * Wikimedia JsonEventGenerator default instance.
     */
    public static final JsonEventGenerator EVENT_GENERATOR = JsonEventGenerator.builder()
        .schemaLoader(EVENT_SCHEMA_LOADER)
        .eventStreamConfig(EVENT_STREAM_CONFIG)
        .build();

    /**
     * Constructor to make maven checkstyle plugin happy.
     * See: https://checkstyle.sourceforge.io/config_design.html#HideUtilityClassConstructor
     */
    private WikimediaExternalDefaults() {
        // prevents calls from subclass
        throw new UnsupportedOperationException();
    }
}
