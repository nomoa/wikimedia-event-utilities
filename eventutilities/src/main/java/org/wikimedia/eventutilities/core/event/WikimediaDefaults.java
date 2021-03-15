package org.wikimedia.eventutilities.core.event;

import java.net.MalformedURLException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.wikimedia.eventutilities.core.http.BasicHttpClient;
import org.wikimedia.eventutilities.core.json.JsonLoader;
import org.wikimedia.eventutilities.core.json.JsonSchemaLoader;
import org.wikimedia.eventutilities.core.util.ResourceLoader;

/**
 * Default values and instances to aide working with Event Streams in
 * Wikimedia Production.
 *
 * You should only use this class for troubleshooting and debugging.
 * If you are writing code for a production job you should properly
 * configure your code externally with non hard-coded values.
 */
public class WikimediaDefaults {

    /**
     * Hostname to use when interacting with the MediaWiki action API.
     */
    public static final String MEDIAWIKI_API_HOST = "meta.wikimedia.org";

    /**
     * List of event schema base URIs used in WMF production.
     */
    public static final List<String> EVENT_SCHEMA_BASE_URIS = Collections.unmodifiableList(
        Arrays.asList(
            "https://schema.discovery.wmnet/repositories/primary/jsonschema",
            "https://schema.discovery.wmnet/repositories/secondary/jsonschema"
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
     * In production, api-ro needs the Host header set to the MediaWiki API that should be accessed.
     */
    public static final String EVENT_STREAM_CONFIG_ENDPOINT = "https://api-ro.discovery.wmnet";

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
    public static final Map<String, URI> EVENT_SERVICE_TO_URI_MAP = Collections.unmodifiableMap(
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
        }}
    );

    /**
     * Http client with routes set to work on the internal wikimedia network.
     */
    public static final BasicHttpClient WIKIMEDIA_HTTP_CLIENT;

    /**
     * Wikimedia EventStreamConfig default instance.
     */
    public static final EventStreamConfig EVENT_STREAM_CONFIG;
    static {
        try {
            WIKIMEDIA_HTTP_CLIENT = BasicHttpClient.builder()
                // Add a custom route to api-ro.wikimedia.org.
                .addRoute("https://" + MEDIAWIKI_API_HOST, EVENT_STREAM_CONFIG_ENDPOINT)
                .build();
        } catch (MalformedURLException mue) {
            throw new IllegalArgumentException(mue);
        }

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
    protected WikimediaDefaults() {
        // prevents calls from subclass
        throw new UnsupportedOperationException();
    }
}
