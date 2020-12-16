package org.wikimedia.eventutilities.core.event;

import java.net.URI;
import java.util.Collections;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.List;

public class WikimediaDefaults {
    /**
     * List of event schema base URIs used in WMF production.
     */
    public static final List<String> SCHEMA_BASE_URIS = Collections.unmodifiableList(
        Arrays.asList(
            "https://schema.wikimedia.org/repositories/primary/jsonschema",
            "https://schema.wikimedia.org/repositories/secondary/jsonschema"
        )
    );

    /**
     * MediaWiki EventStreamConfig API used in WMF production to use to fetch stream configs.
     */
    public static final String EVENT_STREAM_CONFIG_URI = "https://meta.wikimedia.org/w/api.php";

    /**
     * This default is suitable for using in WMF production networks, but
     * may become outdated.  For production jobs, you should provide this
     * yourself, or provide a URI path to a config file that specifies this.
     * See also https://wikitech.wikimedia.org/wiki/Service_ports
     * Used by EventStreamFactoryBuilder if setEventStreamConfig is never called.
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
     * Constructor to make maven checkstyle plugin happy.
     * See: https://checkstyle.sourceforge.io/config_design.html#HideUtilityClassConstructor
     */
    protected WikimediaDefaults() {
        // prevents calls from subclass
        throw new UnsupportedOperationException();
    }
}
