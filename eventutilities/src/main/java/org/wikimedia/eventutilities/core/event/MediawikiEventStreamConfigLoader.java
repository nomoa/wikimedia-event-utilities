package org.wikimedia.eventutilities.core.event;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.net.URI;
import java.net.URLEncoder;
import java.util.List;
import java.util.Locale;

import org.wikimedia.eventutilities.core.json.JsonLoader;
import org.wikimedia.eventutilities.core.json.JsonLoadingException;

import com.fasterxml.jackson.databind.node.ObjectNode;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * EventStreamConfigLoader implementation that loads stream config
 * from a remote Mediawiki EventStreamConfig extension API endpoint.
 */
public class MediawikiEventStreamConfigLoader extends EventStreamConfigLoader {

    /**
     * Used to fetch JSON stream configs from the mediawikiApieEndpoint.
     */
    private final JsonLoader jsonLoader;

    /**
     * Base Mediawiki API endpoint, e.g https://meta.wikimedia.org/w/api.php.
     */
    protected String mediawikiApiEndpoint;

    /**
     * Key into the EventStreamConfig API response in which stream configs are located.
     */
    protected static final String RESPONSE_STREAMS_KEY = "streams";

    /**
     * Constructs a MediawikiEventStreamConfigLoader that loads from mediawikiApiEndpoint using jsonLoader.
     */
    public MediawikiEventStreamConfigLoader(String mediawikiApiEndpoint, JsonLoader jsonLoader) {
        this.mediawikiApiEndpoint = mediawikiApiEndpoint;
        this.jsonLoader = jsonLoader;
    }

    /**
     * EventStreamConfigLoader load implementation.
     */
    @SuppressFBWarnings(value = "EXS_EXCEPTION_SOFTENING_NO_CHECKED", justification = "This method is public.")
    public ObjectNode load(List<String> streamNames) {
        URI uri = makeMediawikiEventStreamConfigApiUri(mediawikiApiEndpoint, streamNames);
        try {
            return (ObjectNode) jsonLoader.load(uri).get(RESPONSE_STREAMS_KEY);
        } catch (JsonLoadingException e) {
            throw new IllegalArgumentException("Failed to load stream configuration", e);
        }
    }

    /**
     * Builds a Mediawiki EventStreamConfig extension API URI for the given streams.
     *
     * If streamNames is empty, this will attempt to request all streams from the API.
     */
    public static URI makeMediawikiEventStreamConfigApiUri(String mediawikiApiEndpoint, List<String> streamNames) {
        final String mediawikiStreamsParamFormat = "streams=%s";
        final String mediawikiStreamsDelimiter;

        try {
            // We need to support streams param delimiters like "|", which is what
            // MW API expects, but URI.create will throw a
            // java.lang.IllegalArgumentException: Illegal character
            // if we don't URL encode "|" first.
            mediawikiStreamsDelimiter = URLEncoder.encode(
                "|", UTF_8.name()
            );
        } catch (java.io.UnsupportedEncodingException e) {
            // This should never happen.
            throw new RuntimeException(
                "Could not URL encode '|'. " + e.getMessage(), e
            );
        }

        String mediawikiEventStreamConfigUri = mediawikiApiEndpoint + "?format=json&action=streamconfigs&all_settings=true";

        // If no specified stream names, try to get them all.
        if (streamNames.isEmpty()) {
            return URI.create(mediawikiEventStreamConfigUri);
        } else {
            // else format the URI to request specific stream names.

            String streamsParam = String.format(
                    Locale.ROOT,
                    mediawikiStreamsParamFormat,
                    String.join(mediawikiStreamsDelimiter, streamNames)
            );

            return URI.create(
                mediawikiEventStreamConfigUri + streamsParam
            );
        }
    }

    public String toString() {
        return this.getClass().getName() + "(" + mediawikiApiEndpoint + ")";
    }

}
