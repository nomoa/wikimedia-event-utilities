package org.wikimedia.eventutilities.core.event;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.wikimedia.eventutilities.core.json.JsonLoader;

import java.net.URI;
import java.net.URLEncoder;
import java.util.List;

/**
 * EventStreamConfigLoader implementation that loads stream config
 * from a remote Mediawiki EventStreamConfig extension API endpoint.
 */
public class MediawikiEventStreamConfigLoader extends EventStreamConfigLoader {

    /**
     * Base Mediawiki API endpoint, e.g https://meta.wikimedia.org/w/api.php
     */
    protected String mediawikiApiEndpoint;

    /**
     * Constructs a MediawikiEventStreamConfigLoader that loads from mediawikiApiEndpoint.
     * @param mediawikiApiEndpoint
     */
    public MediawikiEventStreamConfigLoader(String mediawikiApiEndpoint) {
        this.mediawikiApiEndpoint = mediawikiApiEndpoint;
    }

    /**
     * EventStreamConfigLoader load implementation.
     * @param streamNames
     * @return
     */
    public ObjectNode load(List<String> streamNames) {
        URI uri = makeMediawikiEventStreamConfigApiUri(mediawikiApiEndpoint, streamNames);
        return (ObjectNode) JsonLoader.get(uri);
    }

    /**
     * Builds a Mediawiki EventStreamConfig extension API URI for the given streams.
     * If streamNames is empty, this will attempt to request all streams from the API.
     *
     * @param mediawikiApiEndpoint
     * @param streamNames
     * @return
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
                "|", "UTF-8"
            );
        } catch (java.io.UnsupportedEncodingException e) {
            // This should never happen.
            throw new RuntimeException(
                "Could not URL encode '|'. " + e.getMessage()
            );
        }

        String mediawikiEventStreamConfigUri = mediawikiApiEndpoint + "?format=json&action=streamconfigs&all_settings=true";

        // If no specified stream names, try to get them all.
        if (streamNames.isEmpty()) {
            return URI.create(mediawikiEventStreamConfigUri);
        } else {
            // else format the URI to request specific stream names.
            String streamsParam = String.format(
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
