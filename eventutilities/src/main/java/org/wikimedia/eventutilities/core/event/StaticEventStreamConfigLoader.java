package org.wikimedia.eventutilities.core.event;

import java.net.URI;
import java.util.List;

import org.wikimedia.eventutilities.core.json.JsonLoader;
import org.wikimedia.eventutilities.core.json.JsonLoadingException;

import com.fasterxml.jackson.databind.node.ObjectNode;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Loads stream config once from a static URI.
 */
public class StaticEventStreamConfigLoader  extends EventStreamConfigLoader {
    protected URI streamConfigUri;
    protected ObjectNode staticStreamConfigs;
    private final JsonLoader jsonLoader;

    /**
     * Constructs a StaticEventStreamConfigLoader that loads from streamConfigUri using jsonLoader.
     */
    public StaticEventStreamConfigLoader(URI streamConfigUri, JsonLoader jsonLoader) {
        this.streamConfigUri = streamConfigUri;
        this.jsonLoader = jsonLoader;
    }

    @SuppressFBWarnings(value = "EXS_EXCEPTION_SOFTENING_NO_CHECKED", justification = "This method is public.")
    public ObjectNode load(List<String> streamNames) {
        if (staticStreamConfigs == null) {
            try {
                staticStreamConfigs = (ObjectNode) jsonLoader.load(streamConfigUri);
            } catch (JsonLoadingException e) {
                throw new IllegalArgumentException("Cannot fetch/parse streamConfig at " + streamConfigUri, e);
            }
        }
        return staticStreamConfigs;
    }

    public String toString() {
        return this.getClass().getName() + "(" + streamConfigUri + ")";
    }
}
