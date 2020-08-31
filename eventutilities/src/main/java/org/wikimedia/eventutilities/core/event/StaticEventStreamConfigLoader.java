package org.wikimedia.eventutilities.core.event;

import java.net.URI;
import java.util.List;

import org.wikimedia.eventutilities.core.json.JsonLoader;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Loads stream config once from a static URI.
 */
public class StaticEventStreamConfigLoader  extends EventStreamConfigLoader {
    protected URI streamConfigUri;
    protected ObjectNode staticStreamConfigs;

    public StaticEventStreamConfigLoader(String streamConfigUri) {
        this(URI.create(streamConfigUri));
    }

    public StaticEventStreamConfigLoader(URI streamConfigUri) {
        this.streamConfigUri = streamConfigUri;
    }

    public ObjectNode load(List<String> streamNames) {
        if (staticStreamConfigs == null) {
            staticStreamConfigs = (ObjectNode) JsonLoader.get(streamConfigUri);
        }
        return staticStreamConfigs;
    }

    public String toString() {
        return this.getClass().getName() + "(" + streamConfigUri + ")";
    }
}
