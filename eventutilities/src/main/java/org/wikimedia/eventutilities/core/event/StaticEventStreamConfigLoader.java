package org.wikimedia.eventutilities.core.event;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.wikimedia.eventutilities.core.json.JsonLoader;

import java.net.URI;
import java.util.List;

/**
 * Loads stream config once from a static URI.
 */
public class StaticEventStreamConfigLoader  extends EventStreamConfigLoader {
    protected URI streamConfigUri;
    protected ObjectNode staticStreamConfigs = null;

    public StaticEventStreamConfigLoader(String streamConfigUri) {
        this.streamConfigUri = URI.create(streamConfigUri);
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
