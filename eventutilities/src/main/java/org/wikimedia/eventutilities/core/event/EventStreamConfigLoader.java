package org.wikimedia.eventutilities.core.event;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Collections;
import java.util.List;

/**
 * Abstract class to load event stream config.
 * Subclasses must implement the load(streamNames) method.
 * An instance of an implementing class is injected into EventStreamConfig
 * and used to load stream config on demand.
 */
public abstract class EventStreamConfigLoader {
    /**
     * Loads stream configs for the given stream names.
     * @param streamNames
     * @return
     */
    public abstract ObjectNode load(List<String> streamNames);

    /**
     * Loads stream configs for the given stream name.
     * @param streamName
     * @return
     */
    public ObjectNode load(String streamName) {
        return load(Collections.singletonList(streamName));
    }

    /**
     * Loads stream configs for all streams.
     * @return
     */
    public ObjectNode load() {
        return load(Collections.emptyList());
    }

    public String toString() {
        return this.getClass().getName();
    }

}
