package org.wikimedia.eventutilities.core.event;

import java.util.List;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

/**
 * Abstract class to load event stream config.
 * Subclasses must implement the load(streamNames) method.
 * An instance of an implementing class is injected into EventStreamConfig
 * and used to load stream config on demand.
 */
public abstract class EventStreamConfigLoader {
    /**
     * Loads stream configs for the given stream names.
     */
    public abstract ObjectNode load(List<String> streamNames);

    /**
     * Loads stream configs for the given stream name.
     */
    public ObjectNode load(String streamName) {
        return load(ImmutableList.of(streamName));
    }

    /**
     * Loads stream configs for all streams.
     */
    public ObjectNode load() {
        return load(ImmutableList.of());
    }

    public String toString() {
        return this.getClass().getName();
    }

}
