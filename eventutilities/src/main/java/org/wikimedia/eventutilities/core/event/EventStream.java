package org.wikimedia.eventutilities.core.event;

import java.net.URI;
import java.util.List;

import org.wikimedia.eventutilities.core.json.JsonLoadingException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Represents a single event stream.  An event stream is a named continuous stream
 * of events.  An event is a well typed single datum with a specific timestamp.
 *
 * An event stream in Kafka is composed of 1 or more topics.
 *
 * This class uses Wikimedia specific event stream configuration and schema repositories
 * to abstract looking up stream configuration, schemas, topics and canary events
 * given a stream name.
 */
public class EventStream {

    protected String streamName;

    /**
     * EventSchemaLoader instance used when getting a schema for this stream.
     */
    protected EventSchemaLoader eventSchemaLoader;

    /**
     * EventStreamConfig instance used to lookup stream config settings for this stream.
     */
    protected EventStreamConfig eventStreamConfig;

    public EventStream(
        String streamName,
        EventSchemaLoader eventSchemaLoader,
        EventStreamConfig eventStreamConfig
    ) {
        this.streamName = streamName;
        this.eventSchemaLoader = eventSchemaLoader;
        this.eventStreamConfig = eventStreamConfig;
    }

    /**
     * Gets the stream name of this EventStream instance.
     */
    public String streamName() {
        return streamName;
    }

    /**
     * Gets a setting from stream config for this stream.
     */
    public JsonNode getSetting(String settingName) {
        return eventStreamConfig.getSetting(streamName, settingName);
    }

    /**
     * Gets the list of Kafka topics that compose this stream.
     */
    public List<String> topics() {
        return eventStreamConfig.getTopics(streamName);
    }

    /**
     * Gets this EventStream's destination_event_service name.
     *
     * This is an EventStreamConfig setting that should map
     * to the event service URL where events of this stream
     * can be POSTed.
     */
    public String eventServiceName() {
        return eventStreamConfig.getEventServiceName(streamName);
    }

    /**
     * Returns the (discovery) URL to which events belonging to this EventStream should be POSTed.
     *
     * E.g. https://eventgate-main.discovery.wmnet/v1/events
     */
    public URI eventServiceUri() {
        return eventStreamConfig.getEventServiceUri(streamName);
    }

    /**
     * Returns the datacenter specific event service URL to which events belonging to
     * this EventStream should be POSTed.
     *
     * E.g. https://eventgate-main.svc.eqiad.wmnet/v1/events
     * This assumes that EventStreamConfig is configured locally here with a
     * name to URI map of <event_service_name>-<datacenter>.  If it isn't, this will return null.
     */
    public URI eventServiceUri(String datacenter) {
        return eventStreamConfig.getEventServiceUri(
            streamName, datacenter
        );
    }

    /**
     * Builds a latest relative schema URI for stream based on WMF conventions.
     *
     * This expects that the stream's schema_title will easily map to
     * a schema URI namespace hierarchy in a schema repository.  E.g.
     *   schema_title: my/cool/schema -> /my/cool/schema/latest
     */
    public URI schemaUri() {
        String schemaTitle = eventStreamConfig.getSchemaTitle(streamName);
        // The final part of this URI (here latest) is the schema version.
        // It doesn't actually matter what version we put, since we'll be calling
        // EventSchemaLoader getLatestSchemaUri, and the version will be replaced anyway.
        URI schemaUri = URI.create("/" + schemaTitle + "/latest");
        return eventSchemaLoader.getLatestSchemaUri(schemaUri);
    }

    /**
     * Infers the latest relative schemaUri for the stream from its schema_title
     * stream config setting and fetches and returns the schema at that URI using eventSchemaLoader.
     */
    public JsonNode schema() {
        return loadSchema(schemaUri());
    }

    /**
     * Loads the schema at the relative schemaUri using our eventSchemaLoader.
     */
    protected JsonNode loadSchema(URI schemaUri) {
        try {
            return eventSchemaLoader.getSchema(schemaUri);
        } catch (JsonLoadingException e) {
            throw new RuntimeException(
                "Failed loading schema at " + schemaUri + ". " + e.getMessage()
            );
        }
    }

    /**
     * Gets the schema for stream and returns the first element in its JSONSchema examples.
     *
     * If the schema does not have any examples, returns null.
     */
    public ObjectNode exampleEvent() {
        JsonNode schema = schema();
        JsonNode examples = schema.get("examples");
        if (examples == null) {
            return null;
        } else {
            return (ObjectNode)examples.get(0);
        }
    }

}
