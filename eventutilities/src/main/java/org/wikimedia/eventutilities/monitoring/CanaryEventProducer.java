package org.wikimedia.eventutilities.monitoring;


import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.http.entity.ContentType;
import org.wikimedia.eventutilities.core.event.EventSchemaLoader;
import org.wikimedia.eventutilities.core.event.EventStream;
import org.wikimedia.eventutilities.core.event.EventStreamConfig;
import org.wikimedia.eventutilities.core.event.EventStreamFactory;
import org.wikimedia.eventutilities.core.http.BasicHttpClient;
import org.wikimedia.eventutilities.core.http.HttpResult;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

/**
 * Uses an EventStreamFactory to create and POST Wikimedia canary events to
 * Wikimedia event services.  Canary events are constructed from the first entry in the
 * event schema's examples field.  It is expected that event schemas have the fields listed
 * at https://wikitech.wikimedia.org/wiki/Event_Platform/Schemas/Guidelines#Required_fields, and
 * also that the receiving event intake service (e.g. EventGate) will set meta.dt if it is
 * not present in the event.
 */
public class CanaryEventProducer {

    /**
     * EventStreamFactory instance used when constructing EventStreams.
     */
    protected final EventStreamFactory eventStreamFactory;

    /**
     * List of data center names that will be used to look up event service urls.
     */
    protected static final List<String> DATACENTERS = ImmutableList.of("eqiad", "codfw");

    /**
     * Used for serializing JsonNode events to Strings.
     */
    protected static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Will be used as the value of meta.domain when building canary events.
     * It does not matter what this is, but it should be consistent.
     */
    protected static final String CANARY_DOMAIN = "canary";

    private final BasicHttpClient httpClient;

    /**
     * Constructs a new instance of CanaryEventProducer with a new instance of EventStreamFactory
     * from eventSchemaLoader and eventStreamConfig.
     */
    public CanaryEventProducer(
        EventSchemaLoader eventSchemaLoader,
        EventStreamConfig eventStreamConfig,
        BasicHttpClient httpClient
    ) {
        this(
            EventStreamFactory.builder()
                .setEventSchemaLoader(eventSchemaLoader)
                .setEventStreamConfig(eventStreamConfig)
                .build(),
            httpClient
        );
    }

    /**
     * Constructs a new CanaryEventProducer using the provided EventStreamFactory.
     */
    public CanaryEventProducer(EventStreamFactory eventStreamFactory, BasicHttpClient client) {
        this.eventStreamFactory = eventStreamFactory;
        this.httpClient = client;
    }

    /**
     * Returns the EventStreamFactory this CanaryEventProducer is using.
     */
    public EventStreamFactory getEventStreamFactory() {
        return eventStreamFactory;
    }


    /**
     * Given a streamName, gets its schema and uses the JSONSchema examples to make a canary event.
     */
    public ObjectNode canaryEvent(String streamName) {
        return canaryEvent(eventStreamFactory.createEventStream(streamName));
    }

    /**
     * Given an EventStream, gets its schema and uses the JSONSchema examples to make a canary event.
     */
    public ObjectNode canaryEvent(EventStream es) {
        return makeCanaryEvent(
            es.streamName(),
            es.exampleEvent()
        );
    }

    /**
     * Creates a canary event from an example event for a stream.
     */
    protected static ObjectNode makeCanaryEvent(String streamName, ObjectNode example) {
        if (example == null) {
            throw new NullPointerException(
                "Cannot make canary event for " + streamName + ", example is null."
            );
        }

        ObjectNode canaryEvent = example.deepCopy();
        ObjectNode canaryMeta = (ObjectNode)canaryEvent.get("meta");
        canaryMeta.set("domain", JsonNodeFactory.instance.textNode(CANARY_DOMAIN));
        canaryMeta.set("stream", JsonNodeFactory.instance.textNode(streamName));
        // Remove meta.dt so it is set by the Event Service we will POST this event to.
        canaryMeta.remove("dt");
        canaryEvent.set("meta", canaryMeta);
        return canaryEvent;
    }

    /**
     * Gets canary events to POST for all streams that EventStreamConfig knows about.
     * Refer to docs for getCanaryEventsToPostForStreams(eventStreams).
     */
    public Map<URI, List<ObjectNode>> getAllCanaryEventsToPost() {
        return getCanaryEventsToPost(
            eventStreamFactory.getEventStreamConfig().cachedStreamNames()
        );
    }

    /**
     * Gets canary events to POST for a single stream.
     * Refer to docs for getCanaryEventsToPostForStreams(eventStreams).
     */
    public Map<URI, List<ObjectNode>> getCanaryEventsToPost(String streamName) {
        return getCanaryEventsToPost(Collections.singletonList(streamName));
    }

    /**
     * Gets canary events to POST for a List of stream names.
     * Refer to docs for getCanaryEventsToPostForStreams(eventStreams).
     */
    public Map<URI, List<ObjectNode>> getCanaryEventsToPost(List<String> streamNames) {
        return getCanaryEventsToPostForStreams(eventStreamFactory.createEventStreams(streamNames));
    }

    /**
     * Given a list of streams, this will return a map of
     * datacenter specific event service URIs to a list of canary
     * events that should be POSTed to that event service.
     * These can then be iterated through and posted to each
     * event service URI to post expected canary events for each stream.
     */
    public Map<URI, List<ObjectNode>> getCanaryEventsToPostForStreams(
        List<EventStream> eventStreams
    ) {
        // Build a map of datacenter specific event service url to EventStreams
        Map<URI, List<EventStream>> eventStreamsByEventServiceUrl = new HashMap<>();
        for (String datacenter : DATACENTERS) {
            eventStreamsByEventServiceUrl.putAll(
                eventStreams.stream().collect(Collectors.groupingBy(
                    eventStream -> eventStream.eventServiceUri(datacenter)
                ))
            );
        }

        // Convert the Map of URIs -> EventStreams to URIs -> canary events.
        // Each set of canary events can be POSTed to their keyed
        // event service url.
        return eventStreamsByEventServiceUrl.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().stream()
                    .map(this::canaryEvent)
                    .collect(Collectors.toList())
            ));
    }



    /**
     * POSTs canary events for all known streams.
     *
     * Refer to docs for postCanaryEVents(streamNames).
     * Refer to docs for postCanaryEventsForStreams(eventStreams).
     */
    public Map<URI, HttpResult> postAllCanaryEvents() {
        return postCanaryEvents(
            eventStreamFactory.getEventStreamConfig().cachedStreamNames()
        );
    }

    /**
     * Posts canary events for a single streamName.
     * Refer to docs for postCanaryEventsForStreams(eventStreams).
     */
    public Map<URI, HttpResult> postCanaryEvents(String streamName) {
        return postCanaryEvents(Collections.singletonList(streamName));
    }

    /**
     * Posts canary events for each named event stream.
     * Refer to docs for postCanaryEventsForStreams(eventStreams).
     */
    public Map<URI, HttpResult> postCanaryEvents(List<String> streamNames) {
        return postCanaryEventsForStreams(eventStreamFactory.createEventStreams(streamNames));
    }

    /**
     * Gets canary events for each eventStream, POSTs them to the appropriate
     * event service url(s), and collects the results of each POST
     * into a Map of event service url to result ObjectNode.
     *
     * We want to attempt every POST we are supposed to do without bailing
     * when an error is encountered.  This is why the results are collected in
     * this way  The results should be examined after this method returns
     * to check for any failures.
     */
    public Map<URI, HttpResult> postCanaryEventsForStreams(List<EventStream> eventStreams) {
        return postEventsToUris(getCanaryEventsToPostForStreams(eventStreams));
    }

    /**
     * Given a List of ObjectNodes, returns an ArrayNode of those ObjectNodes.
     */
    public static ArrayNode eventsToArrayNode(List<ObjectNode> events) {
        ArrayNode eventsArray = JsonNodeFactory.instance.arrayNode();
        for (ObjectNode event : events) {
            eventsArray.add(event);
        }
        return eventsArray;
    }

    /**
     * Iterates over the Map of URI to events and posts events to the URI.
     */
    public Map<URI, HttpResult> postEventsToUris(Map<URI, List<ObjectNode>> uriToEvents) {
        return uriToEvents.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> postEvents(entry.getKey(), entry.getValue())
            ));
    }

    /**
     * POSTs the given list of
     * events to the eventServiceUri.
     * Expects that eventServiceUri returns a JSON response.
     * EventGate returns 201 if guaranteed success, 202 if hasty success,
     * and 207 if partial success (some events were accepted, others were not).
     * We want to only consider 201 and 202 as full success so we pass
     * httpPostJson a custom isSuccess function to determine this.
     * https://github.com/wikimedia/eventgate/blob/master/spec.yaml#L72
     *
     * The returned HttpResult will look like:
     *
     *     success: true,
     *     status 201,
     *     message: "HTTP response message",
     *     body: response body if any
     * }
     * If ANY events failed POSTing, success will be false, and the reasons
     * for the failure will be in message and body.
     * If there is a local exception during POSTing, success will be false
     * and the Exception message will be in message, and in the exception field
     * will have the original Exception.
     */
    public HttpResult postEvents(URI eventServiceUri, List<ObjectNode> events) {
        // Convert List of events to ArrayNode of events to allow
        // jackson to serialize them as an array of events.
        ArrayNode eventsArray = eventsToArrayNode(events);

        try {
            return httpClient.post(
                eventServiceUri,
                objectMapper.writeValueAsString(eventsArray).getBytes(StandardCharsets.UTF_8),
                ContentType.APPLICATION_JSON,
                // Only consider 201 and 202 from EventGate as fully successful POSTs.
                statusCode -> statusCode == 201 || statusCode == 202
            );
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(
                "Encountered JsonProcessingException when attempting to POST canary events to " +
                    eventServiceUri + ". " + e.getMessage(), e
            );
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }
}
