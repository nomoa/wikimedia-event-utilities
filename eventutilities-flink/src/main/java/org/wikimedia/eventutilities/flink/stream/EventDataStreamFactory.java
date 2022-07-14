package org.wikimedia.eventutilities.flink.stream;

import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.wikimedia.eventutilities.core.event.EventStream;
import org.wikimedia.eventutilities.core.event.EventStreamFactory;
import org.wikimedia.eventutilities.flink.formats.json.JsonRowDeserializationSchema;
import org.wikimedia.eventutilities.flink.formats.json.JsonSchemaFlinkConverter;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Wraps EventStreamFactory with Flink DataStream API to provide
 * helper methods to instantiate DataStream of Row using
 * EventStream JSONSchemas and other metadata.
 *
 * Examples:
 *
 * Instantiate a EventDataStreamFactory from URIs:
 * <pre>{@code
 *     EventDataStreamFactory factory = EventDataStreamFactory.from(
 *          Arrays.asList(
 *              "https://schema.wikimedia.org/repositories/primary/jsonschema",
 *              "https://schema.wikimedia.org/repositories/secondary/jsonschema",
 *          ),
 *          "https://meta.wikimedia.org/w/api.php"
 *     );
 * }</pre>
 *
 * Get a {@link KafkaSource} for a declared event stream:
 * <pre>{@code
 *     KafkaSource<Row> eventStreamSource = factory.kafkaSourceBuilder(
 *          "test.event.example",  // EventStream name
 *          "localhost:9092",
 *          "my_consumer_group"
 *     ).build();
 * }</pre>
 *
 *
 */
public class EventDataStreamFactory {
    private final EventStreamFactory eventStreamFactory;

    public EventDataStreamFactory(
        EventStreamFactory eventStreamFactory
    ) {
        this.eventStreamFactory = eventStreamFactory;
    }

    /**
     * EventDataStreamFactory factory method.
     *
     * @param eventSchemaBaseUris
     *  URIs from which to fetch event JSONSchemas.
     *
     * @param eventStreamConfigUri
     *  URI from which to fetch event stream config.
     */
    public static EventDataStreamFactory from(
        @Nonnull List<String> eventSchemaBaseUris,
        @Nonnull String eventStreamConfigUri
    ) {
        return EventDataStreamFactory.from(
            eventSchemaBaseUris, eventStreamConfigUri, null
        );
    }

    /**
     * EventDataStreamFactory factory method.
     *
     * @param eventSchemaBaseUris
     *  URIs from which to fetch event JSONSchemas.
     *
     * @param eventStreamConfigUri
     *  URI from which to fetch event stream config.
     *
     * @param httpClientRoutes
     *  Map of source to dest http client routes.
     *  E.g. "https://meta.wikimedia.org" to "https://api-ro.wikimedia.org"
     *  If null, no special routing will be configured.
     */
    public static EventDataStreamFactory from(
        @Nonnull List<String> eventSchemaBaseUris,
        @Nonnull String eventStreamConfigUri,
        Map<String, String> httpClientRoutes
    ) {
        return new EventDataStreamFactory(
            EventStreamFactory.from(
                eventSchemaBaseUris, eventStreamConfigUri, httpClientRoutes
            )
        );
    }

    /**
     * Convenience method to get the EventStreamFactory used
     * by this EventDataStreamFactory.
     */
    public EventStreamFactory getEventStreamFactory() {
        return eventStreamFactory;
    }

    /**
     * Gets the {@link RowTypeInfo} (which is a TypeInformation of Row)
     * for the streamName.
     *
     * @param streamName
     *  Name of the EventStream, must be declared in EventStreamConfig.
     */
    public RowTypeInfo rowTypeInfo(String streamName) {
        EventStream eventStream = eventStreamFactory.createEventStream(streamName);
        ObjectNode jsonSchema = (ObjectNode)eventStream.schema();
        return JsonSchemaFlinkConverter.toRowTypeInfo(jsonSchema);
    }

    /**
     * Gets a JSON to Row DeserializationSchema by streamName.
     *
     * @param streamName
     *  Name of the EventStream, must be declared in EventStreamConfig.
     *
     */
    public JsonRowDeserializationSchema deserializer(String streamName) {
        EventStream eventStream = eventStreamFactory.createEventStream(streamName);
        ObjectNode jsonSchema = (ObjectNode)eventStream.schema();
        return JsonSchemaFlinkConverter.toDeserializationSchemaRow(jsonSchema);
    }

    /**
     * Get a {@link KafkaSourceBuilder} that is primed with settings needed to consume
     * the streamName from Kafka.
     *
     * This sets the following:
     * - bootstrapServers,
     * - topics,
     * - consumer group id
     * - value only deserializer that will deserialize to a Row conforming to streamName's JSONSchema
     * - starting offsets will use committed offsets, resetting to LATEST if no offests are committed.
     *
     * If you want to change any of these settings, then call the appropriate
     * method on the returned KafkaSourceBuilder before calling build().
     *
     * Example:
     *
     * <pre>{@code
     *     EventDataStreamFactory eventDataStreamFactory = EventDataStreamFactory.from(...)
     *     KafkaSource&lt;Row&gt; eventStreamSource = eventDataStreamFactory.kafkaSourceBuilder(
     *          "test.event.example",  // EventStream name
     *          "localhost:9092",
     *          "my_consumer_group"
     *     ).build();
     * }</pre>
     *
     * @param streamName
     *  Name of the EventStream, must be declared in EventStreamConfig.
     *
     * @param bootstrapServers
     *  Kafka bootstrap.servers property.
     *
     * @param consumerGroup
     *  Kafka consumer.group.id property.
     */
    public KafkaSourceBuilder<Row> kafkaSourceBuilder(
        String streamName,
        String bootstrapServers,
        String consumerGroup
    ) {
        EventStream eventStream = eventStreamFactory.createEventStream(streamName);

        KafkaSourceBuilder<Row> builder = KafkaSource.builder();
        builder
            .setBootstrapServers(bootstrapServers)
            .setGroupId(consumerGroup)
            .setTopics(eventStream.topics())
            .setValueOnlyDeserializer(deserializer(eventStream.streamName()))
            .setStartingOffsets(
                OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST)
            );

        return builder;
    }


}
