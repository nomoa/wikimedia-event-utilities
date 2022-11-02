package org.wikimedia.eventutilities.flink.stream;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.wikimedia.eventutilities.core.event.EventStream;
import org.wikimedia.eventutilities.core.event.EventStreamFactory;
import org.wikimedia.eventutilities.core.event.JsonEventGenerator;
import org.wikimedia.eventutilities.flink.EventRowTypeInfo;
import org.wikimedia.eventutilities.flink.formats.json.JsonRowDeserializationSchema;
import org.wikimedia.eventutilities.flink.formats.json.JsonRowSerializationSchema;
import org.wikimedia.eventutilities.flink.formats.json.JsonSchemaFlinkConverter;
import org.wikimedia.eventutilities.flink.formats.json.KafkaEventSerializationSchema;
import org.wikimedia.eventutilities.flink.formats.json.KafkaRecordTimestampStrategy;

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
    private final JsonEventGenerator eventGenerator;

    public EventDataStreamFactory(
        EventStreamFactory eventStreamFactory,
        JsonEventGenerator eventGenerator
    ) {
        this.eventStreamFactory = eventStreamFactory;
        this.eventGenerator = eventGenerator;
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
        EventStreamFactory eventStreamFactory = EventStreamFactory.from(
                eventSchemaBaseUris, eventStreamConfigUri, httpClientRoutes
        );

        JsonEventGenerator generator = JsonEventGenerator.builder()
                .eventStreamConfig(eventStreamFactory.getEventStreamConfig())
                .schemaLoader(eventStreamFactory.getEventSchemaLoader())
                .build();

        return new EventDataStreamFactory(
                eventStreamFactory,
                generator
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
     * Gets the {@link EventRowTypeInfo} (which is a TypeInformation of Row)
     * for the streamName.
     * The corresponding schema is the latest obtained from the EventStream configuration
     * identified by streamName.
     *
     * @param streamName
     *  Name of the EventStream, must be declared in EventStreamConfig.
     */
    public EventRowTypeInfo rowTypeInfo(String streamName) {
        EventStream eventStream = eventStreamFactory.createEventStream(streamName);
        ObjectNode jsonSchema = (ObjectNode)eventStream.schema();
        return JsonSchemaFlinkConverter.toRowTypeInfo(jsonSchema);
    }

    /**
     * Gets the {@link EventRowTypeInfo} (which is a TypeInformation of Row)
     * for the streamName and a particular schema version.
     *
     * @param streamName
     *  Name of the EventStream, must be declared in EventStreamConfig.
     * @param version
     *  Version of the schema to use
     */
    public EventRowTypeInfo rowTypeInfo(String streamName, String version) {
        EventStream eventStream = eventStreamFactory.createEventStream(streamName);
        ObjectNode jsonSchema = (ObjectNode)eventStream.schema(version);
        return JsonSchemaFlinkConverter.toRowTypeInfo(jsonSchema);
    }

    /**
     * Gets a JSON to Row DeserializationSchema by streamName and its latest
     * schema version.
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
     * Gets a JSON to Row DeserializationSchema by streamName.
     *
     * @param streamName
     *  Name of the EventStream, must be declared in EventStreamConfig.
     *
     * @param version
     *  Version of the schema to use.
     */
    public JsonRowDeserializationSchema deserializer(String streamName, String version) {
        EventStream eventStream = eventStreamFactory.createEventStream(streamName);
        ObjectNode jsonSchema = (ObjectNode)eventStream.schema(version);
        return JsonSchemaFlinkConverter.toDeserializationSchemaRow(jsonSchema);
    }

    /**
     * Create a {@link org.apache.flink.api.common.serialization.SerializationSchema} suited for
     * producing json to the stream identified by streamName using the provided schema version.
     */
    public JsonRowSerializationSchema serializer(String streamName, String version) {
        EventStream stream = eventStreamFactory.createEventStream(streamName);
        JsonEventGenerator.EventNormalizer generator = eventGenerator
                .createEventStreamEventGenerator(streamName, stream.schemaUri(version).toString());

        TypeInformation<Row> typeInformation = rowTypeInfo(streamName, version);
        return JsonRowSerializationSchema.builder()
                .withNormalizationFunction(generator)
                .withObjectMapper(generator.getObjectMapper())
                .withTypeInfo(typeInformation)
                .build();
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

    /**
     * Prepare a KafkaSinkBuilder with all the required components to produce to kafka using a json format
     * matching the provided schema.
     *
     * Produces events to the default kafka partition.
     *
     * @see #kafkaSinkBuilder(String, String, String, String, KafkaRecordTimestampStrategy, FlinkKafkaPartitioner)
     */
    public KafkaSinkBuilder<Row> kafkaSinkBuilder(
            String streamName,
            String schemaVersion,
            String bootstrapServers,
            String topic,
            KafkaRecordTimestampStrategy timestampStrategy
    ) {
        return kafkaSinkBuilder(streamName, schemaVersion, bootstrapServers, topic, timestampStrategy, null);
    }

    /**
     * Prepare a KafkaSinkBuilder with all the required components to produce to kafka using a json format
     * matching the provided schema.
     *
     * The produced messages match the WMF Event Platform Rules:
     * - the meta.dt field is filled and will be used as the kafka timestamp
     * - the topic to produce to is one of the topics defined in the EventStream configuration
     * - the provided schema must match the one defined in the EventStream configuration
     * - the resulting json is validated against this schema
     * - the dt (event time) field remains optional and must be set beforehand in the pipeline
     *
     * @param streamName
     *  Name of the EventStream, must be declared in EventStreamConfig.
     *
     * @param schemaVersion
     *  Version of the schema to use.
     *
     * @param bootstrapServers
     *  Kafka bootstrap.servers property.
     *
     * @param topic
     *  The topic to write to, must be part of the topics referenced by the EventStreamConfig.
     *
     * @param timestampStrategy
     *  The strategy to use regarding producer records timestamps and event time
     *
     * @param partitioner
     *  An optional partioner
     *
     * @see KafkaRecordTimestampStrategy
     */
    public KafkaSinkBuilder<Row> kafkaSinkBuilder(
            String streamName,
            String schemaVersion,
            String bootstrapServers,
            String topic,
            KafkaRecordTimestampStrategy timestampStrategy,
            @Nullable FlinkKafkaPartitioner<Row> partitioner
    ) {
        EventStream stream = eventStreamFactory.createEventStream(streamName);
        if (!stream.topics().contains(topic)) {
            throw new IllegalArgumentException("The topic [" + topic + "] is now allowed for the stream [" + streamName + "], " +
                    "only [" + String.join(",", stream.topics()) + "] are allowed.");
        }
        JsonRowSerializationSchema serializationSchema = serializer(streamName, schemaVersion);
        KafkaRecordSerializationSchema<Row> kafkaRecordSerializationSchema = new KafkaEventSerializationSchema(
                serializationSchema.getTypeInformation(),
                serializationSchema,
                Instant::now,
                topic,
                timestampStrategy,
                partitioner
        );
        return KafkaSink.<Row>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(kafkaRecordSerializationSchema);
    }

}
