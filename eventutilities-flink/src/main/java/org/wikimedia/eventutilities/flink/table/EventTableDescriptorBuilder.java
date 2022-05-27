package org.wikimedia.eventutilities.flink.table;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.wikimedia.eventutilities.core.event.EventStream;
import org.wikimedia.eventutilities.core.event.EventStreamFactory;
import org.wikimedia.eventutilities.flink.formats.json.JsonSchemaConverter;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;


/**
 * Builder wrapper to aid in constructing Flink TableDescriptors
 * using Wikimedia Event Streams. TableDescriptors are passed
 * to the Flink TableEnvironment when instantiating Table objects
 * or when creating tables in a catalog.
 *
 * This class uses EventStreamFactory to get information
 * about Event Streams declared in EventStreamConfig, e.g.
 * schema and topics.
 *
 * You must at minimum call eventStream() and connector() before
 * calling build().
 *
 * It is possible to pass in your own Flink Schema.Builder
 * while using this wrapper. If you need to augment the schema, for example
 * to add computed / virtual fields, calling getSchemaBuilder()
 * will get you the Schema.Builder with the stream's schema set as
 * the physical DataType. After modifying the Schema.Builder as desired,
 * call schemaBuilder(modifiedSchemaBuilder) again to use it for the
 * TableDescriptor that will be built.
 *
 * Examples:
 *
 * Instantiate a EventTableDescriptorBuilder from URIs:
 * <code>
 *     EventTableDescriptorBuilder builder = EventTableDescriptorBuilder.from(
 *          Arrays.asList(
 *              "https://schema.wikimedia.org/repositories/primary/jsonschema",
 *              "https://schema.wikimedia.org/repositories/secondary/jsonschema",
 *          ),
 *          "https://meta.wikimedia.org/w/api.php"
 *     );
 * </code>
 *
 * Build a DataGen Table:
 * <code>
 *     Table t = tableEnv.from(
 *          builder
 *              .connector("datagen")
 *              .eventStream("mediawiki.page-create")
 *              .option("number-of-rows", "10")
 *              .build()
*           )
 *     );
 * </code>
 *
 * Build a Kafka Table:
 * <code>
 *     Table t = tableEnv.from(
 *          builder
 *              .eventStream("mediawiki.page-create")
 *              .setupKafka(
 *                  "localhost:9092",
 *                  "my_consumer_group",
 *                  true  // If should auto-add a kafka_timestamp watermark virtual field.
 *               )
 *              .build()
 *           )
 *     );
 * </code>
 *
 */
public class EventTableDescriptorBuilder {
    private final EventStreamFactory eventStreamFactory;

    private EventStream eventStream;

    private String connectorIdentifier;
    private String formatIdentifier;
    private String[] partitionKeys;
    private Schema.Builder _schemaBuilder;
    private Map<String, String> options;

    public EventTableDescriptorBuilder(EventStreamFactory eventStreamFactory) {
        this.eventStreamFactory = eventStreamFactory;
        this.clear();
    }

    /**
     * EventTableDescriptorBuilder factory method.
     *
     * @param eventSchemaBaseUris
     *  URIs from which to fetch event JSONSchemas.
     *
     * @param eventStreamConfigUri
     *  URI from which to fetch event stream config.
     */
    public static EventTableDescriptorBuilder from(
        @Nonnull List<String> eventSchemaBaseUris,
        @Nonnull String eventStreamConfigUri
    ) {
        return EventTableDescriptorBuilder.from(
            eventSchemaBaseUris, eventStreamConfigUri, null
        );
    }

    /**
     * EventTableDescriptorBuilder factory method.
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
    public static EventTableDescriptorBuilder from(
        @Nonnull List<String> eventSchemaBaseUris,
        @Nonnull String eventStreamConfigUri,
        Map<String, String> httpClientRoutes
    ) {
        return new EventTableDescriptorBuilder(
            EventStreamFactory.from(
                eventSchemaBaseUris, eventStreamConfigUri, httpClientRoutes
            )
        );
    }

    /**
     * Convenience method to get the EventStreamFactory used
     * by this EventTableDescriptorBuilder.
     */
    public EventStreamFactory getEventStreamFactory() {
        return eventStreamFactory;
    }

    /**
     * Sets the connector.
     *
     * @param connectorIdentifier
     *  A valid and registered Flink Table Connector Factory Identifier string.
     *  e.g. "kafka" or "filesystem".
     */
    public EventTableDescriptorBuilder connector(String connectorIdentifier) {
        this.connectorIdentifier = connectorIdentifier;
        return this;
    }

    /**
     * Sets the EventStream that will be used to build the TableDescriptor.
     */
    public EventTableDescriptorBuilder eventStream(EventStream eventStream) {
        this.eventStream = eventStream;
        return this;
    }

    /**
     * Sets the EventStream by streamName that will be used to build the TableDescriptor.
     *
     * @param streamName
     *  Name of the EventStream, must be declared in EventStreamConfig.
     */
    public EventTableDescriptorBuilder eventStream(String streamName) {
        return eventStream(eventStreamFactory.createEventStream(streamName));
    }

    /**
     * Sets the Schema.Builder. If you don't set this the Table Schema will be
     * converted from the eventStream's JSONSchema.
     */
    public EventTableDescriptorBuilder schemaBuilder(Schema.Builder schemaBuilder) {
        this._schemaBuilder = schemaBuilder;
        return this;
    }

    /**
     * Sets the TableDescriptor.Builder options.
     * This clears out any previously set options.
     */
    public EventTableDescriptorBuilder options(@Nonnull Map<String, String> options) {
        this.options = options;
        return this;
    }

    /**
     * Sets a single TableDescriptor.Builder option.
     */
    public EventTableDescriptorBuilder option(String key, String value) {
        this.options.put(key, value);
        return this;
    }

    /**
     * Sets the format for the TableDescriptor.Builder.
     * This needed by most, but not all, connectors.
     */
    public EventTableDescriptorBuilder format(String format) {
        this.formatIdentifier = format;
        return this;
    }

    /**
     * Sets the partitionKeys for the TableDescriptor.Builder.
     */
    public EventTableDescriptorBuilder partitionedBy(String... partitionKeys) {
        this.partitionKeys = partitionKeys;
        return this;
    }

    /**
     * Helper to aid in setting up a kafka connector for the EventStream.
     * You must first call eventStream() before calling this method so that
     * it can fill in the details for the connector from the EventStream.
     *
     * @param bootstrapServers
     *  Kafka bootstrap.servers property.
     *
     * @param consumerGroup
     *  Kafka consumer.group.id property.
     *
     * @param withKafkaTimestampAsWatermark
     *  If true, a virtual "kafka_timestamp" field will be added to the
     *  Schema and used as the watermark.
     */
    public EventTableDescriptorBuilder setupKafka(
        String bootstrapServers,
        String consumerGroup,
        Boolean withKafkaTimestampAsWatermark
    ) {
        Preconditions.checkState(
            this.eventStream != null,
            "Must call eventStream() before calling setupKafka()."
        );

        connector("kafka");
        // EventStreams in Kafka are JSON.
        format("json");

        if (withKafkaTimestampAsWatermark) {
            Schema.Builder sb = getSchemaBuilder();
            sb.columnByMetadata(
                    "kafka_timestamp",
                    "TIMESTAMP_LTZ(3) NOT NULL",
                    "timestamp",
                    true
                )
                .watermark(
                    "kafka_timestamp",
                    "kafka_timestamp"
                );
            schemaBuilder(sb);
        }

        option("properties.bootstrap.servers", bootstrapServers);
        option("topic", String.join(";", eventStream.topics()));
        option("properties.group.id", consumerGroup);

        return this;
    }

    /**
     * Use this to get the schemaBuilder for the eventStream
     * in order to augment and reset it before calling build().
     * If you change the Schema.Builder, make sure you call
     * schemaBuilder(modifiedSchemaBuilder) to apply your changes
     * before calling build().
     *
     * @return
     *  The Schema.Builder that will be used to build the Table Schema.
     */
    public Schema.Builder getSchemaBuilder() {
        Preconditions.checkState(
            !(this.eventStream == null && this._schemaBuilder == null),
            "Must call eventStream() or schemaBuilder() before calling getSchemaBuilder()."
        );

        if (this._schemaBuilder == null) {
            this._schemaBuilder = JsonSchemaConverter.toSchemaBuilder(
                (ObjectNode)eventStream.schema()
            );
        }

        return this._schemaBuilder;
    }

    /**
     * Applies all the collected configs and options to
     * build a TableDescriptor using information from the EventStream.
     * You can use the returned TableDescriptor to instantiate a Table object
     * or create a Table in a catalog.
     *
     * Any collected state will be cleared before returning, so you
     * can re-use this EventTableDescriptorBuilder again.
     */
    public TableDescriptor build() {
        Preconditions.checkState(
            connectorIdentifier != null,
            "Must call connector() before calling build()."
        );
        Preconditions.checkState(
            eventStream != null,
            "Must call eventStream() before calling build()."
        );

        TableDescriptor.Builder tdb = TableDescriptor.forConnector(connectorIdentifier)
            .schema(getSchemaBuilder().build())
            .comment(eventStream.toString());

        // Not all connectors use format.
        if (formatIdentifier != null) {
            tdb.format(formatIdentifier);
        }

        if (partitionKeys != null) {
            tdb.partitionedBy(partitionKeys);
        }

        options.forEach(tdb::option);

        TableDescriptor td = tdb.build();
        this.clear();
        return td;
    }

    /**
     * Clears all collected configs and options from this EventTableDescriptorBuilder
     * so that it can be used to build another TableDescriptor.
     */
    public final EventTableDescriptorBuilder clear() {
        this.options = new HashMap<>();
        this.eventStream = null;
        this.connectorIdentifier = null;
        this.formatIdentifier = null;
        this.partitionKeys = null;
        this._schemaBuilder = null;

        return this;
    }

}
