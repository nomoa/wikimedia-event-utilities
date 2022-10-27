package org.wikimedia.eventutilities.flink.table;

import static java.util.Collections.emptyMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.wikimedia.eventutilities.core.event.EventStream;
import org.wikimedia.eventutilities.core.event.EventStreamFactory;
import org.wikimedia.eventutilities.flink.formats.json.JsonSchemaFlinkConverter;

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
 * <pre>{@code
 *     EventTableDescriptorBuilder builder = EventTableDescriptorBuilder.from(
 *          Arrays.asList(
 *              "https://schema.wikimedia.org/repositories/primary/jsonschema",
 *              "https://schema.wikimedia.org/repositories/secondary/jsonschema",
 *          ),
 *          "https://meta.wikimedia.org/w/api.php"
 *     );
 * }</pre>
 *
 * Build a DataGen Table:
 * <pre>{@code
 *     Table t = tableEnv.from(
 *          builder
 *              .connector("datagen")
 *              .eventStream("mediawiki.page-create")
 *              .option("number-of-rows", "10")
 *              .build()
*           )
 *     );
 * }</pre>
 *
 * Build a Kafka Table:
 * <pre>{@code
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
 * }</pre>
 *
 */
@ParametersAreNonnullByDefault
public class EventTableDescriptorBuilder {
    private final EventStreamFactory eventStreamFactory;

    private EventStream eventStream;

    private String connectorIdentifier;
    private String formatIdentifier;
    private String[] partitionKeys;
    private Schema.Builder _schemaBuilder;
    private Map<String, String> options;

    // We want to default to using ISO-8601 timestamp format, not SQL,
    // since WMF Event Platform timestamps are ISO-8601 formatted strings.
    private static final String JSON_TIMESTAMP_FORMAT_KEY = "json.timestamp-format.standard";
    private static final String JSON_TIMESTAMP_FORMAT_DEFAULT = "ISO-8601";

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
    @Nonnull
    public static EventTableDescriptorBuilder from(
        List<String> eventSchemaBaseUris,
        String eventStreamConfigUri
    ) {
        return EventTableDescriptorBuilder.from(
            eventSchemaBaseUris, eventStreamConfigUri, emptyMap()
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
    @Nonnull
    public static EventTableDescriptorBuilder from(
        List<String> eventSchemaBaseUris,
        String eventStreamConfigUri,
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
    @Nonnull
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
    @Nonnull
    public EventTableDescriptorBuilder connector(String connectorIdentifier) {
        this.connectorIdentifier = connectorIdentifier;
        return this;
    }

    /**
     * Sets the EventStream that will be used to build the TableDescriptor.
     */
    @Nonnull
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
    @Nonnull
    public EventTableDescriptorBuilder eventStream(String streamName) {
        return eventStream(eventStreamFactory.createEventStream(streamName));
    }

    /**
     * Sets the Schema.Builder. If you don't set this the Table Schema will be
     * converted from the eventStream's JSONSchema.
     */
    @Nonnull
    public EventTableDescriptorBuilder schemaBuilder(Schema.Builder schemaBuilder) {
        this._schemaBuilder = schemaBuilder;
        return this;
    }

    /**
     * Sets the TableDescriptor.Builder options.
     * This clears out any previously set options.
     */
    @Nonnull
    public EventTableDescriptorBuilder options(Map<String, String> options) {
        this.options = new HashMap<>(options);
        return this;
    }

    /**
     * Sets a single TableDescriptor.Builder option.
     */
    @Nonnull
    public EventTableDescriptorBuilder option(String key, String value) {
        this.options.put(key, value);
        return this;
    }

    /**
     * Sets the format for the TableDescriptor.Builder.
     * This needed by most, but not all, connectors.
     */
    @Nonnull
    public EventTableDescriptorBuilder format(String format) {
        this.formatIdentifier = format;
        return this;
    }

    /**
     * Sets the partitionKeys for the TableDescriptor.Builder.
     */
    @Nonnull
    public EventTableDescriptorBuilder partitionedBy(String... partitionKeys) {
        this.partitionKeys = partitionKeys;
        return this;
    }

    /**
     * Helper to aid in setting up a kafka connector for the EventStream.
     * You must first call eventStream() before calling this method so that
     * it can fill in the details for the connector from the EventStream.
     *
     * Note: The Kafka Table Connector option scan.start.mode will not be set,
     * relying on the default value of "group-offsets".
     * The Kafka consumer property auto.offset.reset will be set to "latest",
     * which will be used in the case that there are no committed group offsets.
     * Override this behavior by calling e.g.
     * {@code option("properties.auto.offset.reset", "earliest")}
     *
     * This does not set the key format or hoist any metadata fields
     * (like kafka_timestamp) into the schema.
     * See withKafkaTimestampAsWatermark to help use the kafka timestamp
     * as the watermark.
     *
     * @param bootstrapServers
     *  Kafka bootstrap.servers property.
     *
     * @param consumerGroup
     *  Kafka consumer.group.id property.
     *
     */
    @Nonnull
    public EventTableDescriptorBuilder setupKafka(
        String bootstrapServers,
        String consumerGroup
    ) {
        Preconditions.checkState(
            this.eventStream != null,
            "Must call eventStream() before calling setupKafka()."
        );

        connector("kafka");
        // EventStreams in Kafka are JSON.
        format("json");

        option("properties.bootstrap.servers", bootstrapServers);
        option("topic", String.join(";", eventStream.topics()));
        option("properties.group.id", consumerGroup);
        option("properties.auto.offset.reset", "latest");

        return this;
    }

    /**
     * Adds a "kafka_timestamp" column to the schema and uses
     * it as the watermark field with a default watermark delay of 10 seconds.
     */
    @Nonnull
    public EventTableDescriptorBuilder withKafkaTimestampAsWatermark() {
        return withKafkaTimestampAsWatermark(
            "kafka_timestamp",
            10
        );
    }

    /**
     * Adds kafka timestamp as a virtual column to the schema,
     * and uses it as the watermark with a delay of watermarkDelaySeconds.
     *
     * @param columnName
     *  Name of the timestamp column to add to the schema.

     * @param watermarkDelaySeconds
     *  Seconds to delay the watermark by.
     *
     */
    @Nonnull
    public EventTableDescriptorBuilder withKafkaTimestampAsWatermark(
        String columnName,
        int watermarkDelaySeconds
    ) {
        String watermarkExpression;

        if (watermarkDelaySeconds == 0) {
            watermarkExpression = columnName;
        } else {
            watermarkExpression = columnName + " - INTERVAL '" + watermarkDelaySeconds + "' SECOND";
        }

        return withKafkaTimestampAsWatermark(
            columnName,
            watermarkExpression
        );
    }

    /**
     * Adds kafka timestamp as a virtual column to the schema,
     * and uses it as the watermark.
     * See also {@link Schema.Builder#watermark(String, String)}
     *
     * @param columnName
     *  Name of the column to add to the schema.
     *
     * @param sqlExpression
     *  SQL expression to use for the watermark.
     *  This should probably refer to the columnName, e.g. "$columnName - INTERVAL '30' seconds"
     *
     */
    @Nonnull
    public EventTableDescriptorBuilder withKafkaTimestampAsWatermark(
        String columnName,
        String sqlExpression
    ) {
        Schema.Builder sb = getSchemaBuilder();
        sb.columnByMetadata(
            columnName,
            "TIMESTAMP_LTZ(3) NOT NULL",
            "timestamp",
            true
        )
        .watermark(columnName, sqlExpression);

        return schemaBuilder(sb);
    }

    /**
     * Use this to get the schemaBuilder for the eventStream
     * in order to augment and reset it before calling build().
     * If you change the Schema.Builder, make sure you call
     * {@code schemaBuilder(modifiedSchemaBuilder)} to apply your changes
     * before calling {@code build()}.
     *
     * @return
     *  The Schema.Builder that will be used to build the Table Schema.
     */
    @Nonnull
    public Schema.Builder getSchemaBuilder() {
        Preconditions.checkState(
            !(this.eventStream == null && this._schemaBuilder == null),
            "Must call eventStream() or schemaBuilder() before calling getSchemaBuilder()."
        );

        if (this._schemaBuilder == null) {
            this._schemaBuilder = JsonSchemaFlinkConverter.toSchemaBuilder(
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
    @Nonnull
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

        // If not specified, set the JSON timestamp format to our default.
        if (!options.containsKey(JSON_TIMESTAMP_FORMAT_KEY)) {
            options.put(JSON_TIMESTAMP_FORMAT_KEY, JSON_TIMESTAMP_FORMAT_DEFAULT);
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
    @Nonnull
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
