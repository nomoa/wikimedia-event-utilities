package org.wikimedia.eventutilities.flink.formats.json;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.DataType;
import org.wikimedia.eventutilities.core.event.types.JsonSchemaConverter;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Converts from JSONSchemas to Flink types.
 *
 * Adheres to a few Event Platform specific schema conventions,
 * specifically support for specifying Map types in JSONSchema
 * via additionalProperties schema.
 *
 * Example:
 * Create a streaming Table from JSON data in a Kafka topic using a JSONSchema.
 * <pre>{@code
 * ObjectNode jsonSchema = # ... Get this somehow, perhaps from EventStream schema() method.
 *
 * # This schemaBuilder will already have the DataType set via the JSONSchema.
 * Schema.Builder schemaBuilder = JsonSchemaFlinkConverter.toSchemaBuilder(jsonSchema);
 *
 * # Add the kafka_timestamp as the metadata column and use it as the watermark.
 * schemaBuilder.columnByMetadata(
 *     "kafka_timestamp",
 *     "TIMESTAMP_LTZ(3) NOT NULL",
 *     "timestamp",
 *     true
 * );
 * schemaBuilder.watermark("kafka_timestamp", "kafka_timestamp");
 *
 * # Create a Dynamic Table from this topic in Kafka.
 * stEnv.createTemporaryTable(
 *     "my_table",
 *     TableDescriptor.forConnector("kafka")
 *         .schema(schemaBuilderlder.build())
 *         .option("properties.bootstrap.servers", "localhost:9092")
 *         .option("topic", "my_stream_topic")
 *         .option("properties.group.id", "my_consumer_group0")
 *         .option("scan.startup.mode", "latest-offset")
 *         .format("json")
 *         .build()
 * )
 * }</pre>
 *
 * Or, get the RowTypeInfo (AKA TypeInformation of Row) corresponding to an event JSONSchema.
 * <pre>{@code
 * ObjectNode jsonSchema = # ... Get this somehow, perhaps from EventStream schema() method.
 * RowTypeInfo eventSchemaTypeInfo = JsonSchemaFlinkConverter.toRowTypeInfo(jsonSchema);
 * }</pre>
 */
@ParametersAreNonnullByDefault
public final class JsonSchemaFlinkConverter {

    /**
     * JsonSchemaConverter instance to convert to Flink Table API DataType.
     */
    private static final JsonSchemaConverter<DataType> dataTypeConverter =
        new JsonSchemaConverter<>(new DataTypeSchemaConversions());

    /**
     * JsonSchemaConverter to convert to Flink DataStream API TypeInformation.
     */
    private static final JsonSchemaConverter<TypeInformation<?>> typeInformationConverter =
        new JsonSchemaConverter<>(new TypeInformationSchemaConversions());

    /**
     * Returns a Table API Schema Builder starting with a Row DataType
     * converted from the provided JSONSchema.
     *
     * @param jsonSchema
     *  The JSONSchema ObjectNode.  This should have "type": "object"
     *  to properly convert to a logical
     *  {@link org.apache.flink.table.types.logical.RowType}
     *
     * @throws IllegalArgumentException
     *  if the JSONSchema is not on "object" type.
     *
     * @return
     *  {@link org.apache.flink.table.api.Schema.Builder}
     *  with DataType with logical RowType as schema.
     */
    @Nonnull
    public static Schema.Builder toSchemaBuilder(ObjectNode jsonSchema) {
        JsonSchemaConverter.checkJsonSchemaIsObject(jsonSchema);

        DataType dataType = toDataType(jsonSchema);

        // Assert that the converted DataType's LogicalType is of type Row.
        LogicalType logicalType = dataType.getLogicalType();
        if (!logicalType.is(LogicalTypeRoot.ROW)) {
            throw new IllegalStateException(
                "Converted JSONSchema to Flink LogicalType " + logicalType +
                " but expected " + LogicalTypeRoot.ROW + ". This should not happen."
            );
        }

        Schema.Builder builder = Schema.newBuilder();
        builder.fromRowDataType(dataType);
        return builder;
    }

    /**
     * Converts this JSONSchema to a Flink Table API DataType.
     *
     * @param jsonSchema
     *  The JSONSchema ObjectNode.  This should have at minimum type.
     *
     * @return
     *  jsonSchema converted to
     *  {@link DataType}
     */
    @Nonnull
    public static DataType toDataType(ObjectNode jsonSchema) {
        return dataTypeConverter.convert(jsonSchema);
    }

    /**
     * Converts this JSONSchema to a Flink DataStream API TypeInformation.
     *
     * @param jsonSchema
     *  The JSONSchema ObjectNode. This should have at minimum "type".
     *
     * @return
     *  jsonSchema converted to {@link TypeInformation}
     */
    @Nonnull
    public static TypeInformation<?> toTypeInformation(ObjectNode jsonSchema) {
        return typeInformationConverter.convert(jsonSchema);
    }

    /**
     * Converts this JSONSchema to a Flink DataStream API {@link RowTypeInfo},
     * which is an instance of
     * {@link TypeInformation}&lt;{@link org.apache.flink.types.Row}&gt;.
     * RowTypeInfo has some extra logic for working with TypeInformation
     * when it represents a {@link org.apache.flink.types.Row}.
     * You can RowTypeInfo as if it were a TypeInformation of Row.
     *
     * @param jsonSchema
     *  The JSONSchema ObjectNode.  his should have "type": "object"
     *  to property convert to
     *  {@link TypeInformation}&lt;{@link org.apache.flink.types.Row}&gt;.
     *
     * @throws IllegalArgumentException
     *  if the JSONSchema is not on "object" type.

     * @return
     *  jsonSchema converted to {@link TypeInformation}&lt;{@link org.apache.flink.types.Row}&gt;.
     */
    @Nonnull
    public static RowTypeInfo toRowTypeInfo(ObjectNode jsonSchema
    ) {
        JsonSchemaConverter.checkJsonSchemaIsObject(jsonSchema);
        return (RowTypeInfo)toTypeInformation(jsonSchema);
    }

    /**
     * Gets a JSON deserializer to Row in hybrid named position mode for the jsonSchema.
     * Missing fields are allowed (and set to null), and parse errors
     * are always thrown.
     *
     * @param jsonSchema
     *  The JSONSchema ObjectNode.  his should have "type": "object"
     *  to property convert to
     *  {@link TypeInformation}&lt;{@link org.apache.flink.types.Row}&gt;.
     *
     * @return
     *  DeserializationSchema of Row that can deserialize JSON data for the jsonSchema to a FLink Row.
     */
    public static JsonRowDeserializationSchema toDeserializationSchemaRow(
        @Nonnull ObjectNode jsonSchema
    ) {
        return new JsonRowDeserializationSchema.Builder(toRowTypeInfo(jsonSchema)).build();
    }

    /**
     * Constructor to make maven checkstyle plugin happy.
     * See: https://checkstyle.sourceforge.io/config_design.html#HideUtilityClassConstructor
     */
    private JsonSchemaFlinkConverter() {}

}
