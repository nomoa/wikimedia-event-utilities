package org.wikimedia.eventutilities.flink.formats.json;
import java.util.Iterator;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;

/**
 * Converts from JSONSchemas to the Flink Table API DataTypes.
 * Adheres to a few Event Platform specific schema conventions,
 * specifically support for specifying Map types in JSONSchema
 * via additionalProperties schema.
 *
 * Example:
 * Create a streaming Table from JSON data in a Kafka topic using a JSONSchema.
 * <code>
 * ObjectNode jsonSchema = # ... Get this somehow, perhaps from EventStream schema() method.
 *
 * # This schemaBuilder will already have the DataType set via the JSONSchema.
 * Schema.Builder schemaBuilder = JsonSchemaConverter.getSchemaBuilder(jsonSchema);
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
 *         .schema(schemaBuilder.build())
 *         .option("properties.bootstrap.servers", "localhost:9092")
 *         .option("topic", "my_stream_topic")
 *         .option("properties.group.id", "my_consumer_group0")
 *         .option("scan.startup.mode", "latest-offset")
 *         .format("json")
 *         .build()
 * )
 * </code>
 *
 */
public final class JsonSchemaConverter {

    // JSONSchema names used to convert the JSONSchema Flink DataTypes
    // (Copied from Flink's JsonRowSchemaConverter
    // see https://spacetelescope.github.io/understanding-json-schema/UnderstandingJSONSchema.pdf
    private static final String TITLE = "title";
    private static final String DESCRIPTION = "description";
    private static final String PROPERTIES = "properties";
    private static final String ADDITIONAL_PROPERTIES = "additionalProperties";
    private static final String TYPE = "type";
    private static final String ITEMS = "items";

    private static final String TYPE_NULL = "null";
    private static final String TYPE_BOOLEAN = "boolean";
    private static final String TYPE_OBJECT = "object";
    private static final String TYPE_ARRAY = "array";
    private static final String TYPE_NUMBER = "number";
    private static final String TYPE_INTEGER = "integer";
    private static final String TYPE_STRING = "string";

    // TODO: Support conversion from these to more specific Flink DataTypes.
    //    private static final String FORMAT_DATE = "date";
    //    private static final String FORMAT_TIME = "time";
    //    private static final String FORMAT_DATE_TIME = "date-time";
    //    private static final String CONTENT_ENCODING_BASE64 = "base64";

    private static final Logger log = LoggerFactory.getLogger(JsonSchemaConverter.class);

    /*
        NOTE: To avoid possible confusion, there are quite a few overloaded terms when
        dealing with Jackson, JSON, JsonSchema and Flink types.
        Jackson JsonNodes represent a single JSON value.  These can be mapped
        to primitives like ints and strings, or collections like arrays, maps and objects.
        A Jackson ObjectNode specifically refers to a JSON object, e.g. {"a": "b"}.
        A JSONSchema is a JSON document that specifies the structure of other
        JSON documents.  So, a JSONSchema ObjectNode should have a "type" field.
        If that "type" is "object", then it means that the type of that field in the
        JSON document should be a JSON object.
        For Flink Table API, we convert JSONSchema ObjectNodes with "type": "object" to
        Flink LogicalType RowType.
        All JSONSchema JsonNodes must be ObjectNodes
        (because you need a JSON object to describe a JSON document).
    */

    /**
     * Returns a Table API Schema Builder starting with a Row DataType
     * converted from the provided JSONSchema object node.
     *
     * @param jsonSchema
     *      The JSONSchema Object. This must be a JSONSchema of type object to
     *      properly convert to a logical RowType.
     *
     * @return Schema.Builder with DataType with logical RowType as schema.
     */
    public static Schema.Builder toSchemaBuilder(
        @Nonnull ObjectNode jsonSchema
    ) {
        String schemaName;
        if (jsonSchema.hasNonNull(TITLE)) {
            schemaName = jsonSchema.get(TITLE).textValue();
        } else {
            schemaName = "Unknown JSONSchema";
        }

        String jsonSchemaType = getJsonSchemaType(jsonSchema, schemaName);
        Preconditions.checkArgument(
            jsonSchemaType.equals(TYPE_OBJECT),
            "When converting JSONSchema to Flink Table Schema, expected JSONSchema \""
            + TYPE + "\" to be \" + " + TYPE_OBJECT + "\" but was " + jsonSchemaType
        );

        DataType dataType = toDataType(jsonSchema);
        LogicalType logicalType = dataType.getLogicalType();
        if (!logicalType.is(LogicalTypeRoot.ROW)) {
            throw new IllegalStateException(
                "Converted JSONSchema to Flink LogicalType " + logicalType +
                " but expected " + LogicalTypeRoot.ROW + ". This should not happen."
            );
        }

        Schema.Builder builder = Schema.newBuilder();
        return builder.fromRowDataType(dataType);
    }

    /**
     * Converts this JSONSchema to a Flink Table API DataType of LogicalType RowType
     *
     * Note: A special case of converting object with an additionalProperties schema to a Map is
     * included. In JSONSchema, additionalProperties can either be a boolean
     * or an object.  If it is an object, it expected to specify the schema
     * of the unknown properties. This is what we need for a MapType.
     * We want to still allow object schemas to indicate that they have specific
     * property keys in a MapType though, so an object with additionalProperties
     * with a schema can still include a defined properties.  In this case, we will
     * use a MapType here and the defined properties will be ignored in the Spark
     * schema.  It is up to the schema author to ensure that the types of the defined
     * properties match the additionalProperties schema; that is, all defined properties
     * must have the same type as the additionalProperties, as this is what will
     * be used for the value in the MapType.
     * See: https://wikitech.wikimedia.org/wiki/Event_Platform/Schemas/Guidelines#map_types
     *
     * @param jsonSchema
     *      The JSONSchema Object.  This should have at minimum type.
     *
     * @return jsonSchema converted to DataType with logical RowType
     */
    public static DataType toDataType(
        @Nonnull ObjectNode jsonSchema
    ) {
        return toDataType(jsonSchema, null);
    }

    /**
     * Converts this JSONSchema to a Flink Table API DataType.
     *
     * @param jsonSchema
     *      The JSONSchema Object.  This should have at minimum type.
     *
     * @param fieldName
     *      Mostly used here for informative error and log messages.
     *      When used by toFieldDataType, the value passed to recursive calls is different.
     *
     * @return jsonSchema converted to Flink DataType.
     */
    @SuppressWarnings({"checkstyle:LineLength", "checkstyle:CyclomaticComplexity"})
    private static DataType toDataType(
        ObjectNode jsonSchema,
        String fieldName
    ) {
        Preconditions.checkArgument(
            !jsonSchema.isNull(),
            "JSONSchema cannot be a \"null\" node"
        );

        if (fieldName == null) {
            if (jsonSchema.hasNonNull(TITLE)) {
                fieldName = jsonSchema.get(TITLE).textValue();
            } else {
                fieldName = "Unknown field name";
            }
        }

        String jsonSchemaType = getJsonSchemaType(jsonSchema, fieldName);
        DataType dataType;
        switch (jsonSchemaType) {

            case TYPE_NULL:
                dataType = DataTypes.NULL();
                break;

            case TYPE_BOOLEAN:
                dataType = DataTypes.BOOLEAN();
                break;

            case TYPE_INTEGER:
                dataType = DataTypes.BIGINT();
                break;

            case TYPE_NUMBER:
                dataType = DataTypes.DOUBLE();
                break;

            case TYPE_STRING:
                dataType = DataTypes.STRING();
                break;

            case TYPE_ARRAY:
                dataType = toArrayDataType(jsonSchema, fieldName);
                break;

            case TYPE_OBJECT:
                // An object type must have a schema defined either in properties (Row type)
                // or additionalProperties (Map type).

                // Special map-case: additionalProperties has a schema,
                // this is a Map type instead of a Row Type.
                // https://wikitech.wikimedia.org/wiki/Event_Platform/Schemas/Guidelines#map_types
                if (
                    jsonSchema.hasNonNull(ADDITIONAL_PROPERTIES) &&
                    jsonSchema.get(ADDITIONAL_PROPERTIES).isObject()
                ) {
                    dataType = toMapDataType(jsonSchema, fieldName);
                    // TODO: if properties are specified, they should be compatible with the map type
                    // (additionalProperties) schema.
                    // Those properties are used more for hinting and validation
                    // (e.g. requiring certain map keys during JSONSchema validation),
                    // so we don't strictly have to do this here, but it is probably a good idea.
                    // See: https://github.com/wikimedia/analytics-refinery-source/blob/master/refinery-spark/src/main/scala/org/wikimedia/analytics/refinery/spark/sql/JsonSchemaConverter.scala#L133
                } else {
                    dataType = toRowDataType(jsonSchema, fieldName);
                }
                break;

            default:
                throw new IllegalArgumentException(
                    "Unknown JSONSchema \"" + TYPE  + "\"" + jsonSchemaType +
                    "in field " + fieldName
                );
        }

        log.debug("Converted JSONSchema field {} to Flink Table DataType {}", fieldName, dataType);
        return dataType;
    }

    /**
     * Array DataTypes must always specify their entry items types.
     *
     * @param jsonSchema
     *      The JSONSchema Object. This must specify the array value
     *      type via the JSONSchema items spec.
     *
     * @param fieldName
     *      Mostly used here for informative error and log messages.
     *
     * @return jsonSchema converted to DataType with logical ArrayType
     */
    private static DataType toArrayDataType(ObjectNode jsonSchema, String fieldName) {
        JsonNode itemsSchema = getJsonNode(
            jsonSchema,
            ITEMS,
            fieldName + " array JSONSchema did not specify the items type"
        );
        Preconditions.checkArgument(
            itemsSchema.isObject() && itemsSchema.has(TYPE),
            fieldName + " array JSONSchema must specify the items type for field, " +
                "e.g. \"" + ITEMS + "\": { \"" + TYPE + "\": \"string\"}"
        );

        DataType itemsType = toDataType((ObjectNode)itemsSchema, fieldName + "." + ITEMS);
        return DataTypes.ARRAY(itemsType);
    }

    /**
     * Map DataTypes are converted from additionalProperties schemas.  These types of
     * schemas specify the possible value types in the map.  Key types are always strings.
     *
     * @param jsonSchema
     *      The JSONSchema Object. This must be an 'object' with an additionalProperties
     *      spec with a type schema that specifies the type of the map values.
     *      Any defined properties are ignored.
     *      See: https://wikitech.wikimedia.org/wiki/Event_Platform/Schemas/Guidelines#map_types
     *
     * @param fieldName
     *      Mostly used here for informative error and log messages.
     *
     * @return jsonSchema converted to DataType with logical MapType
     */
    private static DataType toMapDataType(ObjectNode jsonSchema, String fieldName) {
        String errorMessage = fieldName + " \"" + ADDITIONAL_PROPERTIES +
            "\" must be a JSONSchema object to be a Map DataType";

        ObjectNode additionalPropertiesSchema = (ObjectNode)getJsonNode(
            jsonSchema,
            ADDITIONAL_PROPERTIES,
            errorMessage
        );
        Preconditions.checkArgument(
            additionalPropertiesSchema.isObject(),
            errorMessage
        );
        Preconditions.checkArgument(
            additionalPropertiesSchema.has(TYPE),
            fieldName + " \"" + ADDITIONAL_PROPERTIES + "\" JSONSchema object must specify value \""
                + TYPE + "\"" + "to be a Map DataType"
        );

        DataType key = DataTypes.STRING();
        DataType value = toDataType(
            additionalPropertiesSchema,
            fieldName + "." + ADDITIONAL_PROPERTIES
        );

        return DataTypes.MAP(key, value);
    }

    /**
     * Converts from an JSONSchema object with properties to a DataType with logical RowType.
     * NOTE: We do not need to consider JSONSchema required-ness here, as all
     * Flink Row fields are always nullable.
     *
     * @param jsonSchema
     *      The JSONSchema Object. This must be an 'object' with defined properties.
     *      additionalProperties are not support and will be ignored.
     *
     * @param fieldName
     *      Mostly used here for informative error and log messages.
     *
     * @return jsonSchema converted to DataType with logical RowType
     */
    private static DataType toRowDataType(ObjectNode jsonSchema, String fieldName) {
        String errorMessage = fieldName + " object JSONSchema \"" + PROPERTIES + "\" is not an object.";
        JsonNode properties = getJsonNode(jsonSchema, PROPERTIES, errorMessage);
        Preconditions.checkArgument(properties.isObject(), errorMessage);

        DataTypes.Field[] rowFields = new DataTypes.Field[properties.size()];
        int fieldNum = 0;
        Iterator<Map.Entry<String, JsonNode>> jsonSchemaFields = properties.fields();
        while (jsonSchemaFields.hasNext()) {
            Map.Entry<String, JsonNode> field = jsonSchemaFields.next();
            DataTypes.Field fieldDataType = toFieldDataType(
                (ObjectNode)field.getValue(),
                field.getKey(),
                fieldName
            );
            rowFields[fieldNum] = fieldDataType;
            fieldNum++;
        }
        return DataTypes.ROW(rowFields);
    }

    /**
     * A DataTypes.Field is just a special DataType with a name and an optional description.
     * Useful for named DataType fields within a Row DataType.
     *
     * @param jsonField
     *      The JSONSchema Object. This must be an 'object' with defined properties.
     *      additionalProperties are not support and will be ignored.
     *
     * @param fieldName
     *      The name of this field.  This is explicitly used as the fieldName in the FIELD DataType.
     *
     * @param parentFieldPath
     *  Dotted path of the parent object field.  This is used for logging and error messages, and is
     *  passed through toDataType fieldName as parentFieldPath.fieldName when building the DataType
     *  of the field value. If null, the fieldName will be passed to toDataType instead.
     *
     * @return jsonSchema converted to DataTypes.Field, named by fieldName.
     */
    private static DataTypes.Field toFieldDataType(
        ObjectNode jsonField,
        String fieldName,
        String parentFieldPath
    ) {
        String fieldPath = parentFieldPath != null ? parentFieldPath + "." + fieldName : fieldName;
        DataType fieldValueDataType = toDataType(jsonField, fieldPath);

        DataTypes.Field dataType;
        if (jsonField.hasNonNull(DESCRIPTION)) {
            dataType =  DataTypes.FIELD(
                fieldName,
                fieldValueDataType,
                jsonField.get(DESCRIPTION).textValue()
            );
        } else {
            dataType = DataTypes.FIELD(
                fieldName,
                fieldValueDataType
            );
        }

        return dataType;
    }

    /**
     * DRY helper function to get a JsonNode out of an ObjectNode by key,
     * throwing IllegalArgumentException if the key does not exist, or is set to "null".
     * @param objectNode ObjectNode from which to get JsonNode by key
     * @param key Field name key to get out of the ObjectNode.
     * @param errorMessage Error message for IllegalArgumentException if key is not set or is "null".
     * @return JsonNode in objectNode at key
     */
    private static JsonNode getJsonNode(ObjectNode objectNode, String key, String errorMessage) {
        JsonNode jsonNode = objectNode.get(key);
        Preconditions.checkArgument(
            jsonNode != null && !jsonNode.isNull(),
            errorMessage
        );
        return jsonNode;
    }

    /**
     * DRY helper function to get the JSONSchema "type" field as a String value.
     * Throws IllegalArgumentException if jsonSchema does not have "type" field
     * or "type" is not a String.
     */
    private static String getJsonSchemaType(ObjectNode jsonSchema, String fieldName) {
        String errorMessage = "JSONSchema at \"" + fieldName +
            "\" must contain valid JSONSchema \"" + TYPE + "\"";
        JsonNode schemaTypeNode = getJsonNode(jsonSchema, TYPE, errorMessage);
        Preconditions.checkArgument(schemaTypeNode.isTextual(), errorMessage);
        return schemaTypeNode.textValue();
    }

    /**
     * Constructor to make maven checkstyle plugin happy.
     * See: https://checkstyle.sourceforge.io/config_design.html#HideUtilityClassConstructor
     */
    private JsonSchemaConverter() {}

}
