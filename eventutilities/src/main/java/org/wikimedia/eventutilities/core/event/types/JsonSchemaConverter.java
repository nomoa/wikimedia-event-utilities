package org.wikimedia.eventutilities.core.event.types;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;


/**
 * JsonSchemaConverter iterates through the JSONSchema properties
 * and uses the provided {@link SchemaConversions} implementation to
 * convert from JSONSchema types.
 *
 * TO DO: Implement support for String to Timestamp conversion.
 * https://phabricator.wikimedia.org/T310495
 * https://phabricator.wikimedia.org/T278467
 *
 * @param <T>
 *  {@link SchemaConversions} implementation
 */
@ParametersAreNonnullByDefault
public class JsonSchemaConverter<T> {

    // JSONSchema keywords
    private static final String TITLE = "title";
    private static final String DESCRIPTION = "description";
    private static final String PROPERTIES = "properties";
    private static final String ADDITIONAL_PROPERTIES = "additionalProperties";
    private static final String TYPE = "type";
    private static final String ITEMS = "items";
    private static final String REQUIRED = "required";
    private static final String FORMAT = "format";

    // JSONSchema types
    private static final String TYPE_NULL = "null";
    private static final String TYPE_BOOLEAN = "boolean";
    private static final String TYPE_OBJECT = "object";
    private static final String TYPE_ARRAY = "array";
    private static final String TYPE_NUMBER = "number";
    private static final String TYPE_INTEGER = "integer";
    private static final String TYPE_STRING = "string";


    private static final String FORMAT_DATE_TIME = "date-time";

    // TO DO: Support conversion from these JSONSchema formats to more specific types.
    // See Flink's JsonRowSchemaConverter
    //    private static final String FORMAT_DATE = "date";
    //    private static final String FORMAT_TIME = "time";
    //    private static final String CONTENT_ENCODING_BASE64 = "base64";

    private static final Logger log = LoggerFactory.getLogger(JsonSchemaConverter.class);

    private final @Nonnull
    SchemaConversions<T> schemaConversions;

    public JsonSchemaConverter(SchemaConversions<T> schemaConversions) {
        this.schemaConversions = schemaConversions;
    }

    /**
     * Converts this JSONSchema using the provided SchemaConversions.
     *
     * Note: A special case of converting object with an additionalProperties schema to a Map is
     * included. In JSONSchema, additionalProperties can either be a boolean
     * or an object.  If it is an object, it expected to specify the schema
     * of the unknown properties. This is what we need for a Map type.
     * We want to still allow object schemas to indicate that they have specific
     * property keys in a Map type though, so an object with additionalProperties
     * with a schema can still include a defined properties.  In this case, we will
     * use a Map type here and the defined properties will be ignored in final map type.
     * It is up to the schema author to ensure that the types of the defined
     * properties match the additionalProperties schema; that is, all defined properties
     * must have the same type as the additionalProperties, as this is what will
     * be used for the value type in the Map.
     * See: https://wikitech.wikimedia.org/wiki/Event_Platform/Schemas/Guidelines#map_types
     *
     * @param jsonSchema
     *      The JSONSchema Object.  This should have at minimum type.
     *
     * @return jsonSchema converted to T using the provided SchemaConversions.
     */
    @Nonnull
    public T convert(ObjectNode jsonSchema) {
        checkArgument(!jsonSchema.isNull(), "JSONSchema cannot be a \"null\" node.");
        return convert(jsonSchema, "<root>");
    }

    /**
     * Iterates through jsonSchema and calls appropriate TypeConverter methods
     * on each jsonSchema property type in order to convert it into type T.
     *
     * @param jsonSchema
     *      The JSONSchema Object.  This should have at minimum type.
     *
     * @param nodePath
     *      Fully qualified dotted json path to the current node in the JSONSchema.
     *      Used only for informational error and log messages.
     */
    @Nonnull
    protected T convert(
        ObjectNode jsonSchema,
        String nodePath
    ) {
        // Get the value of the JSONSchema "type".
        String jsonSchemaType = getJsonSchemaType(jsonSchema);
        T convertedType;

        // Convert jsonSchemaType to T.
        switch (jsonSchemaType) {

            case TYPE_NULL:
                convertedType = schemaConversions.typeNull();
                break;

            case TYPE_BOOLEAN:
                convertedType = schemaConversions.typeBoolean();
                break;

            case TYPE_INTEGER:
                convertedType = schemaConversions.typeInteger();
                break;

            case TYPE_NUMBER:
                convertedType = schemaConversions.typeDecimal();
                break;

            case TYPE_STRING:
                if (jsonSchema.hasNonNull(FORMAT)) {
                    String format = getJsonNode(
                        jsonSchema,
                        FORMAT,
                        "Expected field " + nodePath + " to specify " + FORMAT
                    ).asText();

                    if (format.equals(FORMAT_DATE_TIME)) {
                        convertedType = schemaConversions.typeTimestamp();
                    } else {
                        convertedType = schemaConversions.typeString();
                    }
                } else {
                    convertedType = schemaConversions.typeString();
                }
                break;

            case TYPE_ARRAY:
                convertedType = convertArrayType(jsonSchema, nodePath);
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
                    convertedType = convertMapType(jsonSchema, nodePath);
                    // TO DO: if properties are specified, they should be compatible with the map type
                    // (additionalProperties) schema.
                    // Those properties are used more for hinting and validation
                    // (e.g. requiring certain map keys during JSONSchema validation),
                    // so we don't strictly have to do this here, but it is probably a good idea.
                } else {
                    convertedType = convertRowType(jsonSchema, nodePath);
                }
                break;

            default:
                throw new IllegalArgumentException(
                    "Unknown JSONSchema \"" + TYPE  + "\": \"" + jsonSchemaType +
                    "\" at " + nodePath
                );
        }

        log.debug(
            "Converted JSONSchema at {} to Flink Table DataType {}",
            nodePath,
            convertedType
        );

        return convertedType;
    }

    @Nonnull
    protected T convertArrayType(ObjectNode jsonSchema, String nodePath) {
        JsonNode itemsSchema = getJsonNode(
            jsonSchema,
            ITEMS,
            nodePath + " array JSONSchema did not specify the items type"
        );
        checkArgument(
            itemsSchema.isObject() && itemsSchema.has(TYPE),
            nodePath + " array JSONSchema must specify the items type for field, " +
                "e.g. \"" + ITEMS + "\": { \"" + TYPE + "\": \"string\"}"
        );

        T elementType = convert(
            (ObjectNode)itemsSchema,
            nodePath + "." + ITEMS
        );
        return schemaConversions.typeArray(elementType, true);
    }

    @Nonnull
    protected T convertMapType(ObjectNode jsonSchema, String nodePath) {
        String errorMessage = nodePath + "." + ADDITIONAL_PROPERTIES +
            "\" must be a JSONSchema object to be a Map type";

        JsonNode additionalPropertiesSchema = getJsonNode(
            jsonSchema,
            ADDITIONAL_PROPERTIES,
            errorMessage
        );
        checkArgument(additionalPropertiesSchema.isObject(), errorMessage);
        checkArgument(
            additionalPropertiesSchema.has(TYPE),
            nodePath + "." + ADDITIONAL_PROPERTIES +
                "JSONSchema object must specify value \""
                + TYPE + "\"" + "to be a Map type"
        );

        T valueType = convert(
            (ObjectNode)additionalPropertiesSchema,
            nodePath + "." + ADDITIONAL_PROPERTIES
        );


        // Maps made from JSON always have key strings.
        return schemaConversions.typeMap(schemaConversions.typeString(), valueType, true);
    }

    @Nonnull
    protected T convertRowType(ObjectNode jsonSchema, String nodePath) {
        String errorMessage = nodePath + " object JSONSchema \"" + PROPERTIES + "\" is not an object.";
        JsonNode properties = getJsonNode(jsonSchema, PROPERTIES, errorMessage);
        checkArgument(properties.isObject(), errorMessage);

        // Collect info about each JsonSchema property into
        // a RowField DTO.
        List<SchemaConversions.RowField<T>> rowFields = new ArrayList<>();
        Set<String> requiredFields = new HashSet<>();

        if (jsonSchema.hasNonNull(REQUIRED)) {
            JsonNode requiredNode = jsonSchema.get(REQUIRED);
            checkArgument(
                requiredNode.isArray(),
                "JSONSchema at " + nodePath +  "\"" + REQUIRED + "\" at " + "  must be an array"
            );
            requiredNode.elements().forEachRemaining(j -> requiredFields.add(j.asText()));
        }

        // Convert each property to a RowField and collect in a list of RowFields,
        // and then call typeRow on the schemaConversions implementation.
        properties.fields().forEachRemaining((Map.Entry<String, JsonNode> propertiesField) -> {
            String propertyName = propertiesField.getKey();

            String propertyNodePath = nodePath + "." + PROPERTIES + "." + propertyName;
            checkArgument(
                propertiesField.getValue().isObject(),
                "JSONSchema at \"" + propertyNodePath + "\" must be an object node."
            );

            ObjectNode propertyJsonSchema = (ObjectNode)propertiesField.getValue();

            // Covert the property schema type into T
            T propertyType = convert(propertyJsonSchema, propertyNodePath);

            // property is nullable (optional) if it is not in the list of required fields.
            boolean isNullable = !requiredFields.contains(propertyName);

            // description can be null.
            String description = propertyJsonSchema.hasNonNull(DESCRIPTION) ?
                propertyJsonSchema.get(DESCRIPTION).asText() :
                null;

            rowFields.add(new SchemaConversions.RowField<T>(propertyName, propertyType, isNullable, description));
        });

        return schemaConversions.typeRow(rowFields);

    }

    /**
     * DRY helper function to get a JsonNode out of an ObjectNode by key,
     * throwing IllegalArgumentException if the key does not exist, or is set to "null".
     * @param objectNode ObjectNode from which to get JsonNode by key
     * @param key Field name key to get out of the ObjectNode.
     * @param errorMessage Error message for IllegalArgumentException if key is not set or is "null".
     * @return JsonNode in objectNode at key
     */
    @Nonnull
    public static JsonNode getJsonNode(ObjectNode objectNode, String key, String errorMessage) {
        JsonNode jsonNode = objectNode.get(key);
        checkArgument(jsonNode != null && !jsonNode.isNull(), errorMessage);
        return jsonNode;
    }

    /**
     * DRY helper function to extract the JSONSchema "type".
     */
    @Nonnull
    public static String getJsonSchemaType(ObjectNode jsonSchema) {
        String getTypeErrorMessage = "jsonSchema does not contain valid JSONSchema \"" + TYPE + "\".";
        JsonNode schemaTypeNode = getJsonNode(jsonSchema, TYPE, getTypeErrorMessage);
        checkArgument(schemaTypeNode.isTextual(), getTypeErrorMessage);

        return schemaTypeNode.asText();
    }

    /**
     * DRY helper function that asserts that the jsonSchema has "type": "object".
     * @throws IllegalArgumentException if jsonSchema "type" != "object"
     */
    @Nonnull
    public static void checkJsonSchemaIsObject(ObjectNode jsonSchema) {
        String schemaTitle;
        if (jsonSchema.hasNonNull(TITLE)) {
            schemaTitle = jsonSchema.get(TITLE).textValue();
        } else {
            schemaTitle = "Untitled";
        }

        String jsonSchemaType = getJsonSchemaType(jsonSchema);
        checkArgument(
            jsonSchemaType.equals(TYPE_OBJECT),
            "When converting JSONSchema " + schemaTitle +
                " to Flink row schema, expected JSONSchema \"" + TYPE +
                "\" to be \" + " + TYPE_OBJECT + "\" but was " + jsonSchemaType
        );
    }

}
