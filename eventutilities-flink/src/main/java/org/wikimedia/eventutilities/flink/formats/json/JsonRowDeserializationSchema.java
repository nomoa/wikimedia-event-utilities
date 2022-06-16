/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.wikimedia.eventutilities.flink.formats.json;

import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static org.apache.flink.formats.common.TimeFormats.RFC3339_TIMESTAMP_FORMAT;
import static org.apache.flink.formats.common.TimeFormats.RFC3339_TIME_FORMAT;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.DECIMAL;
import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.types.RowUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;


/**
 * Deserialization schema from JSON to Flink Row type.
 *
 * NOTE: This class was directly copied and modified from Flink 1.15.0's deprecated
 * org.apache.flink.formats.json.JsonRowSerializationSchema.
 * It is used to bypass the Table API when you want a DataStream of Row
 * without needing to initialize a TableEnvironment.
 *
 * WMF changes from upstream Flink:
 * - Deserialize into name based Rows instead of position based Rows.
 *
 * <p>Deserializes a {@code byte[]} message as a JSON object and reads the specified fields.
 *
 * <p>Failures during deserialization are forwarded as wrapped IOExceptions.
 *
 * TODO:
 * - Should we return a Row in name based mode?
 * - Should we infer the RowKind automatically?   https://phabricator.wikimedia.org/T310082
 */

// Suppress all spotbugs warnings for this class, as it was copy/pasted from upstream Flink.
@SuppressFBWarnings
@SuppressWarnings({"checkstyle:ClassFanoutComplexity"})
public class JsonRowDeserializationSchema implements DeserializationSchema<Row> {

    private static final long serialVersionUID = -228294330688809195L;

    /** Type information describing the result type. */
    private final RowTypeInfo typeInfo;

    private final boolean failOnMissingField;

    /** Object mapper for parsing the JSON. */
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final DeserializationRuntimeConverter runtimeConverter;

    /** Flag indicating whether to ignore invalid fields/rows (default: throw an exception). */
    private final boolean ignoreParseErrors;

    // -- BEGIN WMF MODIFICATION --
    /**
     * 'field mode' of deserialized Rows.
     * See {@link Row} documentation for more info.
     */
    private final RowFieldMode rowFieldMode;
    // -- END WMF MODIFICATION --

    protected JsonRowDeserializationSchema(
        TypeInformation<Row> typeInfo,
        boolean failOnMissingField,
        boolean ignoreParseErrors,
        // -- BEGIN WMF MODIFICATION --
        RowFieldMode rowFieldMode
        // -- END WMF MODIFICATION --
    ) {
        checkNotNull(typeInfo, "Type information");
        checkArgument(typeInfo instanceof RowTypeInfo, "Only RowTypeInfo is supported");
        if (ignoreParseErrors && failOnMissingField) {
            throw new IllegalArgumentException(
                "JSON format doesn't support failOnMissingField and ignoreParseErrors are both true.");
        }
        this.typeInfo = (RowTypeInfo) typeInfo;
        this.failOnMissingField = failOnMissingField;
        this.runtimeConverter = createConverter(this.typeInfo);
        this.ignoreParseErrors = ignoreParseErrors;

        // -- BEGIN WMF MODIFICATION --
        this.rowFieldMode = rowFieldMode;
        // -- END WMF MODIFICATION --

        RowType rowType = (RowType) fromLegacyInfoToDataType(this.typeInfo).getLogicalType();
        boolean hasDecimalType =
            LogicalTypeChecks.hasNested(rowType, t -> t.getTypeRoot().equals(DECIMAL));
        if (hasDecimalType) {
            objectMapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        }
    }

    // -- BEGIN WMF MODIFICATION --
    /**
     * Default constructor that will deserialize Rows in hybrid POSITION_WITH_NAMES mode.
     */
    protected JsonRowDeserializationSchema(
        TypeInformation<Row> typeInfo,
        boolean failOnMissingField,
        boolean ignoreParseErrors
    ) {
        this(
            typeInfo,
            failOnMissingField,
            ignoreParseErrors,
            RowFieldMode.NAMED_POSITION
        );
    }
    // -- END WMF MODIFICATION --

    @SuppressWarnings("checkstyle:IllegalCatch")
    @Override
    public Row deserialize(byte[] message) throws IOException {
        try {
            final JsonNode root = objectMapper.readTree(message);
            return (Row) runtimeConverter.convert(objectMapper, root);
        } catch (Throwable t) {
            if (ignoreParseErrors) {
                return null;
            }
            throw new IOException(
                format(
                    Locale.getDefault(),
                    "Failed to deserialize JSON '%s'.",
                    new String(message, StandardCharsets.UTF_8)
                ),
                t
            );
        }
    }

    @Override
    public boolean isEndOfStream(Row nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return typeInfo;
    }

    /** Builder for {@link JsonRowDeserializationSchema}. */
    public static class Builder {

        private final RowTypeInfo typeInfo;
        private boolean failOnMissingField;
        private boolean ignoreParseErrors;
        private RowFieldMode rowFieldMode = RowFieldMode.NAMED_POSITION;

        /**
         * Creates a JSON deserialization schema for the given type information.
         *
         * @param typeInfo Type information describing the result type. The field names of {@link
         *     Row} are used to parse the JSON properties.
         */
        public Builder(TypeInformation<Row> typeInfo) {
            checkArgument(typeInfo instanceof RowTypeInfo, "Only RowTypeInfo is supported");
            this.typeInfo = (RowTypeInfo) typeInfo;
        }

        /**
         * Configures schema to fail if a JSON field is missing.
         *
         * <p>By default, a missing field is ignored and the field is set to null.
         */
        public Builder failOnMissingField() {
            this.failOnMissingField = true;
            return this;
        }

        /**
         * Configures schema to fail when parsing json failed.
         *
         * <p>By default, an exception will be thrown when parsing json fails.
         */
        public Builder ignoreParseErrors() {
            this.ignoreParseErrors = true;
            return this;
        }

        // -- BEGIN WMF MODIFICATION --
        public Builder rowFieldMode(RowFieldMode rowFieldMode) {
            this.rowFieldMode = rowFieldMode;
            return this;
        }
        // -- END WMF MODIFICATION --

        public JsonRowDeserializationSchema build() {
            return new JsonRowDeserializationSchema(
                typeInfo,
                failOnMissingField,
                ignoreParseErrors,
                rowFieldMode
            );
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final JsonRowDeserializationSchema that = (JsonRowDeserializationSchema) o;
        return Objects.equals(typeInfo, that.typeInfo)
            && Objects.equals(failOnMissingField, that.failOnMissingField)
            && Objects.equals(ignoreParseErrors, that.ignoreParseErrors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeInfo, failOnMissingField, ignoreParseErrors);
    }

    /*
    Runtime converter
    */

    /** Runtime converter that maps between {@link JsonNode}s and Java objects. */
    @FunctionalInterface
    private interface DeserializationRuntimeConverter extends Serializable {
        Object convert(ObjectMapper mapper, JsonNode jsonNode);
    }

    private DeserializationRuntimeConverter createConverter(TypeInformation<?> typeInfo) {
        DeserializationRuntimeConverter baseConverter =
            createConverterForSimpleType(typeInfo)
                .orElseGet(
                    () ->
                        createContainerConverter(typeInfo)
                            .orElseGet(
                                () ->
                                    createFallbackConverter(
                                        typeInfo.getTypeClass())));
        return wrapIntoNullableConverter(baseConverter);
    }

    @SuppressWarnings("checkstyle:IllegalCatch")
    private DeserializationRuntimeConverter wrapIntoNullableConverter(
        DeserializationRuntimeConverter converter) {
        return (mapper, jsonNode) -> {
            if (jsonNode.isNull()) {
                return null;
            }
            try {
                return converter.convert(mapper, jsonNode);
            } catch (Throwable t) {
                if (!ignoreParseErrors) {
                    throw t;
                }
                return null;
            }
        };
    }

    private Optional<DeserializationRuntimeConverter> createContainerConverter(
        TypeInformation<?> typeInfo) {
        if (typeInfo instanceof RowTypeInfo) {
            return Optional.of(createRowConverter((RowTypeInfo) typeInfo));
        } else if (typeInfo instanceof ObjectArrayTypeInfo) {
            return Optional.of(
                createObjectArrayConverter(
                    ((ObjectArrayTypeInfo) typeInfo).getComponentInfo()));
        } else if (typeInfo instanceof BasicArrayTypeInfo) {
            return Optional.of(
                createObjectArrayConverter(((BasicArrayTypeInfo) typeInfo).getComponentInfo()));
        } else if (isPrimitiveByteArray(typeInfo)) {
            return Optional.of(createByteArrayConverter());
        } else if (typeInfo instanceof MapTypeInfo) {
            MapTypeInfo<?, ?> mapTypeInfo = (MapTypeInfo<?, ?>) typeInfo;
            return Optional.of(
                createMapConverter(
                    mapTypeInfo.getKeyTypeInfo(), mapTypeInfo.getValueTypeInfo()));
        } else {
            return Optional.empty();
        }
    }

    private DeserializationRuntimeConverter createMapConverter(
        TypeInformation keyType, TypeInformation valueType) {
        DeserializationRuntimeConverter valueConverter = createConverter(valueType);
        DeserializationRuntimeConverter keyConverter = createConverter(keyType);
        return (mapper, jsonNode) -> {
            Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
            Map<Object, Object> result = new HashMap<>();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                Object key = keyConverter.convert(mapper, TextNode.valueOf(entry.getKey()));
                Object value = valueConverter.convert(mapper, entry.getValue());
                result.put(key, value);
            }
            return result;
        };
    }

    private DeserializationRuntimeConverter createByteArrayConverter() {
        return (mapper, jsonNode) -> {
            try {
                return jsonNode.binaryValue();
            } catch (IOException e) {
                throw new JsonParseException("Unable to deserialize byte array.", e);
            }
        };
    }

    private boolean isPrimitiveByteArray(TypeInformation<?> typeInfo) {
        return typeInfo instanceof PrimitiveArrayTypeInfo
            && ((PrimitiveArrayTypeInfo) typeInfo).getComponentType() == Types.BYTE;
    }

    private DeserializationRuntimeConverter createObjectArrayConverter(
        TypeInformation elementTypeInfo) {
        DeserializationRuntimeConverter elementConverter = createConverter(elementTypeInfo);
        return assembleArrayConverter(elementTypeInfo, elementConverter);
    }

    private DeserializationRuntimeConverter createRowConverter(RowTypeInfo typeInfo) {
        List<DeserializationRuntimeConverter> fieldConverters =
            Arrays.stream(typeInfo.getFieldTypes())
                .map(this::createConverter)
                .collect(Collectors.toList());

        return assembleRowConverter(typeInfo.getFieldNames(), fieldConverters);
    }

    private DeserializationRuntimeConverter createFallbackConverter(Class<?> valueType) {
        return (mapper, jsonNode) -> {
            // for types that were specified without JSON schema
            // e.g. POJOs
            try {
                return mapper.treeToValue(jsonNode, valueType);
            } catch (JsonProcessingException e) {
                throw new JsonParseException(
                    format(Locale.getDefault(), "Could not convert node: %s", jsonNode),
                    e
                );
            }
        };
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    private Optional<DeserializationRuntimeConverter> createConverterForSimpleType(
        TypeInformation<?> simpleTypeInfo) {
        if (simpleTypeInfo == Types.VOID) {
            return Optional.of((mapper, jsonNode) -> null);
        } else if (simpleTypeInfo == Types.BOOLEAN) {
            return Optional.of(this::convertToBoolean);
        } else if (simpleTypeInfo == Types.STRING) {
            return Optional.of(this::convertToString);
        } else if (simpleTypeInfo == Types.INT) {
            return Optional.of(this::convertToInt);
        } else if (simpleTypeInfo == Types.LONG) {
            return Optional.of(this::convertToLong);
        } else if (simpleTypeInfo == Types.DOUBLE) {
            return Optional.of(this::convertToDouble);
        } else if (simpleTypeInfo == Types.FLOAT) {
            return Optional.of((mapper, jsonNode) -> Float.parseFloat(jsonNode.asText().trim()));
        } else if (simpleTypeInfo == Types.SHORT) {
            return Optional.of((mapper, jsonNode) -> Short.parseShort(jsonNode.asText().trim()));
        } else if (simpleTypeInfo == Types.BYTE) {
            return Optional.of((mapper, jsonNode) -> Byte.parseByte(jsonNode.asText().trim()));
        } else if (simpleTypeInfo == Types.BIG_DEC) {
            return Optional.of(this::convertToBigDecimal);
        } else if (simpleTypeInfo == Types.BIG_INT) {
            return Optional.of(this::convertToBigInteger);
        } else if (simpleTypeInfo == Types.SQL_DATE) {
            return Optional.of(this::convertToDate);
        } else if (simpleTypeInfo == Types.SQL_TIME) {
            return Optional.of(this::convertToTime);
        } else if (simpleTypeInfo == Types.SQL_TIMESTAMP) {
            return Optional.of(this::convertToTimestamp);
        } else if (simpleTypeInfo == Types.LOCAL_DATE) {
            return Optional.of(this::convertToLocalDate);
        } else if (simpleTypeInfo == Types.LOCAL_TIME) {
            return Optional.of(this::convertToLocalTime);
        } else if (simpleTypeInfo == Types.LOCAL_DATE_TIME) {
            return Optional.of(this::convertToLocalDateTime);
        } else {
            return Optional.empty();
        }
    }

    private String convertToString(ObjectMapper mapper, JsonNode jsonNode) {
        if (jsonNode.isContainerNode()) {
            return jsonNode.toString();
        } else {
            return jsonNode.asText();
        }
    }

    private boolean convertToBoolean(ObjectMapper mapper, JsonNode jsonNode) {
        if (jsonNode.isBoolean()) {
            // avoid redundant toString and parseBoolean, for better performance
            return jsonNode.asBoolean();
        } else {
            return Boolean.parseBoolean(jsonNode.asText().trim());
        }
    }

    private int convertToInt(ObjectMapper mapper, JsonNode jsonNode) {
        if (jsonNode.canConvertToInt()) {
            // avoid redundant toString and parseInt, for better performance
            return jsonNode.asInt();
        } else {
            return Integer.parseInt(jsonNode.asText().trim());
        }
    }

    private long convertToLong(ObjectMapper mapper, JsonNode jsonNode) {
        if (jsonNode.canConvertToLong()) {
            // avoid redundant toString and parseLong, for better performance
            return jsonNode.asLong();
        } else {
            return Long.parseLong(jsonNode.asText().trim());
        }
    }

    private double convertToDouble(ObjectMapper mapper, JsonNode jsonNode) {
        if (jsonNode.isDouble()) {
            // avoid redundant toString and parseDouble, for better performance
            return jsonNode.asDouble();
        } else {
            return Double.parseDouble(jsonNode.asText().trim());
        }
    }

    private BigDecimal convertToBigDecimal(ObjectMapper mapper, JsonNode jsonNode) {
        if (jsonNode.isBigDecimal()) {
            // avoid redundant toString and toDecimal, for better performance
            return jsonNode.decimalValue();
        } else {
            return new BigDecimal(jsonNode.asText().trim());
        }
    }

    private BigInteger convertToBigInteger(ObjectMapper mapper, JsonNode jsonNode) {
        if (jsonNode.isBigInteger()) {
            // avoid redundant toString and toBigInteger, for better performance
            return jsonNode.bigIntegerValue();
        } else {
            return new BigInteger(jsonNode.asText().trim());
        }
    }

    private LocalDate convertToLocalDate(ObjectMapper mapper, JsonNode jsonNode) {
        return ISO_LOCAL_DATE.parse(jsonNode.asText()).query(TemporalQueries.localDate());
    }

    private Date convertToDate(ObjectMapper mapper, JsonNode jsonNode) {
        return Date.valueOf(convertToLocalDate(mapper, jsonNode));
    }

    private LocalDateTime convertToLocalDateTime(ObjectMapper mapper, JsonNode jsonNode) {
        // according to RFC 3339 every date-time must have a timezone;
        // until we have full timezone support, we only support UTC;
        // users can parse their time as string as a workaround
        TemporalAccessor parsedTimestamp = RFC3339_TIMESTAMP_FORMAT.parse(jsonNode.asText());

        ZoneOffset zoneOffset = parsedTimestamp.query(TemporalQueries.offset());

        if (zoneOffset != null && zoneOffset.getTotalSeconds() != 0) {
            throw new IllegalStateException(
                "Invalid timestamp format. Only a timestamp in UTC timezone is supported yet. "
                    + "Format: yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        }

        LocalTime localTime = parsedTimestamp.query(TemporalQueries.localTime());
        LocalDate localDate = parsedTimestamp.query(TemporalQueries.localDate());

        return LocalDateTime.of(localDate, localTime);
    }

    private Timestamp convertToTimestamp(ObjectMapper mapper, JsonNode jsonNode) {
        return Timestamp.valueOf(convertToLocalDateTime(mapper, jsonNode));
    }

    private LocalTime convertToLocalTime(ObjectMapper mapper, JsonNode jsonNode) {
        // according to RFC 3339 every full-time must have a timezone;
        // until we have full timezone support, we only support UTC;
        // users can parse their time as string as a workaround

        TemporalAccessor parsedTime = RFC3339_TIME_FORMAT.parse(jsonNode.asText());

        ZoneOffset zoneOffset = parsedTime.query(TemporalQueries.offset());
        LocalTime localTime = parsedTime.query(TemporalQueries.localTime());

        if (zoneOffset != null && zoneOffset.getTotalSeconds() != 0 || localTime.getNano() != 0) {
            throw new IllegalStateException(
                "Invalid time format. Only a time in UTC timezone without milliseconds is supported yet.");
        }

        return localTime;
    }

    private Time convertToTime(ObjectMapper mapper, JsonNode jsonNode) {
        return Time.valueOf(convertToLocalTime(mapper, jsonNode));
    }


    private DeserializationRuntimeConverter assembleRowConverter(
        String[] fieldNames, List<DeserializationRuntimeConverter> fieldConverters) {
        return (mapper, jsonNode) -> {
            ObjectNode node = (ObjectNode) jsonNode;
            int arity = fieldNames.length;

            // -- BEGIN WMF MODIFICATION --

            // DRY closure that converts from the field at fieldIndex using the fieldConverter at fieldIndex.
            IntFunction<Object> convertJsonField = (int fieldIndex) -> {
                String fieldName = fieldNames[fieldIndex];
                JsonNode field = node.get(fieldName);
                return convertField(mapper, fieldConverters.get(fieldIndex), fieldName, field);
            };

            // This deserializer instance is configured to deserialize Rows in one of the
            // supported RowFieldModes. Create the Row in the configured mode.
            Row row;
            switch (rowFieldMode) {
                // This is what upstream Flink 1.15.0 returns.
                case POSITION:
                    row = new Row(arity);
                    for (int i = 0; i < arity; i++) {
                        Object convertedFieldValue = convertJsonField.apply(i);
                        row.setField(i, convertedFieldValue);
                    }
                    break;

                // Named mode will allow setting additional new fields on the returned row.
                // You might want a named row if you want to augment the Row with new fields.
                case NAMED:
                    row = Row.withNames();
                    for (int i = 0; i < arity; i++) {
                        String fieldName = fieldNames[i];
                        Object convertedFieldValue = convertJsonField.apply(i);
                        row.setField(fieldName, convertedFieldValue);
                    }
                    break;

                // Named position hybrid supports both position and named based field lookups,
                // but the schema of the Row is immutable.  I.e. you cannot add new fields.
                // Make a copy or projection of the Row to do that.
                case NAMED_POSITION:
                    // For named-position mode, Row needs to be able to map from
                    // names to positions, and then from positions to fields.
                    // By providing these to the Row constructor, the Row will
                    // operate in position based mode, but allow lookups by name.
                    LinkedHashMap<String, Integer> positionByName = new LinkedHashMap<>(arity);
                    Object[] fieldByPosition = new Object[arity];

                    // Build the positionByName map and the converted fieldByPosition array.
                    for (int i = 0; i < arity; i++) {
                        String fieldName = fieldNames[i];
                        positionByName.put(fieldName, i);
                        Object convertedFieldValue = convertJsonField.apply(i);
                        fieldByPosition[i] = convertedFieldValue;
                    }

                    row = RowUtils.createRowWithNamedPositions(
                        RowKind.INSERT,
                        fieldByPosition,
                        positionByName
                    );
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + rowFieldMode);
            }
            // -- END WMF MODIFICATION --

            return row;
        };
    }



    private Object convertField(
        ObjectMapper mapper,
        DeserializationRuntimeConverter fieldConverter,
        String fieldName,
        JsonNode field) {
        if (field == null) {
            if (failOnMissingField) {
                throw new IllegalStateException(
                    "Could not find field with name '" + fieldName + "'.");
            } else {
                return null;
            }
        } else {
            return fieldConverter.convert(mapper, field);
        }
    }

    private DeserializationRuntimeConverter assembleArrayConverter(
        TypeInformation<?> elementType, DeserializationRuntimeConverter elementConverter) {

        final Class<?> elementClass = elementType.getTypeClass();

        return (mapper, jsonNode) -> {
            final ArrayNode node = (ArrayNode) jsonNode;
            final Object[] array = (Object[]) Array.newInstance(elementClass, node.size());
            for (int i = 0; i < node.size(); i++) {
                final JsonNode innerNode = node.get(i);
                array[i] = elementConverter.convert(mapper, innerNode);
            }

            return array;
        };
    }

    /** Exception which refers to parse errors in converters. */
    private static final class JsonParseException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        JsonParseException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    // -- BEGIN WMF MODIFICATION --\
    /**
     * Used to determine which of the Row field modes in which this deserializer should create Rows.
     */
    public enum RowFieldMode {
        POSITION, NAMED, NAMED_POSITION
    }
    // -- END WMF MODIFICATION --\

}
