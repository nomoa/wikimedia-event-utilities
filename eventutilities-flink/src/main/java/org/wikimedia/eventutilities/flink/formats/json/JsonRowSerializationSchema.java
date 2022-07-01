package org.wikimedia.eventutilities.flink.formats.json;


import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static org.apache.flink.formats.common.TimeFormats.RFC3339_TIMESTAMP_FORMAT;
import static org.apache.flink.formats.common.TimeFormats.RFC3339_TIME_FORMAT;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.WrappingRuntimeException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.EqualsAndHashCode;

/**
 * Serialization schema that serializes an object of Flink types into a JSON bytes.
 *
 * <p>Serializes the input Flink object into a JSON string and converts it into <code>byte[]</code>.
 *
 * <p>Result <code>byte[]</code> messages can be deserialized using {@link
 * JsonRowDeserializationSchema}.
 * (Copied from the flink code-base and adapted)
 */
// Suppress all spotbugs warnings for this class, as it was copy/pasted from upstream Flink.
@SuppressFBWarnings
@SuppressWarnings({"checkstyle:ClassFanoutComplexity", "checkstyle:CyclomaticComplexity"})
@EqualsAndHashCode
public final class JsonRowSerializationSchema implements SerializationSchema<Row> {

    private static final long serialVersionUID = -2885556750743978636L;

    /** Type information describing the input type. */
    private final RowTypeInfo typeInfo;

    /** Event generator responsible for fetching the schema and validating the event against it. */
    private final Function<Consumer<ObjectNode>, ObjectNode> normalization;

    private final SerializationRuntimeConverter runtimeConverter;

    private final ObjectMapper mapper;

    private JsonRowSerializationSchema(TypeInformation<Row> typeInfo, Function<Consumer<ObjectNode>, ObjectNode> normalization, ObjectMapper mapper) {
        checkNotNull(typeInfo, "Type information");
        checkArgument(
                typeInfo instanceof RowTypeInfo, "Only RowTypeInfo is supported");
        this.typeInfo = (RowTypeInfo) typeInfo;
        this.runtimeConverter = createConverter(typeInfo);
        this.normalization = normalization;
        this.mapper = mapper;
    }

    /**
     * The type information supported by this serializer.
     */
    public RowTypeInfo getTypeInformation() {
        return typeInfo;
    }

    /** Builder for {@link JsonRowSerializationSchema}. */
    @PublicEvolving
    public static final class Builder {

        private RowTypeInfo typeInfo;
        private Function<Consumer<ObjectNode>, ObjectNode> normalization;
        private ObjectMapper mapper;
        private Builder() {
        }

        /**
         * Sets type information for JSON serialization schema.
         *
         * @param typeInfo Type information describing the result type. The field names of {@link
         *     Row} are used to parse the JSON properties.
         */
        public Builder withTypeInfo(@Nonnull TypeInformation<Row> typeInfo) {
            checkArgument(
                    typeInfo instanceof RowTypeInfo, "Only SchemaAwareRowTypeInfo is supported");
            this.typeInfo = (RowTypeInfo) typeInfo;
            return this;
        }

        /**
         * The normalization function to apply on the provided event data.
         */
        public Builder withNormalizationFunction(Function<Consumer<ObjectNode>, ObjectNode> normalization) {
            this.normalization = checkNotNull(normalization);
            return this;
        }

        /**
         * Do not apply any normalization to serialized events.
         * Should never be used for producing events to the WMF Event Platform.
         */
        public Builder withoutNormalization() {
            if (mapper == null) {
                mapper = new ObjectMapper();
            }
            this.normalization = (Function<Consumer<ObjectNode>, ObjectNode> & Serializable) (c) -> {
                ObjectNode node = mapper.createObjectNode();
                c.accept(node);
                return node;
            };
            return this;
        }

        public Builder withObjectMapper(ObjectMapper mapper) {
            this.mapper = checkNotNull(mapper);
            return this;
        }

        /**
         * Finalizes the configuration and checks validity.
         *
         * @return Configured {@link JsonRowSerializationSchema}
         */
        public JsonRowSerializationSchema build() {
            checkNotNull(typeInfo, "typeInfo should be set.");
            checkNotNull(normalization, "A normalization method must be explicitly set.");

            return new JsonRowSerializationSchema(typeInfo, normalization, mapper != null ? mapper : new ObjectMapper());
        }
    }

    /** Creates a builder for {@link JsonRowSerializationSchema.Builder}. */
    public static Builder builder() {
        return new Builder();
    }

    @SuppressWarnings("checkstyle:IllegalCatch")
    @Override
    public byte[] serialize(Row row) {
        try {
            ObjectNode event = normalization.apply(root -> runtimeConverter.convert(mapper, root, row));
            return mapper.writeValueAsBytes(event);
        } catch (Exception t) {
            throw new RuntimeException(
                    "Could not serialize row '"
                            + row
                            + "'. "
                            + "Make sure that the schema matches the input.",
                    t);
        }
    }

    /*
    Runtime converters
    */

    /** Runtime converter that maps between Java objects and corresponding {@link JsonNode}s. */
    @FunctionalInterface
    private interface SerializationRuntimeConverter extends Serializable {
        JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object object);
    }

    private SerializationRuntimeConverter createConverter(TypeInformation<?> typeInfo) {
        // -- BEGIN WMF MODIFICATION --
        // dropped nullNode converter
        return createConverterForSimpleType(typeInfo)
                .orElseGet(
                        () ->
                                createContainerConverter(typeInfo)
                                        .orElseGet(this::createFallbackConverter));
        // -- END WMF MODIFICATION --
    }

    private Optional<SerializationRuntimeConverter> createContainerConverter(
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
            return Optional.of(
                    (mapper, reuse, object) -> mapper.getNodeFactory().binaryNode((byte[]) object));
        } else if (typeInfo instanceof MapTypeInfo) {
            return createMapConverter((MapTypeInfo<?, ?>) typeInfo);
        } else {
            // Should we fail?
            return Optional.empty();
        }
    }

    private Optional<SerializationRuntimeConverter> createMapConverter(MapTypeInfo<?, ?> typeInfo) {
        if (!typeInfo.getKeyTypeInfo().getTypeClass().isAssignableFrom(String.class)) {
            throw new IllegalArgumentException("Map types must have String keys");
        }
        SerializationRuntimeConverter mapValueConverter = createConverter(typeInfo.getValueTypeInfo());
        return Optional.of((mapper, reuse, object) -> {
            Map<String, ?> map = (Map<String, ?>) object;
            ObjectNode node = reuse != null ? (ObjectNode) reuse : mapper.createObjectNode();
            map.forEach((String k, Object mapValue) -> {
                if (mapValue != null) {
                    node.set(k, mapValueConverter.convert(mapper, null, mapValue));
                }
            });
            return node;
        });
    }

    private boolean isPrimitiveByteArray(TypeInformation<?> typeInfo) {
        return typeInfo instanceof PrimitiveArrayTypeInfo
                && ((PrimitiveArrayTypeInfo) typeInfo).getComponentType() == Types.BYTE;
    }

    private SerializationRuntimeConverter createObjectArrayConverter(
            TypeInformation elementTypeInfo) {
        SerializationRuntimeConverter elementConverter = createConverter(elementTypeInfo);
        return assembleArrayConverter(elementConverter);
    }

    private SerializationRuntimeConverter createRowConverter(RowTypeInfo typeInfo) {
        List<SerializationRuntimeConverter> fieldConverters =
                Arrays.stream(typeInfo.getFieldTypes())
                        .map(this::createConverter)
                        .collect(Collectors.toList());

        return assembleRowConverter(typeInfo.getFieldNames(), fieldConverters);
    }

    private SerializationRuntimeConverter createFallbackConverter() {
        return (mapper, reuse, object) -> {
            // for types that were specified without JSON schema
            // e.g. POJOs
            try {
                return mapper.valueToTree(object);
            } catch (IllegalArgumentException e) {
                throw new WrappingRuntimeException(
                        format(Locale.ROOT, "Could not convert object: %s", object), e);
            }
        };
    }

    private Optional<SerializationRuntimeConverter> createConverterForSimpleType(
            TypeInformation<?> simpleTypeInfo) {
        if (simpleTypeInfo == Types.VOID) {
            return Optional.of((mapper, reuse, object) -> mapper.getNodeFactory().nullNode());
        } else if (simpleTypeInfo == Types.BOOLEAN) {
            return Optional.of(
                    (mapper, reuse, object) ->
                            mapper.getNodeFactory().booleanNode((Boolean) object));
        } else if (simpleTypeInfo == Types.STRING) {
            return Optional.of(
                    (mapper, reuse, object) -> mapper.getNodeFactory().textNode((String) object));
        } else if (simpleTypeInfo == Types.INT) {
            return Optional.of(
                    (mapper, reuse, object) ->
                            mapper.getNodeFactory().numberNode((Integer) object));
        } else if (simpleTypeInfo == Types.LONG) {
            return Optional.of(
                    (mapper, reuse, object) -> mapper.getNodeFactory().numberNode((Long) object));
        } else if (simpleTypeInfo == Types.DOUBLE) {
            return Optional.of(
                    (mapper, reuse, object) -> mapper.getNodeFactory().numberNode((Double) object));
        } else if (simpleTypeInfo == Types.FLOAT) {
            return Optional.of(
                    (mapper, reuse, object) -> mapper.getNodeFactory().numberNode((Float) object));
        } else if (simpleTypeInfo == Types.SHORT) {
            return Optional.of(
                    (mapper, reuse, object) -> mapper.getNodeFactory().numberNode((Short) object));
        } else if (simpleTypeInfo == Types.BYTE) {
            return Optional.of(
                    (mapper, reuse, object) -> mapper.getNodeFactory().numberNode((Byte) object));
        } else if (simpleTypeInfo == Types.BIG_DEC) {
            return Optional.of(createBigDecimalConverter());
        } else if (simpleTypeInfo == Types.BIG_INT) {
            return Optional.of(createBigIntegerConverter());
        } else if (simpleTypeInfo == Types.SQL_DATE) {
            return Optional.of(this::convertDate);
        } else if (simpleTypeInfo == Types.SQL_TIME) {
            return Optional.of(this::convertTime);
        } else if (simpleTypeInfo == Types.SQL_TIMESTAMP) {
            return Optional.of(this::convertTimestamp);
        } else if (simpleTypeInfo == Types.LOCAL_DATE) {
            return Optional.of(this::convertLocalDate);
        } else if (simpleTypeInfo == Types.LOCAL_TIME) {
            return Optional.of(this::convertLocalTime);
        } else if (simpleTypeInfo == Types.LOCAL_DATE_TIME) {
            return Optional.of(this::convertLocalDateTime);
        // -- BEGIN WMF MODIFICATION --
        } else if (simpleTypeInfo == Types.INSTANT) {
            return Optional.of(this::convertInstant);
        // -- END WMF MODIFICATION --
        } else {
            return Optional.empty();
        }
    }

    private JsonNode convertLocalDate(ObjectMapper mapper, JsonNode reuse, Object object) {
        return mapper.getNodeFactory().textNode(ISO_LOCAL_DATE.format((LocalDate) object));
    }

    private JsonNode convertDate(ObjectMapper mapper, JsonNode reuse, Object object) {
        Date date = (Date) object;
        return convertLocalDate(mapper, reuse, date.toLocalDate());
    }

    private JsonNode convertLocalDateTime(ObjectMapper mapper, JsonNode reuse, Object object) {
        return mapper.getNodeFactory()
                .textNode(RFC3339_TIMESTAMP_FORMAT.format((LocalDateTime) object));
    }

    // -- BEGIN WMF MODIFICATION --
    private JsonNode convertInstant(ObjectMapper mapper, JsonNode reuse, Object object) {
        Instant instant = (Instant) object;
        return convertLocalDateTime(mapper, reuse, instant.atZone(ZoneOffset.UTC).toLocalDateTime());
    }
    // -- END WMF MODIFICATION --

    private JsonNode convertTimestamp(ObjectMapper mapper, JsonNode reuse, Object object) {
        Timestamp timestamp = (Timestamp) object;
        return convertLocalDateTime(mapper, reuse, timestamp.toLocalDateTime());
    }

    private JsonNode convertLocalTime(ObjectMapper mapper, JsonNode reuse, Object object) {
        JsonNodeFactory nodeFactory = mapper.getNodeFactory();
        return nodeFactory.textNode(RFC3339_TIME_FORMAT.format((LocalTime) object));
    }

    private JsonNode convertTime(ObjectMapper mapper, JsonNode reuse, Object object) {
        final Time time = (Time) object;
        return convertLocalTime(mapper, reuse, time.toLocalTime());
    }

    private SerializationRuntimeConverter createBigDecimalConverter() {
        return (mapper, reuse, object) -> {
            // convert decimal if necessary
            JsonNodeFactory nodeFactory = mapper.getNodeFactory();
            if (object instanceof BigDecimal) {
                return nodeFactory.numberNode((BigDecimal) object);
            }
            return nodeFactory.numberNode(BigDecimal.valueOf(((Number) object).doubleValue()));
        };
    }

    private SerializationRuntimeConverter createBigIntegerConverter() {
        return (mapper, reuse, object) -> {
            // convert decimal if necessary
            JsonNodeFactory nodeFactory = mapper.getNodeFactory();
            if (object instanceof BigInteger) {
                return nodeFactory.numberNode((BigInteger) object);
            }
            return nodeFactory.numberNode(BigInteger.valueOf(((Number) object).longValue()));
        };
    }

    private SerializationRuntimeConverter assembleRowConverter(
            String[] fieldNames, List<SerializationRuntimeConverter> fieldConverters) {
        return (mapper, reuse, object) -> {

            ObjectNode node = reuse != null ? (ObjectNode) reuse : mapper.createObjectNode();
            Row row = (Row) object;

            for (int i = 0; i < fieldNames.length; i++) {
                String fieldName = fieldNames[i];
                Object rowValue = row.getField(i);
                if (rowValue != null) {
                    node.set(fieldName,
                            fieldConverters.get(i).convert(mapper, null, rowValue));
                }
            }

            return node;
        };
    }

    private SerializationRuntimeConverter assembleArrayConverter(
            SerializationRuntimeConverter elementConverter) {
        return (mapper, reuse, object) -> {
            ArrayNode node;

            // reuse could be a NullNode if last record is null.
            if (reuse == null || reuse.isNull()) {
                node = mapper.createArrayNode();
            } else {
                node = (ArrayNode) reuse;
                node.removeAll();
            }

            if (object instanceof List) {
                object = ((List<?>) object).toArray();
            }
            Object[] array = (Object[]) object;

            for (Object element : array) {
                node.add(elementConverter.convert(mapper, null, element));
            }

            return node;
        };
    }
}
