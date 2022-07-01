package org.wikimedia.eventutilities.flink;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.function.Function.identity;
import static org.wikimedia.eventutilities.core.event.JsonEventGenerator.EVENT_TIME_FIELD;
import static org.wikimedia.eventutilities.core.event.JsonEventGenerator.META_FIELD;
import static org.wikimedia.eventutilities.core.event.JsonEventGenerator.META_ID_FIELD;
import static org.wikimedia.eventutilities.core.event.JsonEventGenerator.META_INGESTION_TIME_FIELD;
import static org.wikimedia.eventutilities.core.event.JsonEventGenerator.META_STREAM_FIELD;
import static org.wikimedia.eventutilities.core.event.JsonEventGenerator.SCHEMA_FIELD;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.types.RowUtils;

import com.google.common.collect.ImmutableSet;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.EqualsAndHashCode;

/**
 * Simple subclasss of RowTypeInfo that delegates EventStreamRowUtils methods.
 */
@EqualsAndHashCode
public class EventRowTypeInfo extends RowTypeInfo {
    public static EventRowTypeInfo create(String[] fieldNames, TypeInformation<?>... types) {
        return new EventRowTypeInfo(types, fieldNames);
    }

    public static EventRowTypeInfo create(RowTypeInfo info) {
        return new EventRowTypeInfo(info.getFieldTypes(), info.getFieldNames());
    }

    public EventRowTypeInfo(TypeInformation<?>[] types, String[] fieldNames) {
        super(types, fieldNames);
    }

    @Override
    @SuppressFBWarnings(value = "COM_COPIED_OVERRIDDEN_METHOD", justification = "not true, they are different")
    public boolean canEqual(Object obj) {
        // add canEqual manually because lombok may want to add one with another visibility
        // failing with: attempting to assign weaker access privileges; was public
        return obj instanceof EventRowTypeInfo;
    }

    /**
     * List of fields that should not be projected by default
     * when projecting a source event to a target event.
     */
    public static final Set<String> DEFAULT_NON_PROJECTED_FIELDS = ImmutableSet.of(
            META_FIELD + "." + META_ID_FIELD,
            META_FIELD + "." + META_INGESTION_TIME_FIELD,
            META_FIELD + "." + META_STREAM_FIELD,
            SCHEMA_FIELD
    );

    /**
     * Cache holding the suppliers able to create new Rows of the main Row and its subfields.
     * We cache this to avoid having to rebuild the map of positions to names on every new Row.
     * The key of this map is the path to the subfield (possibly dotted for nested subfields)
     * and null for the Row of this typeInformation.
     */
    private transient Map<String, Supplier<Row>> newRowSupplierCache;

    /**
     * A supplier that instantiates new empty rows that support position & name based access.
     *
     * NOTE: the returned supplier is Serializable.
     */
    private static Supplier<Row> createEmptyRowSupplier(TypeInformation<Row> typeInformation) {
        // Build the positionByName map only once
        checkArgument(typeInformation instanceof RowTypeInfo, "Expected a RowTypeInfo");
        final RowTypeInfo rowTypeInfo = (RowTypeInfo) typeInformation;
        final LinkedHashMap<String, Integer> positionByName = Arrays.stream(rowTypeInfo.getFieldNames())
                .collect(Collectors.toMap(identity(), rowTypeInfo::getFieldIndex,
                        (i, j) -> i, LinkedHashMap::new));
        final int arity = rowTypeInfo.getArity();

        return (Supplier<Row> & Serializable) () -> RowUtils.createRowWithNamedPositions(RowKind.INSERT, new Object[arity], positionByName);
    }

    /**
     * Creates a new empty Row matching this RowTypeInfo.
     */
    public Row createEmptyRow() {
        return createRowForFieldPath(null);
    }

    /**
     * Creates a new empty instance of a sub Row identified by fieldPath.
     *
     * This path supports dotted notation to access nested subfields.
     * For arrays or maps of Row simply pass the name of the field of the array or map.
     * @throws IllegalArgumentException if the path leads to a non-existent field or a field that is not a Row.
     */
    public Row createEmptySubRow(String fieldPath) {
        return createRowForFieldPath(fieldPath);
    }

    /**
     * Creates a new empty instance of a sub Row identified by fieldPath.
     * If fieldPath is null it creates the root Row object.
     */
    private Row createRowForFieldPath(@Nullable String fieldPath) {
        if (newRowSupplierCache == null) {
            newRowSupplierCache = new HashMap<>();
        }
        return newRowSupplierCache.computeIfAbsent(fieldPath,
                // a null key is the root
                key -> createEmptyRowSupplier(unwrapRowTypeInfo(key == null ? this : this.getTypeAt(key)))
        ).get();
    }



    /**
     * Recursively inspect MapTypeInfo of ObjectArrayTypeInfo to extract the RowTypeInfo of the leaf value.
     * @throws IllegalArgumentException if it leads to a non RowTypeInfo field.
     */
    private RowTypeInfo unwrapRowTypeInfo(TypeInformation<?> fieldTypeInfo) {
        // Extract the type information from composite types: arrays and maps
        if (fieldTypeInfo instanceof MapTypeInfo) {
            return unwrapRowTypeInfo(((MapTypeInfo<?, ?>) fieldTypeInfo).getValueTypeInfo());
        } else if (fieldTypeInfo instanceof ObjectArrayTypeInfo<?, ?>) {
            return unwrapRowTypeInfo(((ObjectArrayTypeInfo<?, ?>) fieldTypeInfo).getComponentInfo());
        }
        if (!(fieldTypeInfo instanceof RowTypeInfo)) {
            throw new IllegalArgumentException("Expected a RowTypeInfo");
        }
        return (RowTypeInfo) fieldTypeInfo;
    }

    /**
     * Project an input Row to a new Row matching the {@link TypeInformation} of this class.
     *
     * The passed Row must:
     * <ul>
     *  <li>if intersect is false be compatible with this TypeInformation and must have all the fields
     *    declared by it
     *  <li>support named based access
     * </ul>
     *
     * If intersect is true only the fields declared by both row and this TypeInformation will be copied over.
     * The use-case of this method is to support WMF schema downgrades in case a pipeline
     * reads events in schema V2 but wants to write them as a previous version
     * (only new fields are allowed during schema evolution).
     *
     * The param ignoredFields can be used to explicitly ignore some field from the projection. The content must
     * include full-path using a dot as a path separator (e.g. "meta.dt" to exclude the nested "dt" field).
     *
     * Edge-cases:
     * - when intersect is true the resulting Row might be emtpy if no common fields are present
     * - if the intersection results in an empty composite field (map, row, arrays) this field will not be projected.
     */
    public Row projectFrom(Row row, boolean intersect, Set<String> ignoredFields) {
        Row newRow = createEmptyRow();
        copyRowContent(row, newRow, this, null, intersect, ignoredFields);
        return newRow;
    }

    /**
     * Project an input Row to a new Row matching the {@link TypeInformation} of this class.
     *
     * The following data will be ignored:
     * <ul>
     * <li>all the meta field content except the request_id, domain fields
     * <li>the $schema field
     * </ul>
     *
     * @see #projectFrom(Row, boolean, Set)
     * @see #DEFAULT_NON_PROJECTED_FIELDS
     */
    public Row projectFrom(Row row, boolean intersect) {
        return projectFrom(row, intersect, DEFAULT_NON_PROJECTED_FIELDS);
    }

    /**
     * Populate the target row from the source row.
     *
     * @return false if nothing was copied (the target row remained empty)
     */
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    private boolean copyRowContent(Row source, Row target, RowTypeInfo targetTypeinfo, @Nullable String path, boolean intersect, Set<String> ignoredFields) {
        Set<String> fieldNames = source.getFieldNames(true);
        boolean empty = true;
        if (fieldNames == null) {
            throw new IllegalArgumentException("Source Row must support named based access");
        }
        for (String field: targetTypeinfo.getFieldNames()) {
            String npath = path != null ? path + "." + field : field;
            boolean sourceHasField = fieldNames.contains(field);
            // skip ignored fields
            if (ignoredFields.contains(npath) || (intersect && !sourceHasField)) {
                continue;
            }
            TypeInformation<?> targetField = targetTypeinfo.getTypeAt(field);
            if (!intersect && !sourceHasField) {
                throw new IllegalArgumentException("The source row does not support the field [" + npath + "].");
            }
            Object sourceField = source.getField(field);
            if (sourceField != null) {
                Object copiedSource = copy(sourceField, targetField, npath, intersect, ignoredFields);
                if (copiedSource != null) {
                    empty = false;
                    target.setField(field, copy(sourceField, targetField, npath, intersect, ignoredFields));
                }
            }
        }
        return !empty;
    }

    private Object copy(Object source, TypeInformation<?> typeInformation, String fieldPath, boolean intersect, Set<String> ignoredFields) {
        final Object returnValue;
        if (typeInformation instanceof RowTypeInfo) {
            Row targetSubfield = createEmptySubRow(fieldPath);
            boolean copied = copyRowContent((Row) source, targetSubfield, (RowTypeInfo) typeInformation, fieldPath, intersect, ignoredFields);
            // do not return an empty Row
            returnValue = copied ? targetSubfield : null;
        } else if (typeInformation instanceof MapTypeInfo) {
            Map<?, ?> sourceSubfield = (Map<?, ?>) source;
            TypeInformation<?> valueTypeInfo = ((MapTypeInfo<?, ?>) typeInformation).getValueTypeInfo();
            Map<Object, Object> targetSubfield = new HashMap<>();
            for (Map.Entry<?, ?> entry: sourceSubfield.entrySet()) {
                targetSubfield.put(entry.getKey(), copy(entry.getValue(), valueTypeInfo, fieldPath, intersect, ignoredFields));
            }
            returnValue = targetSubfield;
        } else if (typeInformation instanceof ObjectArrayTypeInfo) {
            ObjectArrayTypeInfo<?, ?> objectArrayTypeInfo = (ObjectArrayTypeInfo<?, ?>) typeInformation;
            Object[] sourceArray = (Object[]) source;
            Object[] targetSubfield = (Object[]) Array.newInstance(objectArrayTypeInfo.getComponentInfo().getTypeClass(), ((Object[]) source).length);
            for (int i = 0; i < sourceArray.length; i++) {
                Object sourceMember = sourceArray[i];
                if (sourceMember != null) {
                    targetSubfield[i] = copy(sourceMember, objectArrayTypeInfo.getComponentInfo(), fieldPath, intersect, ignoredFields);
                }
            }
            returnValue = targetSubfield;
        } else {
            // no transformation, we assume that everything that is not a Row, a Map or ObjectArray is immutable
            // (this is probably not true but might cover most WMF use-cases)
            returnValue = source;
        }
        return returnValue;
    }

    /**
     * Set the ingestion time (meta.dt) as per WMF event platform rules.
     *
     * @see <a href="https://wikitech.wikimedia.org/wiki/Event_Platform/Schemas/Guidelines#Required_fields">meta.dt rules</a>
     *
     * @throws IllegalArgumentException if the type information does not declare the meta field.
     */
    public void setIngestionTime(Row element, Instant ingestionTime) {
        if (!this.hasField(META_FIELD)) {
            throw new IllegalArgumentException("This TypeInformation does not declare the " + META_FIELD + " field");
        }
        Row meta = element.getFieldAs(META_FIELD);
        if (meta == null) {
            meta = createEmptySubRow(META_FIELD);
            element.setField(META_FIELD, meta);
        }
        meta.setField(META_INGESTION_TIME_FIELD, ingestionTime);
    }

    /**
     * Set the event time (dt) as per WMF event platform rules.
     *
     * @throws IllegalArgumentException if the type information does not declare the dt field
     */
    public void setEventTime(Row element, Instant eventTime) {
        element.setField(EVENT_TIME_FIELD, eventTime);
    }

    /**
     * Read the event time field.
     * May return null if the field is not set.
     */
    @Nullable
    public Instant getEventTime(Row element) {
        return element.getFieldAs(EVENT_TIME_FIELD);
    }

    /**
     * Get the ingestion time of this event if set.
     */
    public Instant getIngestionTime(Row element) {
        Row meta = element.getFieldAs(META_FIELD);
        if (meta != null) {
            return meta.getFieldAs(EVENT_TIME_FIELD);
        }
        return null;
    }
}
