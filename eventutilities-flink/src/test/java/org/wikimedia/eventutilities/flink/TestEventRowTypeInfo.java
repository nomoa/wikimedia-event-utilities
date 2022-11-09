package org.wikimedia.eventutilities.flink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.wikimedia.eventutilities.core.event.JsonEventGenerator.META_ID_FIELD;
import static org.wikimedia.eventutilities.core.event.JsonEventGenerator.META_INGESTION_TIME_FIELD;
import static org.wikimedia.eventutilities.core.event.JsonEventGenerator.META_STREAM_FIELD;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import com.google.common.collect.ImmutableSet;

/**
 * TestEventStreamRowUtils.
 * Shape of the test event (V1):
 * - $schema
 * - a_string: string
 * - a_int: int
 * - meta: Row
 *      - dt: string
 *      - id: string
 *      - request_id: string
 *      - stream: string
 *      - domain: string
 *      - uri: string
 * - my_subfield: Row
 *      - string_in_subfield
 * - map_type: map of Row values
 *      - string_in_subfield
 * - array_type: array of Row values
 *      - string_in_subfield
 * - complex_subfield: Row
 *      - nested_subfield: Row
 *          - string_in_subfield
 *
 * Shape of the test event (V2):
 * - $schema
 * - a_string: string
 * - a_string_v2: string
 * - a_int: int
 * - meta: Row
 *      - dt: string
 *      - id: string
 *      - request_id: string
 *      - stream: string
 *      - domain: string
 *      - uri: string
 * - my_subfield: Row
 *      - string_in_subfield
 *      - string_in_subfield2
 * - map_type: map of Row values
 *      - string_in_subfield
 *      - string_in_subfield2
 * - array_type: array of Row values
 *      - string_in_subfield
 *      - string_in_subfield_v2
 * - complex_subfield: Row
 *      - nested_subfield: Row
 *          - string_in_subfield
 *      - nested_subfield_v2: Row
 *          - string_in_subfield
 *          - string_in_subfield2
 */
class TestEventRowTypeInfo {
    /**
     * <a href="https://schema.wikimedia.org/repositories//primary/jsonschema/fragment/common/1.1.0.yaml">meta subfield schema v1.1.0</a>.
     * NOTE: the dt field is defined as String
     */
    public static final RowTypeInfo META_FIELD_TYPEINFO_V1_1_0 = new RowTypeInfo(
            new TypeInformation[]{
                TypeInformation.of(Instant.class), // Instant is not yet supported during schema conversion
                TypeInformation.of(String.class),
                TypeInformation.of(String.class),
                TypeInformation.of(String.class),
                TypeInformation.of(String.class),
                TypeInformation.of(String.class),
            },
            new String[]{
                META_INGESTION_TIME_FIELD,
                "domain",
                META_ID_FIELD,
                "request_id",
                META_STREAM_FIELD,
                "uri"
            });
    RowTypeInfo subField = new RowTypeInfo(
            new TypeInformation[]{
                TypeInformation.of(String.class)
            },
            new String[]{"string_in_subfield"}
    );
    RowTypeInfo subFieldV2 = new RowTypeInfo(
            new TypeInformation[]{
                TypeInformation.of(String.class),
                TypeInformation.of(String.class)
            },
            new String[]{"string_in_subfield", "string_in_subfield_v2"}
    );
    Object o = TypeInformation.of(Map.class);
    RowTypeInfo complexSubfield = new RowTypeInfo(
            new TypeInformation[]{
                subField,
            },
            new String[]{"nested_subfield"}
    );
    RowTypeInfo complexSubfieldV2 = new RowTypeInfo(
            new TypeInformation[]{
                subField, subFieldV2
            },
            new String[]{"nested_subfield", "nested_subfield_v2"}
    );
    RowTypeInfo rowType = new RowTypeInfo(
            new TypeInformation[]{
                TypeInformation.of(String.class),
                TypeInformation.of(String.class),
                TypeInformation.of(Integer.class),
                META_FIELD_TYPEINFO_V1_1_0,
                subField,
                new MapTypeInfo<>(TypeInformation.of(String.class), subField),
                ObjectArrayTypeInfo.getInfoFor(subField),
                complexSubfield
            },
            new String[]{"$schema", "a_string", "a_int", "meta", "my_subfield",
                "map_type", "array_type", "complex_subfield"});

    RowTypeInfo rowTypeV2 = new RowTypeInfo(
            new TypeInformation[]{
                TypeInformation.of(String.class),
                TypeInformation.of(String.class),
                TypeInformation.of(String.class),
                TypeInformation.of(Integer.class),
                META_FIELD_TYPEINFO_V1_1_0,
                subFieldV2,
                new MapTypeInfo<>(TypeInformation.of(String.class), subFieldV2),
                ObjectArrayTypeInfo.getInfoFor(subFieldV2),
                complexSubfieldV2
            },
            new String[]{"$schema", "a_string", "a_string_v2", "a_int", "meta", "my_subfield",
                "map_type", "array_type", "complex_subfield"});

    EventRowTypeInfo eventRowTypeInfo = EventRowTypeInfo.create(rowType);

    @Test
    void test_create_instance() {
        Row r = eventRowTypeInfo.createEmptyRow();

        r.setField(1, "my_string");
        assertThat(r.getField("a_string")).isEqualTo("my_string");

        r.setField("a_int", 3);
        assertThat(r.getField(2)).isEqualTo(3);

        assertThat(r.getFieldNames(true))
                .containsExactly("$schema", "a_string", "a_int", "meta", "my_subfield",
                        "map_type", "array_type", "complex_subfield");

        Row subfield = eventRowTypeInfo.createEmptySubRow("my_subfield");
        assertThat(subfield.getFieldNames(true))
                .containsExactly("string_in_subfield");

        Row mapRowEntry = eventRowTypeInfo.createEmptySubRow("map_type");
        mapRowEntry.setField("string_in_subfield", "string in a map");
        r.setField("map_type", ImmutableMap.of("entry1", mapRowEntry));
        assertThat(r.<Map<String, Row>>getFieldAs("map_type")).containsEntry("entry1", mapRowEntry);

        Row arrayRowEntry = eventRowTypeInfo.createEmptySubRow("array_type");
        arrayRowEntry.setField("string_in_subfield", "string in an array");
        r.setField("map_type", new Row[]{arrayRowEntry});
        assertThat(r.<Row[]>getFieldAs("map_type")).containsExactly(arrayRowEntry);

        Row nestedSubfield = eventRowTypeInfo.createEmptySubRow("complex_subfield.nested_subfield");
        Row complexSubfield = eventRowTypeInfo.createEmptySubRow("complex_subfield");
        nestedSubfield.setField("string_in_subfield", "string in a complex subfield");
        complexSubfield.setField("nested_subfield", nestedSubfield);
        r.setField("complex_subfield", complexSubfield);

        assertThat(r.<Row>getFieldAs("complex_subfield")
                    .<Row>getFieldAs("nested_subfield")
                    .<String>getFieldAs("string_in_subfield"))
                .isEqualTo("string in a complex subfield");
    }

    @Test
    void test_set_ingestion_date() {
        Row r = eventRowTypeInfo.createEmptyRow();

        Instant ingestionDate1 = Instant.now();
        eventRowTypeInfo.setIngestionTime(r, ingestionDate1);
        assertThat(eventRowTypeInfo.getIngestionTime(r))
                .isEqualTo(ingestionDate1);

        Instant ingestionDateOverride = ingestionDate1.plus(1, ChronoUnit.HOURS);

        eventRowTypeInfo.setIngestionTime(r, ingestionDateOverride);
        assertThat(eventRowTypeInfo.getIngestionTime(r))
                .isEqualTo(ingestionDateOverride);
    }

    @Test
    void test_set_ingestion_date_with_existing_meta_field() {
        Row r = eventRowTypeInfo.createEmptyRow();
        r.setField("meta", eventRowTypeInfo.createEmptySubRow("meta"));
        Instant ingestionDate1 = Instant.now();
        eventRowTypeInfo.setIngestionTime(r, ingestionDate1);
        assertThat(eventRowTypeInfo.getIngestionTime(r))
                .isEqualTo(ingestionDate1);
    }

    @Test
    void test_set_ingestion_date_fails_without_a_meta_field() {
        EventRowTypeInfo subfieldUtils = EventRowTypeInfo.create(subField);
        Row event = subfieldUtils.createEmptyRow();
        assertThatThrownBy(() -> subfieldUtils.setIngestionTime(event, Instant.EPOCH))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void test_is_serializable() {
        EventRowTypeInfo orig = EventRowTypeInfo.create(rowType);
        EventRowTypeInfo unser = SerializationUtils.deserialize(SerializationUtils.serialize(orig));
        assertThat(unser).isEqualTo(orig);
    }

    @Test
    void test_is_failing_when_accessing_non_row() {
        assertThatThrownBy(() -> eventRowTypeInfo.createEmptySubRow("a_string"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("RowTypeInfo");
    }

    @Test
    void test_project_from() {
        EventRowTypeInfo typeV2 = EventRowTypeInfo.create(rowTypeV2);
        EventRowTypeInfo typeV1 = EventRowTypeInfo.create(rowType);

        Row v2 = typeV2.createEmptyRow();
        Row v1 = typeV1.projectFrom(v2, false);
        assertThat(v1.getFieldNames(true))
                .containsExactly("$schema", "a_string", "a_int", "meta", "my_subfield",
                        "map_type", "array_type", "complex_subfield");

        // voluntarily omit a_string: v2.setField("a_string", "my string");
        v2.setField("a_string_v2", "my string v2");
        v2.setField("a_int", 3);
        typeV2.setIngestionTime(v2, Instant.EPOCH);
        v2.setField("my_subfield", typeV2.createEmptySubRow("my_subfield"));
        v2.<Row>getFieldAs("my_subfield").setField("string_in_subfield", "string in subfield V1");
        v2.<Row>getFieldAs("my_subfield").setField("string_in_subfield_v2", "string in subfield V2");

        Map<String, Row> map = new HashMap<>();
        Row entry = typeV2.createEmptySubRow("map_type");
        entry.setField("string_in_subfield", "string in subfield in a map");
        entry.setField("string_in_subfield_v2", "string in subfield in a map v2");
        map.put("entry1", entry);
        entry = typeV2.createEmptySubRow("map_type");
        entry.setField("string_in_subfield", "(second entry) string in subfield in a map");
        entry.setField("string_in_subfield_v2", "(second entry) string in subfield in a map v2");
        map.put("entry2", entry);
        v2.setField("map_type", map);

        Row[] array = new Row[2];
        entry = typeV2.createEmptySubRow("array_type");
        entry.setField("string_in_subfield", "string in subfield in an array");
        entry.setField("string_in_subfield_v2", "string in subfield in an array v2");
        array[0] = entry;
        entry = typeV2.createEmptySubRow("map_type");
        entry.setField("string_in_subfield", "string in subfield in an array");
        entry.setField("string_in_subfield_v2", "string in subfield in an array v2");
        array[1] = entry;
        v2.setField("array_type", array);

        Row complexSubField = typeV2.createEmptySubRow("complex_subfield");
        Row nestedField = typeV2.createEmptySubRow("complex_subfield.nested_subfield");
        nestedField.setField("string_in_subfield", "string in complex nested field");
        complexSubField.setField("nested_subfield", nestedField);
        nestedField = typeV2.createEmptySubRow("complex_subfield.nested_subfield_v2");
        nestedField.setField("string_in_subfield", "string in complex nested v2 field");
        nestedField.setField("string_in_subfield_v2", "string in complex nested v2 field V2");
        v2.setField("complex_subfield", complexSubField);

        v1 = typeV1.projectFrom(v2, false);

        // Recreate the expected V1 event
        Row expectedV1 = typeV1.createEmptyRow();
        expectedV1.setField("a_int", 3);
        // We do not set ingestion since it's not projected by default
        expectedV1.setField("my_subfield", typeV1.createEmptySubRow("my_subfield"));
        expectedV1.<Row>getFieldAs("my_subfield").setField("string_in_subfield", "string in subfield V1");

        map = new HashMap<>();
        entry = typeV1.createEmptySubRow("map_type");
        entry.setField("string_in_subfield", "string in subfield in a map");
        map.put("entry1", entry);
        entry = typeV1.createEmptySubRow("map_type");
        entry.setField("string_in_subfield", "(second entry) string in subfield in a map");
        map.put("entry2", entry);
        expectedV1.setField("map_type", map);

        array = new Row[2];
        entry = typeV1.createEmptySubRow("array_type");
        entry.setField("string_in_subfield", "string in subfield in an array");
        array[0] = entry;
        entry = typeV1.createEmptySubRow("map_type");
        entry.setField("string_in_subfield", "string in subfield in an array");
        array[1] = entry;
        expectedV1.setField("array_type", array);

        complexSubField = typeV1.createEmptySubRow("complex_subfield");
        nestedField = typeV1.createEmptySubRow("complex_subfield.nested_subfield");
        nestedField.setField("string_in_subfield", "string in complex nested field");
        complexSubField.setField("nested_subfield", nestedField);
        expectedV1.setField("complex_subfield", complexSubField);

        assertThat(v1).isEqualTo(expectedV1);
    }

    @Test
    void test_project_from_fails_with_incompatible_schema() {
        EventRowTypeInfo sourceType = EventRowTypeInfo.create(subField);
        EventRowTypeInfo targetType = EventRowTypeInfo.create(subFieldV2);
        Row event = sourceType.createEmptyRow();

        assertThatThrownBy(() -> targetType.projectFrom(event, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The source row does not support the field [string_in_subfield_v2].");
    }

    @Test
    void test_project_from_fails_with_position_based_row() {
        EventRowTypeInfo targetType = EventRowTypeInfo.create(subFieldV2);
        Row r = new Row(2);
        assertThatThrownBy(() -> targetType.projectFrom(r, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Source Row must support named based access");
    }

    @Test
    void test_project_from_can_intersect() {
        EventRowTypeInfo sourceType = EventRowTypeInfo.create(subField);
        EventRowTypeInfo targetType = EventRowTypeInfo.create(subFieldV2);
        Row sourceRow = sourceType.createEmptyRow();
        sourceRow.setField("string_in_subfield", "my string");
        Row targetRow = targetType.projectFrom(sourceRow, true);
        assertThat(targetRow.getFieldNames(true))
                .containsExactly("string_in_subfield", "string_in_subfield_v2");
        assertThat(targetRow.<String>getFieldAs("string_in_subfield")).isEqualTo("my string");
    }

    @Test
    void test_default_projected_fields() {
        Row source = eventRowTypeInfo.createEmptyRow();
        eventRowTypeInfo.setIngestionTime(source, Instant.EPOCH);
        source.setField("$schema", "my_schema");
        Row meta = source.getFieldAs("meta");
        meta.setField("request_id", "the request id");
        meta.setField("id", UUID.randomUUID().toString());
        meta.setField("domain", "my.domain");
        meta.setField("uri", "event uri (obsolete)");
        meta.setField("stream", "the source stream");
        source.setField("$schema", "my_schema");

        Row target = eventRowTypeInfo.projectFrom(source, false);
        assertThat(target.getFieldNames(true)).containsExactlyElementsOf(source.getFieldNames(true));
        assertThat(target.getField("$schema")).isNull();
        Row targetMeta = target.getFieldAs("meta");
        assertThat(targetMeta.getField("request_id")).isEqualTo("the request id");
        assertThat(targetMeta.getField("domain")).isEqualTo("my.domain");
        assertThat(targetMeta.getField("uri")).isEqualTo("event uri (obsolete)");
        assertThat(targetMeta.getField("id")).isNull();
        assertThat(targetMeta.getField("dt")).isNull();
        assertThat(targetMeta.getField("stream")).isNull();
    }

    @Test
    void test_custom_non_projected_fields() {
        Row source = eventRowTypeInfo.createEmptyRow();
        eventRowTypeInfo.setIngestionTime(source, Instant.EPOCH);
        source.setField("$schema", "my_schema");
        Row meta = source.getFieldAs("meta");
        meta.setField("request_id", "the request id");
        meta.setField("id", "my_id");
        meta.setField("domain", "my.domain");
        meta.setField("uri", "event uri (obsolete)");
        meta.setField("stream", "the source stream");
        source.setField("$schema", "my_schema");

        Row target = eventRowTypeInfo.projectFrom(source, false, ImmutableSet.of("meta.request_id", "meta.uri"));
        assertThat(target.getFieldNames(true)).containsExactlyElementsOf(source.getFieldNames(true));
        assertThat(target.getField("$schema")).isEqualTo("my_schema");
        Row targetMeta = target.getFieldAs("meta");
        assertThat(targetMeta.getField("request_id")).isNull();
        assertThat(targetMeta.getField("domain")).isEqualTo("my.domain");
        assertThat(targetMeta.getField("uri")).isNull();
        assertThat(targetMeta.getField("id")).isEqualTo("my_id");
        assertThat(eventRowTypeInfo.getIngestionTime(target))
                .isEqualTo(Instant.EPOCH);
        assertThat(targetMeta.getField("stream")).isEqualTo("the source stream");
    }
}
