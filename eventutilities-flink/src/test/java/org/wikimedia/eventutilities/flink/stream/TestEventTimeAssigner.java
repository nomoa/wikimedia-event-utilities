package org.wikimedia.eventutilities.flink.stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.LinkedHashMap;

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.types.RowUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestEventTimeAssigner {
    Instant eventTime = Instant.now();
    LinkedHashMap<String, Integer> positionByName = new LinkedHashMap<>();
    TimestampAssigner<Row> timestampAssigner = EventTimeAssigner.create();

    @BeforeEach
    void setup() {
        positionByName.put("dt", 0);
    }

    @Test
    void test_extract_time_with_string_type() {
        Row row = RowUtils.createRowWithNamedPositions(RowKind.INSERT, new Object[]{eventTime}, positionByName);
        assertThat(timestampAssigner.extractTimestamp(row, Instant.EPOCH.toEpochMilli()))
                .isEqualTo(eventTime.toEpochMilli());
    }

    @Test
    void test_extract_time_with_instant_type() {
        Row row = RowUtils.createRowWithNamedPositions(RowKind.INSERT, new Object[]{eventTime.toString()}, positionByName);
        assertThat(timestampAssigner.extractTimestamp(row, Instant.EPOCH.toEpochMilli()))
                .isEqualTo(eventTime.toEpochMilli());
    }

    @Test
    void test_extract_time_with_wrong_type() {
        Row row = RowUtils.createRowWithNamedPositions(RowKind.INSERT, new Object[]{1}, positionByName);
        assertThatThrownBy(() -> timestampAssigner.extractTimestamp(row, 1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Row records must have a dt field set with type String or Instant, [java.lang.Integer] found");
    }

    @Test
    void test_extract_time_without_field() {
        Row row = RowUtils.createRowWithNamedPositions(RowKind.INSERT, new Object[0], new LinkedHashMap<>());
        assertThatThrownBy(() -> timestampAssigner.extractTimestamp(row, 1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown field name 'dt'");
    }
}
