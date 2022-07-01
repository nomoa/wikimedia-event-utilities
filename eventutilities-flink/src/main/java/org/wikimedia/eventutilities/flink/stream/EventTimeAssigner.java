package org.wikimedia.eventutilities.flink.stream;

import java.time.Instant;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.types.Row;
import org.wikimedia.eventutilities.core.event.JsonEventGenerator;

/**
 * Creates a flink TimestampAssigner that extracts the event time.
 */
public final class EventTimeAssigner {
    private EventTimeAssigner() {}

    /**
     * A timestamp extractor that reads the event time field from Event Platform events.
     */
    public static SerializableTimestampAssigner<Row> create() {
        return (element, recordTimestamp) -> {
            // TODO: add a way to configure the field name instead of hardcoding EVENT_TIME_FIELD here
            Object eventTime = element.getField(JsonEventGenerator.EVENT_TIME_FIELD);
            // We have to support both String and Instant as currently the conversion from JSONSchema to data types
            // does not support parsing dates.
            if (eventTime == null) {
                throw new IllegalArgumentException("Event time field 'dt' not set");
            }
            if (eventTime instanceof String) {
                return Instant.parse((String) eventTime).toEpochMilli();
            } else if (eventTime instanceof Instant) {
                return ((Instant) eventTime).toEpochMilli();
            } else {
                throw new IllegalArgumentException("Row records must have a dt field set with " +
                        "type String or Instant, [" + eventTime.getClass().getName() + "] found");
            }
        };
    }
}
