package org.wikimedia.eventutilities.flink.formats.json;

import java.io.Serializable;

import org.wikimedia.eventutilities.flink.stream.EventDataStreamFactory;

/**
 * Strategy regarding how to assign the kafka timestamp of the output messages.
 *
 * Used by the sink function obtained via
 * {@link EventDataStreamFactory#kafkaSinkBuilder(String, String, String, String, KafkaRecordTimestampStrategy)}
 */
public enum KafkaRecordTimestampStrategy implements Serializable {
    /**
     * Use the timestamp of the flink stream record to populate the event dt field and the kafka timestamp.
     * Useful for pipeline running in event time semantics, the input Row dt field, if set, is overridden.
     * Will fail if the pipeline does not assign any timestamp.
     */
    FLINK_RECORD_EVENT_TIME,

    /**
     * Use the timestamp found in the input Row record.
     * Useful when the pipeline runs in processing time without any watermarks.
     * Will fail if the input Row does not have a dt field set.
     */
    ROW_EVENT_TIME
}
