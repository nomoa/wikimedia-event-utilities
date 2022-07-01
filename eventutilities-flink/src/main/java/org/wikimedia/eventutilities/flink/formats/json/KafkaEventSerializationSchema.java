package org.wikimedia.eventutilities.flink.formats.json;

import java.time.Instant;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.wikimedia.eventutilities.core.SerializableClock;
import org.wikimedia.eventutilities.flink.EventRowTypeInfo;

/**
 * This class is designed to produce events compliant with the WMF Event Platform.
 */
@ParametersAreNonnullByDefault
public class KafkaEventSerializationSchema implements KafkaRecordSerializationSchema<Row> {
    private final String topic;
    @Nullable
    private final FlinkKafkaPartitioner<Row> partitioner;
    private final SerializationSchema<Row> serializationSchema;

    private final  SerializableClock ingestionTimeClock;

    private final EventRowTypeInfo eventRowTypeInfo;
    private final KafkaRecordTimestampStrategy timestampStrategy;

    public KafkaEventSerializationSchema(
            RowTypeInfo typeInformation,
            SerializationSchema<Row> serializationSchema,
            SerializableClock ingestionTimeClock,
            String topic,
            KafkaRecordTimestampStrategy timestampStrategy,
            @Nullable FlinkKafkaPartitioner<Row> partitioner
    ) {
        this.timestampStrategy = timestampStrategy;
        this.topic = topic;
        this.partitioner = partitioner;
        this.serializationSchema = serializationSchema;
        // Ideally we'd like to use the ProcessingTimeService, but it does not seem trivial to access it from here
        this.ingestionTimeClock = ingestionTimeClock;
        this.eventRowTypeInfo = typeInformation instanceof EventRowTypeInfo ? (EventRowTypeInfo) typeInformation : EventRowTypeInfo.create(typeInformation);
    }

    @Override
    public void open(SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext) {
        if (partitioner != null) {
            partitioner.open(sinkContext.getParallelInstanceId(), sinkContext.getNumberOfParallelInstances());
        }
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Row element, KafkaSinkContext context, @Nullable Long timestamp) {
        eventRowTypeInfo.setIngestionTime(element, ingestionTimeClock.get());
        Instant kafkaTimestamp;
        switch (timestampStrategy) {
            case ROW_EVENT_TIME:
                kafkaTimestamp = eventRowTypeInfo.getEventTime(element);
                if (kafkaTimestamp == null) {
                    throw new IllegalArgumentException("The row " + element + " does not have its event time set");
                }
                break;
            case FLINK_RECORD_EVENT_TIME:
                if (timestamp == null) {
                    throw new IllegalArgumentException("The stream record timestamp is null, " +
                            "the pipeline must set record timestamps");
                }
                kafkaTimestamp = Instant.ofEpochMilli(timestamp);
                eventRowTypeInfo.setEventTime(element, kafkaTimestamp);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported timestamp strategy: " + timestampStrategy);
        }
        byte[] messageBody = serializationSchema.serialize(element);
        Integer partition = partitioner != null ? partitioner.partition(element, null, messageBody, topic, context.getPartitionsForTopic(topic)) : null;
        return new ProducerRecord<>(topic, partition, kafkaTimestamp.toEpochMilli(), null, messageBody);
    }
}
