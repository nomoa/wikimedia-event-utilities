# Wikimedia Event Utilities for Flink

Wikimedia Event Utilities for Flink is a java library for working with
event streams using Flink as a streaming processor.  It uses
`org.wikimedia:eventutilities` for managing Event Streams JSON schemas.

## Examples using the flink DataStream APIs

The EventDataStreamFactory allows to create Kafka Sinks and Sources and can be created
like this:

```java
EventDataStreamFactory eventDataStreamFactory = EventDataStreamFactory.from(
        ImmutableList.of("https://shemarepo.local/repo1", "https://shemarepo.local/repo2"),
        "https://streamconfig.wiki.local/w/api.php"
);
```

### Obtaining a KafkaSource

To obtain a KafkaSource for the stream `my_stream` reading events using the latest schema available
at the time the pipeline is constructed:

```java
// Prepare the KafkaSourceBuilder
KafkaSourceBuilder<Row> sourceBuilder = eventDataStreamFactory.kafkaSourceBuilder(
    "input-stream",
    "localhost:9092",
    "my_consumer_group"
);
// Set custom kafka options
sourceBuilder.setProperty("custom.kafka.option", "custom_value");
// Create the Source that can be attached to your StreamExecutionEnvironment
KafkaSource<Row> source = sourceBuilder.build();
DataStream<Row> dataStream = streamEnv.addSource(source);
```

This builder method will initialize the following configuration:
- bootstrap servers
- consumer group ID
- topics to consume from
- value only deserializer
- kafka offsets reset strategy set to LATEST

While some of these settings could be overridden the value deserializer should not be changed.

#### Working with event time

Flink will by default work with event time semantics and will use the kafka timestamp of the source events, if your
source does not assign the kafka timestamp or that you know that the event time is not the kafka timestamp you can still
tell flink to use the event time found in the content of source event by
using `org.wikimedia.eventutilities.flink.stream.EventTimeAssigner`:

```java
StreamExecutionEnvironment env = ...;
// Create you kafka source
KafkaSource<Row> source = this.kafkaSourceBuilder("my_stream", "localhost:9092", "consumer_group").build();

// Creates a watermark strategy that supports out of order events with 30secs of allowed lateness
WatermarkStrategy<Row> wmStrategy = WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofSeconds(30))
        // set idleness of the source, usefull to not block the stream if some sources might become idle
        .withIdleness(Duration.ofSeconds(30))
        // set the timestamp extractor that will inspect the source row to propagate its event time to the pipeline
        .withTimestampAssigner(EventTimeAssigner.create());

// Attach your source to the pipeline using the custom watermark strategy
DataStream<Row> dataStream = env.fromSource(source, wmStrategy, "my_source");
```

### Obtaining a KafkaSink

```java
DataStream<Row> dataStream = ... // The data stream

KafkaSinkBuilder<Row> sinkBuilder = eventDataStreamFactory.kafkaSinkBuilder(
        "output-stream", // the name of the stream
        "1.1.0", // the version of the schema to validate produced events
        "localhost:9092", // the list of kafka brokers (comma separated)
        "output_topic",
        KafkaRecordTimestampStrategy.FLINK_RECORD_EVENT_TIME); // how to manage the kafka timestamp and the event time

// Possibly costomize the KafkaProducer
Properties properties = new Properties();
properties.setProperty("custom.kafka.option", "custom_value");
sinkBuilder.setKafkaProducerConfig(properties);
// Build the Sink and attach it to your DataStream
KafkaSink sink = sinkBuilder.build();
dataStream.addSink(sink);
```

This builder method will initialize the following configuration:
- bootstrap servers
- the producer record serializer
- the output topic

This configuration should not be overridden on the resulting KafkaSourceBuilder.

This sink function will validate the events prior to sending to kafka using the schema defined to this stream (the
schema URI is inferred from the schema title) for the version specified.

The following fields will be managed by the sink function:
- `$schema`
- `meta.stream`
- `meta.id`
- `meta.dt`
- `dt`

Note that `meta.id` can be preset by the pipeline but `$schema`, `meta.stream`, `meta.dt` and possibly `dt` (depending on
the KafkaRecordTimestampStrategy used) are enforced (overridden) by the sink function.

#### Kafka record timestamps and event time

Defined in the enum `org.wikimedia.eventutilities.flink.formats.json.KafkaRecordTimestampStrategy` these strategies allow to
control how to treat event time and the timestamp of the produced kafka records.

- `FLINK_RECORD_EVENT_TIME` instructs the sink function to use the record timestamp of the flink records. The pipeline
  therefor must be extracting these timestamp either using the kafka source or using a custom watermark strategy with
  timestamp assigner. The sink function will fail if the record to serialize does not have any timestamp set. The
  resulting event will have its `dt` enforced to this same timestamp (if it was previously set by the pipeline it will
  be overridden).
- `ROW_EVENT_TIME` instructs the sink function to use the event time present in the Row object under the field `dt`. The
  pipeline will fail if the Row element does not have its `dt` field set.

### Managing a `Row` object

When creating events that will be produced to an event stream you will have to create new rows. In the following example
a stream of POJOs is transformed to a stream of `Row` matching the schema `1.1.0` of the stream `my_stream`.

```java
    public static class MyData {
        String title;
        Namespace namespace;

        public static class Namespace {
            String namespaceText;
            Integer namespaceId;
        }
    }

    /**
     * Transforms a stream of MyData events to a stream of Rows matching the my_stream schema at version 1.1.0
     */
    public DataStream<Row> transform(DataStream<MyData> inputStream) {
        // Obtain the type information from the schema of the my_stream stream at version 1.1.0
        EventRowTypeInfo typeInfo = eventDataStreamFactory.rowTypeInfo("my_stream", "1.1.0");

        return inputStream.map(input -> {
            // Create the base Row using the utility method
            Row r = typeInfo.createEmptyRow();
            r.setField("title", input.title);
            // Subfield should be created using the utility too.
            // The provided path supports nested subfields using a period as the separator.
            Row namespace = typeInfo.createEmptySubRow("namespace");
            namespace.setField("id", input.namespace.namespaceId);
            namespace.setField("text", input.namespace.namespaceText);
            r.setField("namespace", namespace);
            return r;
        }, typeInfo);
    }
```

`EventRowTypeInfo` can also be built from an existing `RowTypeInfo` using `EventRowTypeInfo.create(RowTypeInfo)`.

#### Projecting a Row from one schema to another
If the input data is also a Row and that the schema of the input and the output shares many common fields (same
fragments) the `projectFrom` method can be used to copy these fields for you.
The first argument is the source row to read data from and the second argument instructs whether or not
the available fields should be *intersected*, if set to:

- *true*: only the fields available in both the source and the target schema will be copied. In other words the target and
  source schema must share common fragments.
- *false*: will **fail** if the source does not have all the fields this type information expects, in other
  words the target schema must be a **subset** of the source schema.

```java
    public DataStream<Row> transform(DataStream<Row> inputStream) {
        EventRowTypeInfo outputType = eventDataStreamFactory.rowTypeInfo("my_stream", "1.1.0");
        return inputStream.map(inputRow -> outputType.projectFrom(intputRow, true), outputType);
    }
```

Using true to intersect common fields might lead to some interesting edge cases. When none of the fields are in common
the resulting Row is empty.
If the projection leads to an empty composite field (empty arrays, rows or maps) this field will be ignored, example:

Given the input schema:
```yaml
properties:
  test:
    type: string
    default: default value
  test_array:
    description: Array with items schema
    type: array
    items:
      type: object
      properties:
        prop1:
          description: prop 1 field in complex array items
          type: string
        prop2:
          description: prop 2 field in complex array items
          type: string
```

and the output schema:
```yaml
properties:
  test:
    type: string
    default: default value
  test_array:
    description: Array with items schema
    type: array
    items:
      type: object
      properties:
        prop1:
          description: prop 1 field in complex array items
          type: string
```

Projecting (note that `prop1` is not required)
```json
{"test": "value", "test_array": [{"prop2": "value"}]}
```
will produce:
```json
{"test": "value"}
```

The array member `{"prop2": "value"}` producing an empty Row is not added to the resulting array which then remains
empty and thus is not projected to the resulting event.

#### Caveats
Having a stream typed with `DataStream<Row>` might rapidly become ambiguous as you can't use your language
type system to distinguish rows of different schemas. You might be tempted to use `TypeInformation.of(Row.class)` (esp.
with the scala api that makes the TypeInformation implicit in the various DataStream operations) to hide
these differences and make your pipeline compile or even work in some cases, but you should really avoid this because you
would lose all the schema information about the type of `Row` you manipulate. It is strongly advised to only use `RowTypeInfo`
with explicit assignment in the DataStream operations.

```scala
  val dataStream: DataStream<Row> = ???
  val outputRowType = eventDataStreamFactory.rowTypeInfo("my_output", "1.1.0")

  dataStream.map(input => {
    val output = outputRowType.createEmptyRow()
    output.setField("field_lowercase", input.getFieldAs[String]("other_field").toLowerCase())
    output
  })(outputRowType)
```

## Limitations

- `Row` does not support state schema evolution so avoid storing them directly in your flink state, see [FLINK-10896](https://issues.apache.org/jira/browse/FLINK-10896).
- Position and named based access: the JSON serializers and EventStreamRowUtils functions expect a Row with named
  position. It is important to create the Row object with `EventStreamRowUtils#createInstance`
  and `EventStreamRowUtils.createSubfieldInstance(String)`. In other words avoid using `Row#withNames()` or `new Row(int)`.