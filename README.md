# Wikimedia Event Utilities

Wikimedia Event Utilities is a java library for working with
event streams as part of Wikimedia's Event Platform.  It uses
JSONSchema repositories and stream configuration to help
identify streams of events and their schemas.

## Declaring the dependency

In your project Maven `pom.xml`:
```xml
<project>
    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>org.wikimedia</groupId>
                <artifactId>eventutilities</artifactId>
                <version>1.1.0</version>
            </dependency>
        </dependencies>
    </dependencyManagement>
</project>
```

If you are NOT using `org.wikimedia.discovery:discovery-parent-pom` as a
parent, you will need to add the Wikimedia Maven repository:
```xml
<project>
    <distributionManagement>
        <repository>
            <id>archiva.releases</id>
            <name>Wikimedia Release Repository</name>
            <url>https://archiva.wikimedia.org/repository/releases/</url>
        </repository>
        <snapshotRepository>
            <id>archiva.snapshots</id>
            <name>Wikimedia Snapshot Repository</name>
            <url>https://archiva.wikimedia.org/repository/snapshots/</url>
        </snapshotRepository>
</project>
```

## Example

### Instantiate and use an EventStream using schema repo URLs and Mediawiki EventStreamConfig

```java
import org.wikimedia.eventutilities.core.event.*;
List<String> schemaBaseUris = Arrays.asList(
    "https://schema.wikimedia.org/repositories/primary/jsonschema",
    "https://schema.wikimedia.org/repositories/secondary/jsonschema"
);

// Make an EventStreamFactory that uses meta.wikimedia.org/w/api.php to get stream config,
// and a local config file to convert from event service name to event service URI.
EventStreamFactory eventStreamFactory = EventStreamFactory.builder()
    .setEventSchemaLoader(schemaBaseUris)
    .setEventStreamConfig(
        "https://meta.wikimedia.org/w/api.php?action=streamconfigs",
        "file:///path/to/event_service_uri_config.yaml"
    )
    .build();

// Instantiate a mediawiki.revision-create EventStream.
EventStream revisionCreateStream = eventStreamFactory.createEventStream("mediawiki.revision-create");

// Get the revision-create stream's JSONSchema
ObjectNode revisionCreateSchema = revisionCreateStream.schema();

// Get the topics that make up the revision-create stream
List<string> topics = revisionCreateStream.topics()

// etc...

```

### Validate an event against its schema
```java
import org.wikimedia.eventutilities.core.event.*;

String eventData = "{\"$schema\": \"/someschema/1.0.0\", ...}";
// Get the schema loader using WMF default repositories
EventSchemaLoader loader = new EventSchemaLoader(WikimediaDefaults.SCHEMA_BASE_URIS);
// Create the schema validator
EventSchemaValidator validator = new EventSchemaValidator(loader);
// validate and obtain the report, an exception is thrown if the event is not proper json
// or if there is a problem finding/loading the schema
ProcessingReport report = validator.validate(eventData);
if (report.isSuccess()) {
    logger.info("Event is valid");
} else {
    logger.error("Event is not valid: {}", report)
}
```

### Generate an event prior sending it to kafka
JVM clients might prefer to ship events directly to kafka to avoid the extra
hop through event-gate. To assist this use-case the utilities provide
JsonEventGenerator that makes sure the event has the required fields:
- `$schema` mandatory schema pointer
- `dt` optional event time
- `meta.dt` mandatory kafka-ingestion time
- `meta.stream` mandatory stream name

It also ensures the coherence of these fields by:
- validating the resulting json event against its schema
- ensuring that the stream provided matches the schema title given an EventStreamConfig

Example:
```
EventStreamConfig streamConfig = EventStreamConfig.builder()
        .setEventStreamConfigLoader(WikimediaDefaults.EVENT_STREAM_CONFIG_URI)
        .build();
JsonEventGenerator generator = JsonEventGenerator.builder()
        .schemaLoader(new EventSchemaLoader(WikimediaDefaults.SCHEMA_BASE_URIS))
        .eventStreamConfig(streamConfig)
        .withUuidSupplier(UUID::randomUUID)
        .build();

Consumer<ObjectNode> eventCreator = root -> {
    root.put("my_field", "some data");
};
Instant eventTime = Instant.EPOCH;
ObjectNode root = generator.generateEvent("my_stream", "my_schema/1.0.0", eventCreator, eventTime);
byte[] eventData = generator.serializeAsBytes(root);
// can send eventData to kafka
```

Some fields may be provided from the `eventCreator` Supplier, for instance to
better control the `meta.dt` if the producer is willing to make sure the kafka
timestamp matches `meta.dt`:
```java
Instant kafkaTimestamp = Instant.now();
Instant eventTime = Instant.EPOCH;
Consumer<ObjectNode> eventCreator = root -> {
    ObjectNode meta = root.putObject(JsonEventGenerator.META_FIELD);
    meta.put(JsonEventGenerator.META_INGESTION_TIME_FIELD, kafkaTimestamp.toString());
    root.put("my_field", "some data");
};
ObjectNode root = generator.generateEvent("my_stream", "my_schema/1.0.0", eventCreator, eventTime);
byte[] eventData = generator.serializeAsBytes(root);
// can send eventData to kafka using kafkaTimestamp as the record timestamp
```

The field `$schema` and `meta.stream` will always be overridden by JsonEventGenerator and thus cannot be set from the
`eventCreator` Supplier.

NOTE the generator is thread-safe and should be reused as it is using an
internal cache to speed-up some of the checks it is doing.
## Misc

This project is based on discovery-parent-pom. See its README for more details
on the build process and static analysis.

## Release Process

WMF builds and releases wikimedia-event-utilities using Jenkins.
A 'wikimedia-event-utilities-maven-release-docker' job has been configured and can be
scheduled at https://integration.wikimedia.org/ci/job/wikimedia-event-utilities-maven-release-docker/build
This will run the `mvn release:prepare` and `mvn release:perform` commands to bump the version number
and upload artifacts to archiva.wikimedia.org.
See also https://wikitech.wikimedia.org/wiki/Archiva
