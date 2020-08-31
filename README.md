# Wikimedia Event Utilities

Wikimedia Event Utilities is a java library for working with 
event streams as part of Wikimedia's Event Platform.  It uses
JSONSchema repositories and stream configuration to help
identify streams of events and their schemas. 

## Example

Instantiate and use an EventStream using schema repo URLs and Mediawiki EventStreamConfig

```java
import org.wikimedia.eventutilities.core.event.*;
List<String> schemaBaseUris = Arrays.asList(
    "https://schema.wikimedia.org/repositories/primary/jsonschema",
    "https://schema.wikimedia.org/repositories/secondary/jsonschema"
);

# Make an EventStreamFactory that uses meta.wikimedia.org/w/api.php to get stream config,
# and a local config file to convert from event service name to event service URI.
EventStreamFactory eventStreamFactory = EventStreamFactory.builder()
    .setEventSchemaLoader(schemaBaseUris)
    .setEventStreamConfig(
        "https://meta.wikimedia.org/w/api.php?action=streamconfigs",
        "file:///path/to/event_service_uri_config.yaml"
    )
    .build();

# Instantiate a mediawiki.revision-create EventStream.
EventStream revisionCreateStream = eventStreamFactory.createEventStream("mediawiki.revision-create");

# Get the revision-create stream's JSONSchema
ObjectNode revisionCreateSchema = revisionCreateStream.schema();

# Get the topics that make up the revision-create stream
List<string> topics = revisionCreateStream.topics()

# etc...

``` 

## Misc

This project is based on discovery-parent-pom. See its README for more details
on the build process and static analysis.