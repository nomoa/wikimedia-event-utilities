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

# Make an EventStreamFactory that uses meta.wikimedia.org/w/api.php to get stream config.
EventStreamFactory eventStreamFactory = createMediawikiConfigEventStreamFactory(schemaBaseUris);

# Instantiate a mediawiki.revision-create EventStream.
EventStream revisionCreateStream = eventStreamFactory.createEventStream("mediawiki.revision-create");


# Get the revision-create stream's JSONSchema
ObjectNode revisionCreateSchema = revisionCreateStream.schema();

# Get the topics that make up the revision-create stream
List<string> topics = revisionCreateStream.topics()

# etc...

``` 
