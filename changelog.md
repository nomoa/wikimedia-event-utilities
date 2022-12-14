## 1.2.0
- Refactor JsonSchemaConverter to allow for pluggable schema conversions.
- Implement a JSONSchema conversion to Flink TypeInformation<Row>
- Copy Flink upstream JsonRowDeserializationSchema
- Add EventDataStreamFactory to aid in getting DataStream<Row> of WMF Event Platform streams

## 1.1.0
- Add flink module
- Add JsonSchemaConverter for Flink Table API schema conversion
- Add EventTableDescriptorBuilder to aid in constructing Flink Tables from Wikimedia EventStreams
- Add builder helper methods to EventStreamFactory for easier use
- Add WikimediaExternalDefaults for testing in repl outside WMF production

## 1.0.9
- create assembly jar-with-dependencies

## 1.0.8
- Move from log4j-2 to slf4j

## 1.0.7
- Support filtering EventStreamConfig by JsonPointer path
- BasicHttpClient needs to close the underlying HttpClient.
- Allow using EventStreamConfig without providing an eventServiceToUriMap

## 1.0.6
- Fix NPE in BasicHttpResult getBodyAsString

## 1.0.5
- Fix for BasicHttpClient so that a non 2xx response will fail when used with ResourceLoader

## 1.0.4
- Add ResourceLoader and BasicHttpClient

## 1.0.3
- Add JsonEventGenerator
- Fix few api.php URI for meta.wikimedia.org
- Add an utility class to validate an event against its schema
