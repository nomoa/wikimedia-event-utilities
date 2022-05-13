
/**
 * Schema used for simple tests
 *
 * @param $schema A URI identifying the JSONSchema for this event. This should match an schema's $id in a schema repository. E.g. /schema/title/1.0.0
 * @param meta
 * @param test
 * @param test_map We want to support 'map' types using additionalProperties to specify the value types.  (Keys are always strings.) A map that has concrete properties specified is still a map. The concrete properties can be used for validation or to require that certain keys exist in the map. The concrete properties will not be considered part of the schema. The concrete properties MUST match the additionalProperties (map value) schema.
 * @param test_array Array with items schema
 */
case class test/event (
     `$schema`: String,
     meta: meta,
     test: Option[String],
     test_map: Option[test_map],
     test_array: Option[List[test_array]]
)


/**
 * @param uri Unique URI identifying the event or entity
 * @param request_id Unique ID of the request that caused the event
 * @param id Unique ID of this event
 * @param dt UTC event datetime, in ISO-8601 format
 * @param domain Domain the event or entity pertains to
 * @param stream Name of the stream/queue/dataset that this event belongs in
 */
case class meta (
     uri: Option[String],
     request_id: Option[String],
     id: Option[String],
     dt: String,
     domain: Option[String],
     stream: String
)


/**
 * We want to support 'map' types using additionalProperties to specify the value types.  (Keys are always strings.) A map that has concrete properties specified is still a map. The concrete properties can be used for validation or to require that certain keys exist in the map. The concrete properties will not be considered part of the schema. The concrete properties MUST match the additionalProperties (map value) schema.
 * 
 *
 * @param key1
 */
case class test_map (
     key1: String
)


/**
 * @param prop1 prop 1 field in complex array items
 */
case class test_array (
     prop1: Option[String]
)

