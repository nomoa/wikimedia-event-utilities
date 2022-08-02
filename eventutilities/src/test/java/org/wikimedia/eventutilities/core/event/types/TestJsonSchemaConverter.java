package org.wikimedia.eventutilities.core.event.types;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

/**
 * Basic test of JsonSchemaConverter with a Dummy SchemaConversions implementation.
 * This mostly just checks that a JsonSchema can be iterated through and call
 * the appropriate SchemaConversions methods.
 */
public class TestJsonSchemaConverter {
    private static final File jsonSchemaFile =
        new File("src/test/resources/event-schemas/repo4/test/event/1.1.0.json");

    /**
     * A nonsense SchemaConversions implementation, useful for testing only.
     */
    static final SchemaConversions<Object> TEST_SCHEMA_CONVERTERS = new SchemaConversions<Object>() {
        public Void typeNull() {
            return null;
        }

        public Boolean typeBoolean() {
            return true;
        }

        public String typeString() {
            return "string";
        }

        public Double typeDecimal() {
            return 1.0D;
        }

        public Long typeInteger() {
            return 1L;
        }

        public Instant typeTimestamp() {
            return Instant.ofEpochSecond(1546318800L);
        }

        public Map<String, Object> typeMap(Object keyType, Object valueType, boolean valuesAreNullable) {
            HashMap<String, Object> m = new HashMap<>();
            m.put("key1", "val1");
            return m;
        }

        public List<Object> typeArray(Object elementType, boolean elementsAreNullable) {
            ArrayList<Object> l = new ArrayList<>();
            l.add("element1");
            return l;
        }

        public Map<String, Object> typeRow(List<RowField<Object>> rowFields) {
            HashMap<String, Object> o = new HashMap<>();
            for (RowField<Object> rowField : rowFields) {
                o.put(rowField.getName(), rowField.getType());
            }
            return o;
        }
    };

    static Map<String, Object> expectedMap;
    static {
        Map<String, Object> meta = new HashMap<>();
        meta.put("uri", "string");
        meta.put("request_id", "string");
        meta.put("id", "string");
        meta.put("dt", Instant.ofEpochSecond(1546318800L));
        meta.put("domain", "string");
        meta.put("stream", "string");

        Map<String, Object> testMap = new HashMap<>();
        testMap.put("key1", "val1");

        List<Object> testArray = new ArrayList<>();
        testArray.add("element1");

        expectedMap = new HashMap<>();
        expectedMap.put("$schema", "string");
        expectedMap.put("meta", meta);
        expectedMap.put("test", "string");
        expectedMap.put("test_map", testMap);
        expectedMap.put("test_array", testArray);
    }

    static ObjectNode jsonSchema;
    static JsonSchemaConverter<Object> testJsonSchemaConverter;

    @BeforeAll
    public static void setUp() throws IOException {
        String jsonSchemaString = Files.asCharSource(jsonSchemaFile, Charsets.UTF_8).read();
        ObjectMapper mapper = new ObjectMapper();
        jsonSchema = (ObjectNode)mapper.readTree(jsonSchemaString);
        testJsonSchemaConverter = new JsonSchemaConverter<>(TEST_SCHEMA_CONVERTERS);
    }

    @Test
    void testConvert() {
        Map<String, Object> convertedObject = (Map<String, Object>)testJsonSchemaConverter.convert(jsonSchema);
        assertThat(convertedObject).isEqualTo(expectedMap);
    }

}
