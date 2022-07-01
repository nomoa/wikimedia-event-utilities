package org.wikimedia.eventutilities.flink.formats.json;

import java.time.Instant;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.wikimedia.eventutilities.core.event.types.SchemaConversions;


/**
 * Implementation of {@link SchemaConversions}
 * that can convert to Flink DataStream API TypeInformation.
 * Used by JsonSchemaFlinkConverter.
 *
 * If you modify this class, please also augment {@link DataTypeSchemaConversions} accordingly.
 * To handle proper conversion between Table API and DataStream,
 * Flink needs the types consistently converted to the same underlying representations.
 */
@ParametersAreNonnullByDefault
public class TypeInformationSchemaConversions implements SchemaConversions<TypeInformation<?>> {

    /**
     * @return
     *  {@link Types#VOID}
     */
    @Override
    public TypeInformation<Void> typeNull() {
        return Types.VOID;
    }

    /**
     * @return
     *  {@link Types#BOOLEAN}
     */
    @Override
    public TypeInformation<Boolean> typeBoolean() {
        return Types.BOOLEAN;
    }

    /**
     * @return
     *  {@link Types#STRING}
     */
    @Override
    public TypeInformation<String> typeString() {
        return Types.STRING;
    }

    /**
     * @return
     *  {@link Types#DOUBLE}
     */
    @Override
    public TypeInformation<Double> typeDecimal() {
        return Types.DOUBLE;
    }

    /**
     * @return
     *  {@link Types#LONG}
     */
    @Override
    public TypeInformation<Long> typeInteger() {
        return Types.LONG;
    }

    /**
     * There are many ways to represent timestamps.  We want to choose the representation
     * that Flink chooses when converting from {@link org.apache.flink.table.data.TimestampData}
     * to DataStream API types.
     * In {@link DataTypeSchemaConversions#typeTimestamp},
     * we use {@link org.apache.flink.table.api.DataTypes#TIMESTAMP_LTZ},
     * which will end up using org.apache.flink.table.data.conversion.LocalZonedTimestampInstantConverter
     * when doing conversions.  So, we choose {@link java.time.Instant} here.
     *
     * @return
     *  {@link Types#INSTANT}
     */
    @Override
    public TypeInformation<Instant> typeTimestamp() {
        return Types.INSTANT;
    }

    /**
     * {@code elementsAreNullable} is ignored, element values can always be null.
     * See {@link Types#OBJECT_ARRAY}.
     *
     * @return
     *  {@link Types#OBJECT_ARRAY}(elementType)
     */
    @Override
    public TypeInformation<?> typeArray(
        TypeInformation<?> elementType,
        boolean elementsAreNullable
    ) {
        return Types.OBJECT_ARRAY(elementType);
    }

    // TODO what is the generic return type here?
    /**
     * {@code valuesAreNullable} is ignored, an map values can always be null.
     * See {@link Types#MAP}.
     *
     * @return
     *  {@link Types#MAP}(keyType, valueType)
     */
    @Override
    public TypeInformation<?> typeMap(
        TypeInformation<?> keyType,
        TypeInformation<?> valueType,
        boolean valuesAreNullable
    ) {
        return Types.MAP(typeString(), valueType);
    }

    /**
     * Converts rowFields to {@link TypeInformation} of {@link Row}.
     * {@link RowField} description and isNullable is ignored.
     * Every field can be null independent of the field's type.
     * See {@link Types#ROW_NAMED}
     *
     * @return
     *  {@link Types#ROW_NAMED} with RowFields names and types.
     */
    @Override
    public TypeInformation<Row> typeRow(@Nonnull List<RowField<TypeInformation<?>>> rowFields) {
        String[] fieldNames = new String[rowFields.size()];
        TypeInformation<?>[] fieldTypes = new TypeInformation<?>[rowFields.size()];

        // extract list of names and types from the list of rowFields.
        for (int i = 0; i < rowFields.size(); i++) {
            fieldNames[i] = rowFields.get(i).getName();
            fieldTypes[i] = rowFields.get(i).getType();
        }

        return new RowTypeInfo(fieldTypes, fieldNames);
    }

}
