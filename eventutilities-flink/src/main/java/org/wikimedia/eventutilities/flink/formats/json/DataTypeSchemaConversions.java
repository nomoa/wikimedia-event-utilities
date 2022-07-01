package org.wikimedia.eventutilities.flink.formats.json;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.wikimedia.eventutilities.core.event.types.SchemaConversions;

/**
 * Implementation of {@link SchemaConversions}
 * that can convert to Flink Table API DataType.
 * Used by JsonSchemaFlinkConverter.
 *
 * If you modify this class, please also augment {@link TypeInformationSchemaConversions} accordingly.
 * To handle proper conversion between Table API and DataStream,
 * Flink needs the types consistently converted to the same underlying representations.
 */
@ParametersAreNonnullByDefault
public class DataTypeSchemaConversions implements SchemaConversions<DataType> {

    /**
     * @return
     *  {@link DataTypes#NULL}
     */
    @Override
    public DataType typeNull() {
        return DataTypes.NULL();
    }

    /**
     * @return
     *  {@link DataTypes#BOOLEAN}
     */
    @Override
    public DataType typeBoolean() {
        return DataTypes.BOOLEAN();
    }

    /**
     * @return
     *  {@link DataTypes#STRING}
     */
    @Override
    public DataType typeString() {
        return DataTypes.STRING();
    }

    /**
     * @return
     *  {@link DataTypes#DOUBLE}
     */
    @Override
    public DataType typeDecimal() {
        return DataTypes.DOUBLE();
    }

    /**
     * Note that the default conversion of LogicalType BigIntType is Long,
     * which is NOT the same as Flink Types.BIG_INT (in the DataStreamAPI).
     *
     * @return
     *  {@link DataTypes#BIGINT}
     */
    @Override
    public DataType typeInteger() {
        return DataTypes.BIGINT();
    }

    /**
     * <code>elementsAreNullable</code> is ignored; all Flink Table API elements are nullable.
     *
     * @return
     *  {@link DataTypes#ARRAY}
     */
    @Override
    public DataType typeArray(
        DataType elementType,
        boolean elementsAreNullable
    ) {
        return DataTypes.ARRAY(elementType);
    }

    /**
     * <code>valuesAreNullable</code> is ignored; all Flink Table API elements are nullable.
     *
     * @return
     *  {@link DataTypes#MAP}
     */
    @Override
    public DataType typeMap(
        DataType keyType,
        DataType valueType,
        boolean valuesAreNullable
    ) {
        return DataTypes.MAP(keyType, valueType);
    }

    /**
     * If a RowFields description is not null,
     * it will be used as the {@link DataTypes.Field}'s description.
     * RowField nullalble-ness is not relevant;
     * all Flink Table API fields are nullable.
     *
     * @return
     *  {@link DataTypes#ROW}
     */
    @Override
    public DataType typeRow(List<RowField<DataType>> rowFields) {
        List<DataTypes.Field> dataTypeFields = new ArrayList<>();

        for (RowField<DataType> rowField : rowFields) {
            dataTypeFields.add(
                buildFieldDataType(
                    rowField.getName(),
                    rowField.getType(),
                    rowField.getDescription()
                )
            );
        }

        return DataTypes.ROW(dataTypeFields);
    }

    /**
     * Helper for constructing a DataTypes.Field with name,
     * DataType, and optional description.
     */
    @Nonnull
    protected DataTypes.Field buildFieldDataType(
        String name,
        DataType dataType,
        @Nullable String description
    ) {
        if (description != null) {
            return DataTypes.FIELD(
                name,
                dataType,
                description
            );
        } else {
            return DataTypes.FIELD(
                name,
                dataType
            );
        }
    }

}
