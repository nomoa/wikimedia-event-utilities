package org.wikimedia.eventutilities.core.event.types;

import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

/**
 * Interface for supported WMF Event Schema type conversions.
 * This is used to map to a particular schema container, e.g. Flink DataTypes or Spark DataTypes.
 *
 * An implementation should return the appropriate T for the given method.
 * E.g. Flink DataType: typeString() to DataTypes.STRING(), or Spark: typeString() to DataTypes.StringType
 *
 * @param <T>
 *     Parent schema type.
 *     E.g. Flink DataType or TypeInformation, or Spark DataType.
 *
 * TO DO: Implement support for String to Timestamp conversion.
 * https://phabricator.wikimedia.org/T310495
 * https://phabricator.wikimedia.org/T278467
 */
@ParametersAreNonnullByDefault
public interface SchemaConversions<T> {

    /**
     * Returns type that represents null.
     */
    @Nonnull
    T typeNull();

    /**
     * Returns type that represents boolean.
     */
    @Nonnull
    T typeBoolean();

    /**
     * Returns type that represents String.
     */
    @Nonnull
    T typeString();

    /**
     * Returns type that represents integer.
     */
    @Nonnull
    T typeInteger();

    /**
     * Returns type that represents decimal.
     */
    @Nonnull
    T typeDecimal();

    /**
     * Returns type that represents Map.
     *
     * @param keyType
     *  type of the Map keys.
     *
     * @param valueType
     *  type of the Map values.
     *
     * @param valuesAreNullable
     *  Whether Map values are possibly null.
     */
    @Nonnull
    T typeMap(T keyType, T valueType, boolean valuesAreNullable);

    /**
     * Returns type that represents Array.
     *
     * @param elementType
     *  type of the Array elements.
     *
     * @param elementsAreNullable
     *  Whether Array elements are possibly null.
     */
    @Nonnull
    T typeArray(T elementType, boolean elementsAreNullable);

    /**
     * Returns type that represents a row AKA a struct AKA a JSON object.
     * Rows have strictly defined field names and types.
     * A RowField may or may not be nullable, and it may have a description.
     *
     * @param rowFields
     *  List of {@link RowField}s in this row.  Use these to construct your row type.
     */
    @Nonnull
    T typeRow(List<RowField<T>> rowFields);


    /**
     * Data Transfer Object wrapper for a 'row field' type.
     * Only used for representing a field in a row that is passed
     * to a {@link SchemaConversions#typeRow} implementation.
     */
    class RowField<T> {

        private final @Nonnull
        String name;
        private final @Nonnull T type;
        private final boolean isNullable;
        private final @Nullable
        String description;

        /**
         * @param name
         *  Field name.
         *
         * @param type
         *  Field type, should be compatible with the containing row type.
         *
         * @param isNullable
         *  If the field should be nullable.
         *
         * @param description
         *  Optional field description or comment.
         */
        public RowField(
            @Nonnull String name,
            @Nonnull T type,
            boolean isNullable,
            @Nullable String description
        ) {
            this.name = name;
            this.type = type;
            this.isNullable = isNullable;
            this.description = description;
        }

        @Nonnull
        public String getName() {
            return name;
        }

        @Nonnull
        public T getType() {
            return type;
        }

        public boolean getIsNullable() {
            return isNullable;
        }

        @Nullable
        public String getDescription() {
            return description;
        }

    }
}
