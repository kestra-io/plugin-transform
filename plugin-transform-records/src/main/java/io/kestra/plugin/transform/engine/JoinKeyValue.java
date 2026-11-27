package io.kestra.plugin.transform.engine;

import com.amazon.ion.IonBool;
import com.amazon.ion.IonDecimal;
import com.amazon.ion.IonFloat;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonString;
import com.amazon.ion.IonTimestamp;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import io.kestra.plugin.transform.util.JoinMessages;

import java.util.Objects;

/**
 * Key value shared by all join implementations, so that they match the same rows: decimals are compared by value
 * (1.0 = 1.00), timestamps by instant (whatever their offset or precision), and values of different types never match.
 */
public final class JoinKeyValue implements Comparable<JoinKeyValue> {

    private final IonType type;
    private final Comparable<?> value;

    private JoinKeyValue(IonType type, Comparable<?> value) {
        this.type = type;
        this.value = value;
    }

    /** The Ion value must not be null, and its type must be one of the processable key types. */
    static JoinKeyValue of(IonValue ionValue) {
        Comparable<?> normalized = switch (ionValue.getType()) {
            case BOOL -> ((IonBool) ionValue).booleanValue();
            case INT -> ((IonInt) ionValue).bigIntegerValue();
            case FLOAT -> ((IonFloat) ionValue).doubleValue();
            case DECIMAL -> ((IonDecimal) ionValue).decimalValue().stripTrailingZeros();
            case STRING -> ((IonString) ionValue).stringValue();
            case TIMESTAMP -> ((IonTimestamp) ionValue).timestampValue().getDecimalMillis().stripTrailingZeros();
            default -> throw new IllegalArgumentException(JoinMessages.UNPROCESSABLE_COMPARED_TYPES);
        };
        return new JoinKeyValue(ionValue.getType(), normalized);
    }

    public IonType getType() {
        return type;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof JoinKeyValue key && type == key.type && value.equals(key.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, value);
    }

    /** Keys of different types cannot be ordered: SortedMergeJoin checks the types before sorting. */
    @Override
    @SuppressWarnings("unchecked")
    public int compareTo(JoinKeyValue other) {
        if (type != other.type) {
            throw new IllegalStateException(String.format(JoinMessages.DIFFERENT_TYPES, type, other.type));
        }
        return ((Comparable<Object>) value).compareTo(other.value);
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
