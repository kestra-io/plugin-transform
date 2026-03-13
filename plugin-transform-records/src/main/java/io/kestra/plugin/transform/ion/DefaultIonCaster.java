package io.kestra.plugin.transform.ion;

import com.amazon.ion.IonList;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonValue;
import com.amazon.ion.Timestamp;

import java.math.BigDecimal;
import java.time.Instant;

public final class DefaultIonCaster implements IonCaster {
    @Override
    public IonValue cast(IonValue value, IonTypeName targetType) throws CastException {
        if (IonValueUtils.isNull(value)) {
            return IonValueUtils.nullValue();
        }

        return switch (targetType) {
            case STRING -> IonValueUtils.system().newString(IonValueUtils.asString(value));
            case INT -> castInt(value);
            case FLOAT -> IonValueUtils.system().newFloat(IonValueUtils.asDecimal(value).doubleValue());
            case DECIMAL -> IonValueUtils.system().newDecimal(IonValueUtils.asDecimal(value));
            case BOOLEAN -> IonValueUtils.system().newBool(IonValueUtils.asBoolean(value));
            case TIMESTAMP -> {
                Instant instant = IonValueUtils.asInstant(value);
                yield IonValueUtils.system().newTimestamp(Timestamp.forMillis(instant.toEpochMilli(), null));
            }
            case LIST -> {
                if (!(value instanceof IonList)) {
                    throw new CastException("Expected list value, got " + value.getType());
                }
                yield value;
            }
            case STRUCT -> {
                if (!(value instanceof IonStruct)) {
                    throw new CastException("Expected struct value, got " + value.getType());
                }
                yield value;
            }
        };
    }

    private IonValue castInt(IonValue value) throws CastException {
        BigDecimal decimal = IonValueUtils.asDecimal(value);
        try {
            return IonValueUtils.system().newInt(decimal.longValueExact());
        } catch (ArithmeticException e) {
            throw new CastException("Expected integer value, got " + decimal, e);
        }
    }
}
