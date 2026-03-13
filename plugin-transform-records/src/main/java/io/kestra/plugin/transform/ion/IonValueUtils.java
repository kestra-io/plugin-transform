package io.kestra.plugin.transform.ion;

import com.amazon.ion.IonBool;
import com.amazon.ion.IonDecimal;
import com.amazon.ion.IonFloat;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonList;
import com.amazon.ion.IonString;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonTimestamp;
import com.amazon.ion.IonValue;
import com.amazon.ion.Timestamp;
import com.amazon.ion.system.IonSystemBuilder;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;

public final class IonValueUtils {
    private static final IonSystem SYSTEM = IonSystemBuilder.standard().build();

    private IonValueUtils() {
    }

    public static IonSystem system() {
        return SYSTEM;
    }

    public static boolean isNull(IonValue value) {
        return value == null || value.isNullValue();
    }

    public static IonValue nullValue() {
        return SYSTEM.newNull();
    }

    public static IonValue cloneValue(IonValue value) {
        if (value == null) {
            return null;
        }
        try {
            return value.clone();
        } catch (Exception e) {
            return value;
        }
    }

    public static IonValue toIonValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof IonValue ionValue) {
            return ionValue;
        }
        if (value instanceof String stringValue) {
            return SYSTEM.newString(stringValue);
        }
        if (value instanceof Integer || value instanceof Long || value instanceof Short || value instanceof Byte) {
            return SYSTEM.newInt(((Number) value).longValue());
        }
        if (value instanceof Float || value instanceof Double) {
            return SYSTEM.newFloat(((Number) value).doubleValue());
        }
        if (value instanceof BigDecimal decimal) {
            return SYSTEM.newDecimal(decimal);
        }
        if (value instanceof Boolean bool) {
            return SYSTEM.newBool(bool);
        }
        if (value instanceof Instant instant) {
            return SYSTEM.newTimestamp(Timestamp.forMillis(instant.toEpochMilli(), null));
        }
        if (value instanceof OffsetDateTime offsetDateTime) {
            return SYSTEM.newTimestamp(Timestamp.forMillis(offsetDateTime.toInstant().toEpochMilli(), null));
        }
        if (value instanceof ZonedDateTime zonedDateTime) {
            return SYSTEM.newTimestamp(Timestamp.forMillis(zonedDateTime.toInstant().toEpochMilli(), null));
        }
        if (value instanceof Date date) {
            return SYSTEM.newTimestamp(Timestamp.forMillis(date.getTime(), null));
        }
        if (value instanceof java.util.Map<?, ?> map) {
            IonStruct struct = SYSTEM.newEmptyStruct();
            for (java.util.Map.Entry<?, ?> entry : map.entrySet()) {
                String key = String.valueOf(entry.getKey());
                IonValue ionValue = toIonValue(entry.getValue());
                struct.put(key, ionValue == null ? nullValue() : ionValue);
            }
            return struct;
        }
        if (value instanceof List<?> list) {
            IonList ionList = SYSTEM.newEmptyList();
            for (Object element : list) {
                IonValue ionValue = toIonValue(element);
                ionList.add(ionValue == null ? nullValue() : ionValue);
            }
            return ionList;
        }
        return SYSTEM.newString(String.valueOf(value));
    }

    public static BigDecimal asDecimal(IonValue value) throws CastException {
        if (isNull(value)) {
            return null;
        }
        if (value instanceof IonDecimal ionDecimal) {
            return ionDecimal.bigDecimalValue();
        }
        if (value instanceof IonInt ionInt) {
            return BigDecimal.valueOf(ionInt.longValue());
        }
        if (value instanceof IonFloat ionFloat) {
            return BigDecimal.valueOf(ionFloat.doubleValue());
        }
        if (value instanceof IonString ionString) {
            try {
                return new BigDecimal(ionString.stringValue());
            } catch (NumberFormatException e) {
                throw new CastException("Invalid decimal: " + ionString.stringValue(), e);
            }
        }
        throw new CastException("Expected numeric value, got " + value.getType());
    }

    public static String asString(IonValue value) {
        if (isNull(value)) {
            return null;
        }
        if (value instanceof IonString ionString) {
            return ionString.stringValue();
        }
        return value.toString();
    }

    public static Boolean asBoolean(IonValue value) throws CastException {
        if (isNull(value)) {
            return null;
        }
        if (value instanceof IonBool ionBool) {
            return ionBool.booleanValue();
        }
        if (value instanceof IonString ionString) {
            String raw = ionString.stringValue();
            if ("true".equalsIgnoreCase(raw)) {
                return true;
            }
            if ("false".equalsIgnoreCase(raw)) {
                return false;
            }
        }
        throw new CastException("Expected boolean value, got " + value.getType());
    }

    public static Instant asInstant(IonValue value) throws CastException {
        if (isNull(value)) {
            return null;
        }
        if (value instanceof IonTimestamp ionTimestamp) {
            return Instant.ofEpochMilli(ionTimestamp.timestampValue().getMillis());
        }
        if (value instanceof IonString ionString) {
            try {
                return Instant.parse(ionString.stringValue());
            } catch (Exception e) {
                throw new CastException("Invalid timestamp: " + ionString.stringValue(), e);
            }
        }
        throw new CastException("Expected timestamp value, got " + value.getType());
    }

    public static Object toJavaValue(IonValue value) {
        if (isNull(value)) {
            return null;
        }
        if (value instanceof IonStruct ionStruct) {
            java.util.Map<String, Object> map = new java.util.LinkedHashMap<>();
            for (IonValue child : ionStruct) {
                map.put(child.getFieldName(), toJavaValue(child));
            }
            return map;
        }
        if (value instanceof IonList ionList) {
            java.util.List<Object> list = new java.util.ArrayList<>();
            for (IonValue child : ionList) {
                list.add(toJavaValue(child));
            }
            return list;
        }
        if (value instanceof IonString ionString) {
            return ionString.stringValue();
        }
        if (value instanceof IonInt ionInt) {
            return ionInt.longValue();
        }
        if (value instanceof IonFloat ionFloat) {
            return ionFloat.doubleValue();
        }
        if (value instanceof IonDecimal ionDecimal) {
            return ionDecimal.bigDecimalValue();
        }
        if (value instanceof IonBool ionBool) {
            return ionBool.booleanValue();
        }
        if (value instanceof IonTimestamp ionTimestamp) {
            return Instant.ofEpochMilli(ionTimestamp.timestampValue().getMillis()).toString();
        }
        return value.toString();
    }
}
