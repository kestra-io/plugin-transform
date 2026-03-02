package io.kestra.plugin.transform.ion;

import com.amazon.ion.IonList;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonTimestamp;
import com.amazon.ion.IonValue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

class IonValueUtilsTest {
    @Test
    void convertsToIonValues() throws Exception {
        Map<String, Object> input = Map.of(
            "name", "alpha",
            "count", 12,
            "price", new BigDecimal("3.50"),
            "active", true,
            "tags", List.of("a", "b")
        );

        IonValue ionValue = IonValueUtils.toIonValue(input);
        IonStruct struct = (IonStruct) ionValue;

        assertThat(((com.amazon.ion.IonString) struct.get("name")).stringValue(), is("alpha"));
        assertThat(((com.amazon.ion.IonInt) struct.get("count")).intValue(), is(12));
        assertThat(((com.amazon.ion.IonDecimal) struct.get("price")).bigDecimalValue(), is(new BigDecimal("3.50")));
        assertThat(((com.amazon.ion.IonBool) struct.get("active")).booleanValue(), is(true));
        IonList tags = (IonList) struct.get("tags");
        assertThat(tags.size(), is(2));
        assertThat(((com.amazon.ion.IonString) tags.get(0)).stringValue(), is("a"));
    }

    @Test
    void convertsFromIonValues() {
        Instant instant = Instant.parse("2024-01-01T00:00:00Z");
        IonTimestamp timestamp = IonValueUtils.system().newTimestamp(com.amazon.ion.Timestamp.forMillis(instant.toEpochMilli(), null));

        IonStruct struct = IonValueUtils.system().newEmptyStruct();
        struct.put("name", IonValueUtils.system().newString("beta"));
        struct.put("count", IonValueUtils.system().newInt(42));
        struct.put("when", timestamp);

        Map<String, Object> result = (Map<String, Object>) IonValueUtils.toJavaValue(struct);

        assertThat(result.get("name"), is("beta"));
        assertThat(result.get("count"), is(42L));
        assertThat(result.get("when"), is("2024-01-01T00:00:00Z"));
    }

    @Test
    void parsesCastsAndNulls() throws Exception {
        IonValue nullValue = IonValueUtils.nullValue();
        assertThat(IonValueUtils.asDecimal(nullValue), is(nullValue()));
        assertThat(IonValueUtils.asBoolean(nullValue), is(nullValue()));
        assertThat(IonValueUtils.asInstant(nullValue), is(nullValue()));

        IonValue decimal = IonValueUtils.system().newDecimal(new BigDecimal("12.75"));
        assertThat(IonValueUtils.asDecimal(decimal), is(new BigDecimal("12.75")));

        IonValue bool = IonValueUtils.system().newBool(true);
        assertThat(IonValueUtils.asBoolean(bool), is(true));

        IonValue stringTimestamp = IonValueUtils.system().newString("2024-02-01T12:00:00Z");
        assertThat(IonValueUtils.asInstant(stringTimestamp), is(Instant.parse("2024-02-01T12:00:00Z")));
    }

    @Test
    void rejectsInvalidCasts() {
        IonValue badDecimal = IonValueUtils.system().newString("nope");
        Assertions.assertThrows(CastException.class, () -> IonValueUtils.asDecimal(badDecimal));

        IonValue badBoolean = IonValueUtils.system().newString("truthy");
        Assertions.assertThrows(CastException.class, () -> IonValueUtils.asBoolean(badBoolean));

        IonValue badTimestamp = IonValueUtils.system().newString("not-a-timestamp");
        Assertions.assertThrows(CastException.class, () -> IonValueUtils.asInstant(badTimestamp));
    }
}
