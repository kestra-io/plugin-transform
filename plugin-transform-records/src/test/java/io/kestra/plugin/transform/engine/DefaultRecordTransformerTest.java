package io.kestra.plugin.transform.engine;

import com.amazon.ion.IonStruct;
import io.kestra.plugin.transform.util.TransformException;
import io.kestra.plugin.transform.util.TransformOptions;
import io.kestra.plugin.transform.expression.DefaultExpressionEngine;
import io.kestra.plugin.transform.ion.DefaultIonCaster;
import io.kestra.plugin.transform.ion.IonTypeName;
import io.kestra.plugin.transform.ion.IonValueUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class DefaultRecordTransformerTest {
    @Test
    void failsOnMissingRequiredField() {
        DefaultRecordTransformer transformer = new DefaultRecordTransformer(
            List.of(new FieldMapping("value", "value", IonTypeName.INT, false)),
            new DefaultExpressionEngine(),
            new DefaultIonCaster(),
            new TransformOptions(false, true, TransformOptions.OnErrorMode.FAIL)
        );

        IonStruct record = IonValueUtils.system().newEmptyStruct();

        TransformException exception = Assertions.assertThrows(
            TransformException.class,
            () -> transformer.transform(record)
        );

        assertThat(exception.getMessage(), is("Missing required field: value"));
    }

    @Test
    void nullsFieldOnErrorWhenConfigured() throws Exception {
        DefaultRecordTransformer transformer = new DefaultRecordTransformer(
            List.of(new FieldMapping("value", "value", IonTypeName.INT, false)),
            new DefaultExpressionEngine(),
            new DefaultIonCaster(),
            new TransformOptions(false, false, TransformOptions.OnErrorMode.NULL)
        );

        IonStruct record = IonValueUtils.system().newEmptyStruct();
        IonStruct output = transformer.transform(record);

        assertThat(IonValueUtils.isNull(output.get("value")), is(true));
    }

    @Test
    void keepsOriginalFieldsWhenEnabled() throws Exception {
        DefaultRecordTransformer transformer = new DefaultRecordTransformer(
            List.of(new FieldMapping("value", "value", IonTypeName.INT, true)),
            new DefaultExpressionEngine(),
            new DefaultIonCaster(),
            new TransformOptions(true, true, TransformOptions.OnErrorMode.FAIL)
        );

        IonStruct record = IonValueUtils.system().newEmptyStruct();
        record.put("extra", IonValueUtils.system().newString("keep-me"));
        record.put("value", IonValueUtils.system().newInt(7));

        IonStruct output = transformer.transform(record);

        assertThat(((com.amazon.ion.IonString) output.get("extra")).stringValue(), is("keep-me"));
        assertThat(((com.amazon.ion.IonInt) output.get("value")).intValue(), is(7));
    }
}
