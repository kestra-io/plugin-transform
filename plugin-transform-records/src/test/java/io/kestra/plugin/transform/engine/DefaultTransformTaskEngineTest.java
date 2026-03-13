package io.kestra.plugin.transform.engine;

import com.amazon.ion.IonStruct;
import io.kestra.plugin.transform.util.TransformOptions;
import io.kestra.plugin.transform.expression.DefaultExpressionEngine;
import io.kestra.plugin.transform.ion.DefaultIonCaster;
import io.kestra.plugin.transform.ion.IonTypeName;
import io.kestra.plugin.transform.ion.IonValueUtils;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

class DefaultTransformTaskEngineTest {
    @Test
    void skipsRecordsAndReportsStats() throws Exception {
        DefaultRecordTransformer transformer = new DefaultRecordTransformer(
            List.of(new FieldMapping("value", "value", IonTypeName.INT, false)),
            new DefaultExpressionEngine(),
            new DefaultIonCaster(),
            new TransformOptions(false, true, TransformOptions.OnErrorMode.SKIP)
        );
        DefaultTransformTaskEngine engine = new DefaultTransformTaskEngine(transformer);

        IonStruct ok = IonValueUtils.system().newEmptyStruct();
        ok.put("value", IonValueUtils.system().newInt(1));
        IonStruct bad = IonValueUtils.system().newEmptyStruct();

        TransformResult result = engine.execute(List.of(ok, bad));

        assertThat(result.records(), hasSize(1));
        assertThat(result.stats().processed(), is(2));
        assertThat(result.stats().failed(), is(1));
        assertThat(result.stats().dropped(), is(1));
        assertThat(result.stats().fieldErrors().size(), is(1));
    }
}
