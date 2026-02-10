package io.kestra.plugin.transform.engine;

import com.amazon.ion.IonStruct;
import io.kestra.plugin.transform.util.TransformException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public final class DefaultTransformTaskEngine implements TransformTaskEngine {
    private final DefaultRecordTransformer transformer;

    public DefaultTransformTaskEngine(DefaultRecordTransformer transformer) {
        this.transformer = transformer;
    }

    @Override
    public TransformResult execute(List<IonStruct> records) throws TransformException {
        List<IonStruct> output = new ArrayList<>();
        java.util.Map<String, String> fieldErrors = new HashMap<>();
        int processed = 0;
        int failed = 0;
        int dropped = 0;

        for (int i = 0; i < records.size(); i++) {
            IonStruct record = records.get(i);
            processed++;
            DefaultRecordTransformer.TransformOutcome outcome = transformer.transformWithErrors(record);
            if (outcome.failed) {
                failed++;
                for (java.util.Map.Entry<String, String> entry : outcome.fieldErrors.entrySet()) {
                    fieldErrors.put(i + "." + entry.getKey(), entry.getValue());
                }
            }
            if (outcome.dropped) {
                dropped++;
                continue;
            }
            output.add(outcome.record);
        }

        return new TransformResult(output, new TransformStats(processed, failed, dropped, fieldErrors));
    }
}
