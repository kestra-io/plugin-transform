package io.kestra.plugin.transform.engine;

import com.amazon.ion.IonStruct;
import com.amazon.ion.IonValue;
import io.kestra.plugin.transform.util.TransformException;
import io.kestra.plugin.transform.util.TransformOptions;
import io.kestra.plugin.transform.expression.ExpressionEngine;
import io.kestra.plugin.transform.expression.ExpressionException;
import io.kestra.plugin.transform.ion.CastException;
import io.kestra.plugin.transform.ion.IonCaster;
import io.kestra.plugin.transform.ion.IonValueUtils;

import java.util.HashMap;
import java.util.List;

public final class DefaultRecordTransformer implements RecordTransformer {
    private final List<FieldMapping> mappings;
    private final java.util.Map<String, FieldMapping> mappingByTarget;
    private final ExpressionEngine expressionEngine;
    private final IonCaster caster;
    private final TransformOptions options;

    public DefaultRecordTransformer(List<FieldMapping> mappings,
                                    ExpressionEngine expressionEngine,
                                    IonCaster caster,
                                    TransformOptions options) {
        this.mappings = mappings;
        this.expressionEngine = expressionEngine;
        this.caster = caster;
        this.options = options;
        this.mappingByTarget = new HashMap<>();
        for (FieldMapping mapping : mappings) {
            this.mappingByTarget.put(mapping.targetField(), mapping);
        }
    }

    @Override
    public IonStruct transform(IonStruct input) throws TransformException {
        return transformWithErrors(input).record;
    }

    public TransformOutcome transformWithErrors(IonStruct input) throws TransformException {
        IonStruct output = IonValueUtils.system().newEmptyStruct();
        java.util.Map<String, String> errors = new HashMap<>();
        boolean failed = false;
        boolean dropped = false;

        if (options.keepOriginalFields()) {
            for (IonValue value : input) {
                String fieldName = value.getFieldName();
                if (mappingByTarget.containsKey(fieldName)) {
                    continue;
                }
                if (options.dropNulls() && IonValueUtils.isNull(value)) {
                    continue;
                }
                output.put(fieldName, IonValueUtils.cloneValue(value));
            }
        }

        for (FieldMapping mapping : mappings) {
            IonValue evaluated;
            try {
                evaluated = expressionEngine.evaluate(mapping.expression(), input);
                if (IonValueUtils.isNull(evaluated)) {
                    if (!mapping.optional()) {
                        throw new TransformException("Missing required field: " + mapping.targetField());
                    }
                }
                IonValue casted = IonValueUtils.isNull(evaluated)
                    ? IonValueUtils.nullValue()
                    : (mapping.type() == null ? evaluated : caster.cast(evaluated, mapping.type()));
                if (options.dropNulls() && IonValueUtils.isNull(casted)) {
                    continue;
                }
                output.put(mapping.targetField(), IonValueUtils.cloneValue(casted));
            } catch (ExpressionException | CastException | TransformException e) {
                failed = true;
                String message = e.getMessage();
                errors.put(mapping.targetField(), message);
                if (options.onError() == TransformOptions.OnErrorMode.FAIL) {
                    throw new TransformException(message, e);
                }
                if (options.onError() == TransformOptions.OnErrorMode.SKIP) {
                    dropped = true;
                    break;
                }
                if (options.onError() == TransformOptions.OnErrorMode.NULL) {
                    if (!options.dropNulls()) {
                        output.put(mapping.targetField(), IonValueUtils.nullValue());
                    }
                }
            }
        }

        return new TransformOutcome(output, failed, dropped, errors);
    }

    public static final class TransformOutcome {
        public final IonStruct record;
        public final boolean failed;
        public final boolean dropped;
        public final java.util.Map<String, String> fieldErrors;

        public TransformOutcome(IonStruct record, boolean failed, boolean dropped, java.util.Map<String, String> fieldErrors) {
            this.record = record;
            this.failed = failed;
            this.dropped = dropped;
            this.fieldErrors = fieldErrors;
        }
    }
}
