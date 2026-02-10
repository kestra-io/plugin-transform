package io.kestra.plugin.transform;

import com.amazon.ion.IonBool;
import com.amazon.ion.IonList;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.fasterxml.jackson.annotation.JsonCreator;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.transform.expression.DefaultExpressionEngine;
import io.kestra.plugin.transform.expression.ExpressionException;
import io.kestra.plugin.transform.ion.CastException;
import io.kestra.plugin.transform.ion.DefaultIonCaster;
import io.kestra.plugin.transform.ion.IonTypeName;
import io.kestra.plugin.transform.ion.IonValueUtils;
import io.kestra.plugin.transform.util.OutputFormat;
import io.kestra.plugin.transform.util.TransformException;
import io.kestra.plugin.transform.util.TransformProfiler;
import io.kestra.plugin.transform.util.TransformTaskSupport;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.AllArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Select records",
    description = "Align multiple inputs by position, optionally filter rows, and project output records."
)
@Plugin(
    examples = {
        @io.kestra.core.models.annotations.Example(
            title = "Enrich and filter by row position",
            code = {
                "inputs:",
                "  - \"{{ outputs.orders.uri }}\"",
                "  - \"{{ outputs.customers.uri }}\"",
                "  - \"{{ outputs.scores.uri }}\"",
                "where: amount > 100 && $3.score > 0.8",
                "fields:",
                "  orderId: order_id",
                "  customer: $2.name",
                "  amount: $1.amount",
                "  score: $3.score"
            }
        )
    },
    metrics = {
        @Metric(name = "processed", type = Counter.TYPE),
        @Metric(name = "passed", type = Counter.TYPE),
        @Metric(name = "dropped", type = Counter.TYPE),
        @Metric(name = "failed", type = Counter.TYPE)
    }
)
public class Select extends Task implements RunnableTask<Select.Output> {
    @Schema(
        title = "Input records",
        description = "List of inputs (Ion list/struct or storage URIs) to align by row position."
    )
    private List<Property<Object>> inputs;

    @Schema(
        title = "Filter expression",
        description = "Optional boolean expression evaluated on each merged row (supports $1, $2, ...)."
    )
    private Property<String> where;

    @Schema(
        title = "Projection mappings",
        description = "Optional mapping of output field names to expressions and types (supports $1, $2, ...). If omitted, outputs the merged row."
    )
    private java.util.Map<String, FieldDefinition> fields;

    @Schema(
        title = "Keep input fields",
        description = "When fields are provided, include the selected input fields (1-based indices) in addition to projected fields."
    )
    @Builder.Default
    private List<Integer> keepInputFields = List.of();

    @Builder.Default
    @Schema(title = "Drop null fields")
    private boolean dropNulls = true;

    @Builder.Default
    @Schema(title = "On length mismatch behavior")
    private OnLengthMismatchMode onLengthMismatch = OnLengthMismatchMode.FAIL;

    @Builder.Default
    @Schema(title = "On error behavior")
    private OnErrorMode onError = OnErrorMode.FAIL;

    @Builder.Default
    @Schema(
        title = "Output format",
        description = "Experimental: TEXT or BINARY. Only transform tasks can read binary Ion. Use TEXT as the final step."
    )
    private OutputFormat outputFormat = OutputFormat.TEXT;

    @Schema(
        title = "Output mode",
        description = "AUTO stores to internal storage when any input is a storage URI; otherwise it returns records."
    )
    @Builder.Default
    private OutputMode output = OutputMode.AUTO;

    @Override
    public Output run(RunContext runContext) throws Exception {
        if (inputs == null || inputs.isEmpty()) {
            throw new TransformException("inputs is required");
        }

        List<TransformTaskSupport.ResolvedInput> resolvedInputs = new ArrayList<>(inputs.size());
        boolean anyFromStorage = false;
        for (Property<Object> input : inputs) {
            TransformTaskSupport.ResolvedInput resolved = TransformTaskSupport.resolveInput(runContext, input);
            resolvedInputs.add(resolved);
            anyFromStorage = anyFromStorage || resolved.fromStorage();
        }

        String whereExpr = null;
        if (where != null) {
            whereExpr = runContext.render(where).as(String.class).orElse(null);
        }
        if (whereExpr != null && whereExpr.isBlank()) {
            whereExpr = null;
        }

        boolean needsPositionalContext = false;
        if (whereExpr != null && whereExpr.contains("$")) {
            needsPositionalContext = true;
        }
        if (!needsPositionalContext && fields != null) {
            for (FieldDefinition definition : fields.values()) {
                if (definition != null && definition.expr != null && definition.expr.contains("$")) {
                    needsPositionalContext = true;
                    break;
                }
            }
        }

        OutputMode effectiveOutput = output == OutputMode.AUTO
            ? (anyFromStorage ? OutputMode.STORE : OutputMode.RECORDS)
            : output;

        DefaultExpressionEngine expressionEngine = new DefaultExpressionEngine();
        DefaultIonCaster caster = new DefaultIonCaster();
        StatsAccumulator stats = new StatsAccumulator();

        if (effectiveOutput == OutputMode.STORE) {
            URI storedUri = selectToStorage(runContext, resolvedInputs, whereExpr, needsPositionalContext, expressionEngine, caster, stats);
            runContext.metric(Counter.of("processed", stats.processed))
                .metric(Counter.of("passed", stats.passed))
                .metric(Counter.of("dropped", stats.dropped))
                .metric(Counter.of("failed", stats.failed));
            return Output.builder()
                .uri(storedUri.toString())
                .build();
        }

        List<Object> rendered = selectToRecords(runContext, resolvedInputs, whereExpr, needsPositionalContext, expressionEngine, caster, stats);
        runContext.metric(Counter.of("processed", stats.processed))
            .metric(Counter.of("passed", stats.passed))
            .metric(Counter.of("dropped", stats.dropped))
            .metric(Counter.of("failed", stats.failed));
        return Output.builder()
            .records(rendered)
            .build();
    }

    private List<Object> selectToRecords(RunContext runContext,
                                         List<TransformTaskSupport.ResolvedInput> resolvedInputs,
                                         String whereExpr,
                                         boolean needsPositionalContext,
                                         DefaultExpressionEngine expressionEngine,
                                         DefaultIonCaster caster,
                                         StatsAccumulator stats) throws TransformException {
        try (MultiCursor cursor = openCursors(runContext, resolvedInputs)) {
            List<Object> outputRecords = new ArrayList<>();
            while (cursor.hasAlignedNext(onLengthMismatch)) {
                List<IonStruct> sourceRows = cursor.nextRow();
                stats.processed++;
                IonStruct mergedFlat = mergeFlat(sourceRows);
                IonStruct evalContext = needsPositionalContext ? buildEvalContext(mergedFlat, sourceRows) : mergedFlat;

                IonStruct outputRow = processRow(whereExpr, sourceRows, mergedFlat, evalContext, expressionEngine, caster, stats);
                if (outputRow == null) {
                    continue;
                }
                outputRecords.add(IonValueUtils.toJavaValue(outputRow));
            }
            return outputRecords;
        }
    }

    private URI selectToStorage(RunContext runContext,
                                List<TransformTaskSupport.ResolvedInput> resolvedInputs,
                                String whereExpr,
                                boolean needsPositionalContext,
                                DefaultExpressionEngine expressionEngine,
                                DefaultIonCaster caster,
                                StatsAccumulator stats) throws TransformException {
        String name = "select-" + UUID.randomUUID() + ".ion";
        try (MultiCursor cursor = openCursors(runContext, resolvedInputs)) {
            java.nio.file.Path outputPath = runContext.workingDir().createTempFile(".ion");
            try (OutputStream outputStream = TransformTaskSupport.wrapCompression(
                TransformTaskSupport.bufferedOutput(outputPath));
                IonWriter writer = TransformTaskSupport.createWriter(outputStream, outputFormat)) {
                boolean profile = TransformProfiler.isEnabled();
                while (cursor.hasAlignedNext(onLengthMismatch)) {
                    List<IonStruct> sourceRows = cursor.nextRow();
                    stats.processed++;
                    long transformStart = profile ? System.nanoTime() : 0L;
                    IonStruct mergedFlat = mergeFlat(sourceRows);
                    IonStruct evalContext = needsPositionalContext ? buildEvalContext(mergedFlat, sourceRows) : mergedFlat;

                    IonStruct outputRow = processRow(whereExpr, sourceRows, mergedFlat, evalContext, expressionEngine, caster, stats);
                    if (profile) {
                        TransformProfiler.addTransformNs(System.nanoTime() - transformStart);
                    }
                    if (outputRow == null) {
                        continue;
                    }
                    long writeStart = profile ? System.nanoTime() : 0L;
                    outputRow.writeTo(writer);
                    TransformTaskSupport.writeDelimiter(outputStream, outputFormat);
                    if (profile) {
                        TransformProfiler.addWriteNs(System.nanoTime() - writeStart);
                    }
                }
                writer.finish();
            }
            return runContext.storage().putFile(outputPath.toFile(), name);
        } catch (IOException e) {
            throw new TransformException("Unable to store selected records", e);
        }
    }

    private IonStruct processRow(String whereExpr,
                                 List<IonStruct> sourceRows,
                                 IonStruct mergedFlat,
                                 IonStruct evalContext,
                                 DefaultExpressionEngine expressionEngine,
                                 DefaultIonCaster caster,
                                 StatsAccumulator stats) throws TransformException {
        if (whereExpr == null && (fields == null || fields.isEmpty())) {
            stats.passed++;
            if (dropNulls) {
                dropNullFields(mergedFlat);
            }
            return mergedFlat;
        }

        if (whereExpr != null) {
            try {
                Boolean decision = evaluateBoolean(whereExpr, evalContext, expressionEngine);
                if (!decision) {
                    stats.dropped++;
                    return null;
                }
            } catch (ExpressionException | TransformException e) {
                stats.failed++;
                if (onError == OnErrorMode.FAIL) {
                    throw new TransformException(e.getMessage(), e);
                }
                if (onError == OnErrorMode.SKIP) {
                    stats.dropped++;
                    return null;
                }
                if (onError == OnErrorMode.KEEP) {
                    stats.passed++;
                    IonStruct output = cloneStruct(mergedFlat);
                    if (dropNulls) {
                        dropNullFields(output);
                    }
                    return output;
                }
            }
        }

        IonStruct projected = projectRow(sourceRows, mergedFlat, evalContext, expressionEngine, caster, stats);
        if (projected == null) {
            return null;
        }
        stats.passed++;
        if (dropNulls) {
            dropNullFields(projected);
        }
        return projected;
    }

    private IonStruct projectRow(List<IonStruct> sourceRows,
                                 IonStruct mergedFlat,
                                 IonStruct evalContext,
                                 DefaultExpressionEngine expressionEngine,
                                 DefaultIonCaster caster,
                                 StatsAccumulator stats) throws TransformException {
        if (fields == null || fields.isEmpty()) {
            return cloneStruct(mergedFlat);
        }

        IonStruct output = keepInputFields == null || keepInputFields.isEmpty()
            ? IonValueUtils.system().newEmptyStruct()
            : mergeSelectedInputs(sourceRows);

        for (java.util.Map.Entry<String, FieldDefinition> entry : fields.entrySet()) {
            String targetField = entry.getKey();
            FieldDefinition definition = entry.getValue();
            if (definition == null) {
                throw new TransformException("Field definition is required for '" + targetField + "'");
            }
            String expression = definition.expr;
            if (expression == null || expression.isBlank()) {
                throw new TransformException("expr is required for '" + targetField + "'");
            }
            try {
                IonValue evaluated = expressionEngine.evaluate(expression, evalContext);
                if (IonValueUtils.isNull(evaluated) && !definition.optional) {
                    throw new TransformException("Missing required field: " + targetField);
                }
                IonValue casted = IonValueUtils.isNull(evaluated)
                    ? IonValueUtils.nullValue()
                    : (definition.type == null ? evaluated : caster.cast(evaluated, definition.type));
                IonValue normalized = casted == null ? IonValueUtils.nullValue() : casted;
                if (dropNulls && IonValueUtils.isNull(normalized)) {
                    continue;
                }
                output.put(targetField, IonValueUtils.cloneValue(normalized));
            } catch (ExpressionException | CastException | TransformException e) {
                stats.failed++;
                if (onError == OnErrorMode.FAIL) {
                    throw new TransformException(e.getMessage(), e);
                }
                if (onError == OnErrorMode.SKIP) {
                    stats.dropped++;
                    return null;
                }
                if (onError == OnErrorMode.KEEP) {
                    return cloneStruct(mergedFlat);
                }
            }
        }
        return output;
    }

    private IonStruct mergeSelectedInputs(List<IonStruct> sourceRows) throws TransformException {
        IonStruct merged = IonValueUtils.system().newEmptyStruct();
        for (Integer index : keepInputFields) {
            if (index == null) {
                throw new TransformException("keepInputFields must not contain null");
            }
            if (index < 1 || index > sourceRows.size()) {
                throw new TransformException("keepInputFields contains out-of-range index: " + index);
            }
            IonStruct record = sourceRows.get(index - 1);
            for (IonValue value : record) {
                merged.put(value.getFieldName(), IonValueUtils.cloneValue(value));
            }
        }
        return merged;
    }

    private Boolean evaluateBoolean(String whereExpr,
                                    IonStruct record,
                                    DefaultExpressionEngine expressionEngine) throws ExpressionException, TransformException {
        IonValue evaluated = expressionEngine.evaluate(whereExpr, record);
        if (IonValueUtils.isNull(evaluated)) {
            throw new TransformException("where expression evaluated to null");
        }
        if (evaluated instanceof IonBool ionBool) {
            return ionBool.booleanValue();
        }
        try {
            Boolean value = IonValueUtils.asBoolean(evaluated);
            if (value == null) {
                throw new TransformException("where expression evaluated to null");
            }
            return value;
        } catch (io.kestra.plugin.transform.ion.CastException e) {
            throw new TransformException("where expression must return boolean, got " + evaluated.getType(), e);
        }
    }

    private IonStruct mergeFlat(List<IonStruct> records) {
        IonStruct merged = IonValueUtils.system().newEmptyStruct();
        for (IonStruct record : records) {
            for (IonValue value : record) {
                merged.put(value.getFieldName(), IonValueUtils.cloneValue(value));
            }
        }
        return merged;
    }

    private IonStruct buildEvalContext(IonStruct mergedFlat, List<IonStruct> sourceRows) throws TransformException {
        IonStruct context = cloneStruct(mergedFlat);
        for (int i = 0; i < sourceRows.size(); i++) {
            context.put("$" + (i + 1), cloneStruct(sourceRows.get(i)));
        }
        return context;
    }

    private IonStruct cloneStruct(IonStruct struct) throws TransformException {
        IonValue cloned = IonValueUtils.cloneValue(struct);
        if (cloned instanceof IonStruct clonedStruct) {
            return clonedStruct;
        }
        throw new TransformException("Expected struct record, got " + (cloned == null ? "null" : cloned.getType()));
    }

    private void dropNullFields(IonStruct struct) {
        List<String> remove = new ArrayList<>();
        for (IonValue value : struct) {
            if (IonValueUtils.isNull(value)) {
                remove.add(value.getFieldName());
            }
        }
        for (String field : remove) {
            struct.remove(field);
        }
    }

    private MultiCursor openCursors(RunContext runContext,
                                    List<TransformTaskSupport.ResolvedInput> inputs) throws TransformException {
        List<RecordCursor> cursors = new ArrayList<>(inputs.size());
        for (TransformTaskSupport.ResolvedInput input : inputs) {
            cursors.add(openCursor(runContext, input));
        }
        return new MultiCursor(cursors);
    }

    private RecordCursor openCursor(RunContext runContext,
                                    TransformTaskSupport.ResolvedInput input) throws TransformException {
        if (!input.fromStorage()) {
            List<IonStruct> records = TransformTaskSupport.normalizeRecords(input.value());
            return RecordCursor.ofList(records);
        }
        try {
            InputStream inputStream = runContext.storage().getFile(input.storageUri());
            Iterator<IonValue> iterator = IonValueUtils.system().iterate(inputStream);
            if (!iterator.hasNext()) {
                return RecordCursor.ofIterator(List.<IonValue>of().iterator(), inputStream);
            }
            IonValue first = iterator.next();
            if (first instanceof IonList list) {
                if (iterator.hasNext()) {
                    throw new TransformException("Expected Ion list or newline-delimited structs, got mixed values");
                }
                return RecordCursor.ofIterator(list.iterator(), inputStream);
            }
            return RecordCursor.ofIterator(new PrependIterator(first, iterator), inputStream);
        } catch (IOException e) {
            throw new TransformException("Unable to read Ion file from storage: " + input.storageUri(), e);
        }
    }

    private static final class MultiCursor implements AutoCloseable {
        private final List<RecordCursor> cursors;
        private boolean mismatchPending;

        private MultiCursor(List<RecordCursor> cursors) {
            this.cursors = cursors;
        }

        boolean hasAlignedNext(OnLengthMismatchMode mismatchMode) throws TransformException {
            if (mismatchPending) {
                if (mismatchMode == OnLengthMismatchMode.FAIL) {
                    throw new TransformException("inputs must have same length");
                }
                return false;
            }
            boolean any = false;
            boolean all = true;
            for (RecordCursor cursor : cursors) {
                boolean has = cursor.hasNext();
                any = any || has;
                all = all && has;
            }
            if (!any) {
                return false;
            }
            if (!all) {
                mismatchPending = true;
                if (mismatchMode == OnLengthMismatchMode.FAIL) {
                    throw new TransformException("inputs must have same length");
                }
                return false;
            }
            return true;
        }

        List<IonStruct> nextRow() throws TransformException {
            List<IonStruct> row = new ArrayList<>(cursors.size());
            for (RecordCursor cursor : cursors) {
                row.add(cursor.nextStruct());
            }
            return row;
        }

        @Override
        public void close() {
            for (RecordCursor cursor : cursors) {
                try {
                    cursor.close();
                } catch (Exception ignored) {
                }
            }
        }
    }

    private static final class PrependIterator implements Iterator<IonValue> {
        private IonValue first;
        private final Iterator<IonValue> delegate;

        private PrependIterator(IonValue first, Iterator<IonValue> delegate) {
            this.first = first;
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            return first != null || delegate.hasNext();
        }

        @Override
        public IonValue next() {
            if (first != null) {
                IonValue value = first;
                first = null;
                return value;
            }
            return delegate.next();
        }
    }

    private static final class RecordCursor implements AutoCloseable {
        private final Iterator<IonValue> iterator;
        private final AutoCloseable closeable;

        private RecordCursor(Iterator<IonValue> iterator, AutoCloseable closeable) {
            this.iterator = iterator;
            this.closeable = closeable;
        }

        static RecordCursor ofList(List<IonStruct> records) {
            return new RecordCursor(new Iterator<IonValue>() {
                private final Iterator<IonStruct> delegate = records.iterator();

                @Override
                public boolean hasNext() {
                    return delegate.hasNext();
                }

                @Override
                public IonValue next() {
                    return delegate.next();
                }
            }, null);
        }

        static RecordCursor ofIterator(Iterator<IonValue> iterator, AutoCloseable closeable) {
            return new RecordCursor(iterator, closeable);
        }

        boolean hasNext() {
            return iterator.hasNext();
        }

        IonStruct nextStruct() throws TransformException {
            IonValue value = iterator.next();
            if (value instanceof IonStruct struct) {
                return struct;
            }
            throw new TransformException("Expected struct record, got " + (value == null ? "null" : value.getType()));
        }

        @Override
        public void close() throws Exception {
            if (closeable != null) {
                closeable.close();
            }
        }
    }

    public enum OnLengthMismatchMode {
        FAIL,
        SKIP
    }

    public enum OnErrorMode {
        FAIL,
        SKIP,
        KEEP
    }

    public enum OutputMode {
        AUTO,
        RECORDS,
        STORE;

        @JsonCreator
        public static OutputMode from(Object value) {
            if (value == null) {
                return null;
            }
            String raw = String.valueOf(value).trim();
            if ("URI".equalsIgnoreCase(raw)) {
                return STORE;
            }
            return OutputMode.valueOf(raw.toUpperCase(java.util.Locale.ROOT));
        }
    }

    private static final class StatsAccumulator {
        private int processed;
        private int passed;
        private int dropped;
        private int failed;
    }

    @Builder
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class FieldDefinition {
        private String expr;
        private IonTypeName type;

        @Builder.Default
        private boolean optional = false;

        @JsonCreator
        public static FieldDefinition from(Object value) {
            if (value == null) {
                return null;
            }
            if (value instanceof String stringValue) {
                return FieldDefinition.builder().expr(stringValue).build();
            }
            if (value instanceof java.util.Map<?, ?> map) {
                Object exprValue = map.get("expr");
                Object typeValue = map.get("type");
                Object optionalValue = map.get("optional");
                IonTypeName type = null;
                if (typeValue instanceof IonTypeName ionTypeName) {
                    type = ionTypeName;
                } else if (typeValue instanceof String typeString) {
                    type = IonTypeName.valueOf(typeString);
                }
                boolean optional = optionalValue instanceof Boolean bool ? bool : false;
                return FieldDefinition.builder()
                    .expr(exprValue == null ? null : String.valueOf(exprValue))
                    .type(type)
                    .optional(optional)
                    .build();
            }
            throw new IllegalArgumentException("Unsupported field definition: " + value);
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Stored Ion file URI",
            description = "URI to the stored Ion file when output mode is STORE or AUTO resolves to STORE."
        )
        private final String uri;

        @Schema(
            title = "Selected records",
            description = "JSON-safe records when output mode is RECORDS or AUTO resolves to RECORDS."
        )
        private final List<Object> records;
    }
}
