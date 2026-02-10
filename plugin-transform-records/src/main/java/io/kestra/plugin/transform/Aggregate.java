package io.kestra.plugin.transform;

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
import io.kestra.plugin.transform.util.TransformProfiler;
import io.kestra.plugin.transform.util.TransformTaskSupport;
import io.kestra.plugin.transform.util.TransformException;
import io.kestra.plugin.transform.util.TransformOptions;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Aggregate records",
    description = "Group records and compute typed aggregates."
)
@Plugin(
    examples = {
        @io.kestra.core.models.annotations.Example(
            title = "Aggregate totals",
            code = {
                "from: \"{{ outputs.normalize.records }}\"",
                "groupBy:",
                "  - customer_id",
                "  - country",
                "aggregates:",
                "  order_count:",
                "    expr: count()",
                "    type: INT",
                "  total_spent:",
                "    expr: sum(total_spent)",
                "    type: DECIMAL",
                "onError: FAIL"
            }
        ),
        @io.kestra.core.models.annotations.Example(
            title = "Aggregate with stored output",
            full = true,
            code = """
                id: aggregate_totals
                namespace: company.team

                tasks:
                  - id: fetch
                    type: io.kestra.plugin.core.output.OutputValues
                    values:
                      records:
                        - customer_id: "c1"
                          country: "FR"
                          total_spent: 10
                        - customer_id: "c1"
                          country: "FR"
                          total_spent: 5

                  - id: aggregate
                    type: io.kestra.plugin.transform.Aggregate
                    from: "{{ outputs.fetch.values.records }}"
                    output: STORE
                    groupBy:
                      - customer_id
                      - country
                    aggregates:
                      order_count:
                        expr: count()
                        type: INT
                      total_spent:
                        expr: sum(total_spent)
                        type: DECIMAL
                """
        )
    },
    metrics = {
        @Metric(name = "processed", type = Counter.TYPE),
        @Metric(name = "groups", type = Counter.TYPE),
        @Metric(name = "failed", type = Counter.TYPE)
    }
)
public class Aggregate extends Task implements RunnableTask<Aggregate.Output> {
    @Schema(
        title = "Input records",
        description = "Ion list or struct to transform, or a storage URI pointing to an Ion file."
    )
    private Property<Object> from;

    @Schema(
        title = "Group by fields",
        description = "Fields to group on."
    )
    private Property<List<String>> groupBy;

    @Schema(
        title = "Aggregate definitions",
        description = "Aggregate expressions with optional types."
    )
    private Map<String, AggregateDefinition> aggregates;

    @Builder.Default
    @Schema(title = "On error behavior")
    private TransformOptions.OnErrorMode onError = TransformOptions.OnErrorMode.FAIL;

    @Builder.Default
    @Schema(
        title = "Output format",
        description = "Experimental: TEXT or BINARY. Only transform tasks can read binary Ion. Use TEXT as the final step."
    )
    private OutputFormat outputFormat = OutputFormat.TEXT;

    @Schema(
        title = "Output mode",
        description = "AUTO stores to internal storage when the input is a storage URI; otherwise it returns records."
    )
    @Builder.Default
    private OutputMode output = OutputMode.AUTO;

    @Override
    public Output run(RunContext runContext) throws Exception {
        TransformTaskSupport.ResolvedInput resolvedInput = TransformTaskSupport.resolveInput(runContext, from);

        List<String> groupByFields = runContext.render(groupBy).asList(String.class);
        if (groupByFields == null || groupByFields.isEmpty()) {
            throw new TransformException("groupBy is required");
        }
        if (aggregates == null || aggregates.isEmpty()) {
            throw new TransformException("aggregates is required");
        }

        List<AggregateMapping> mappings = new ArrayList<>();
        for (Map.Entry<String, AggregateDefinition> entry : aggregates.entrySet()) {
            AggregateDefinition definition = entry.getValue();
            if (definition == null) {
                throw new TransformException("Aggregate definition is required for '" + entry.getKey() + "'");
            }
            String expr = Objects.requireNonNull(definition.expr, "expr is required");
            AggregateFunction function = parseAggregateFunction(expr);
            mappings.add(new AggregateMapping(
                entry.getKey(),
                function,
                definition.type
            ));
        }

        DefaultExpressionEngine expressionEngine = new DefaultExpressionEngine();
        DefaultIonCaster caster = new DefaultIonCaster();
        StatsAccumulator stats = new StatsAccumulator();

        Map<GroupKey, GroupBucket> grouped = new LinkedHashMap<>();
        if (resolvedInput.fromStorage()) {
            groupFromStorage(runContext, resolvedInput.storageUri(), groupByFields, mappings, grouped, stats, expressionEngine);
        } else {
            List<IonStruct> records = TransformTaskSupport.normalizeRecords(resolvedInput.value());
            groupRecords(records, groupByFields, mappings, grouped, stats, expressionEngine);
        }

        OutputMode effectiveOutput = output == OutputMode.AUTO
            ? (resolvedInput.fromStorage() ? OutputMode.STORE : OutputMode.RECORDS)
            : output;

        if (effectiveOutput == OutputMode.STORE) {
            URI stored = storeRecords(runContext, grouped, groupByFields, mappings, caster, stats);
            runContext.metric(Counter.of("processed", stats.processed))
                .metric(Counter.of("groups", stats.groups))
                .metric(Counter.of("failed", stats.failed));
            return Output.builder()
                .uri(stored.toString())
                .build();
        }

        List<Object> rendered = aggregateToRecords(grouped, groupByFields, mappings, caster, stats);
        runContext.metric(Counter.of("processed", stats.processed))
            .metric(Counter.of("groups", stats.groups))
            .metric(Counter.of("failed", stats.failed));
        return Output.builder()
            .records(rendered)
            .build();
    }

    private List<Object> aggregateToRecords(Map<GroupKey, GroupBucket> grouped,
                                            List<String> groupByFields,
                                            List<AggregateMapping> mappings,
                                            DefaultIonCaster caster,
                                            StatsAccumulator stats) throws TransformException {
        List<Object> outputRecords = new ArrayList<>();
        for (GroupBucket bucket : grouped.values()) {
            IonStruct output = aggregateBucket(bucket, groupByFields, mappings, caster, stats);
            if (output != null) {
                outputRecords.add(IonValueUtils.toJavaValue(output));
            }
        }
        return outputRecords;
    }

    private void groupRecords(List<IonStruct> records,
                              List<String> groupByFields,
                              List<AggregateMapping> mappings,
                              Map<GroupKey, GroupBucket> grouped,
                              StatsAccumulator stats,
                              DefaultExpressionEngine expressionEngine) throws TransformException {
        for (IonStruct record : records) {
            groupRecord(record, groupByFields, mappings, grouped, stats, expressionEngine);
        }
    }

    private void groupFromStorage(RunContext runContext,
                                  URI uri,
                                  List<String> groupByFields,
                                  List<AggregateMapping> mappings,
                                  Map<GroupKey, GroupBucket> grouped,
                                  StatsAccumulator stats,
                                  DefaultExpressionEngine expressionEngine) throws TransformException {
        try (InputStream inputStream = runContext.storage().getFile(uri)) {
            Iterator<IonValue> iterator = IonValueUtils.system().iterate(inputStream);
            while (iterator.hasNext()) {
                IonValue value = iterator.next();
                if (value instanceof IonList list) {
                    for (IonValue element : list) {
                        groupStreamRecord(element, groupByFields, mappings, grouped, stats, expressionEngine);
                    }
                } else {
                    groupStreamRecord(value, groupByFields, mappings, grouped, stats, expressionEngine);
                }
            }
        } catch (IOException e) {
            throw new TransformException("Unable to read Ion file from storage: " + uri, e);
        }
    }

    private void groupStreamRecord(IonValue value,
                                   List<String> groupByFields,
                                   List<AggregateMapping> mappings,
                                   Map<GroupKey, GroupBucket> grouped,
                                   StatsAccumulator stats,
                                   DefaultExpressionEngine expressionEngine) throws TransformException {
        IonStruct record = asStruct(value);
        boolean profile = TransformProfiler.isEnabled();
        long transformStart = profile ? System.nanoTime() : 0L;
        groupRecord(record, groupByFields, mappings, grouped, stats, expressionEngine);
        if (profile) {
            TransformProfiler.addTransformNs(System.nanoTime() - transformStart);
        }
    }

    private URI storeRecords(RunContext runContext,
                             Map<GroupKey, GroupBucket> grouped,
                             List<String> groupByFields,
                             List<AggregateMapping> mappings,
                             DefaultIonCaster caster,
                             StatsAccumulator stats) throws TransformException {
        String name = "aggregate-" + UUID.randomUUID() + ".ion";
        try {
            java.nio.file.Path outputPath = runContext.workingDir().createTempFile(".ion");
            try (OutputStream outputStream = TransformTaskSupport.wrapCompression(
                TransformTaskSupport.bufferedOutput(outputPath));
                 IonWriter writer = TransformTaskSupport.createWriter(outputStream, outputFormat)) {
                boolean profile = TransformProfiler.isEnabled();
                for (GroupBucket bucket : grouped.values()) {
                    IonStruct output = aggregateBucket(bucket, groupByFields, mappings, caster, stats);
                    if (output != null) {
                        long writeStart = profile ? System.nanoTime() : 0L;
                        output.writeTo(writer);
                        TransformTaskSupport.writeDelimiter(outputStream, outputFormat);
                        if (profile) {
                            TransformProfiler.addWriteNs(System.nanoTime() - writeStart);
                        }
                    }
                }
                writer.finish();
            }
            return runContext.storage().putFile(outputPath.toFile(), name);
        } catch (IOException e) {
            throw new TransformException("Unable to store aggregated records", e);
        }
    }

    private IonStruct aggregateBucket(GroupBucket bucket,
                                      List<String> groupByFields,
                                      List<AggregateMapping> mappings,
                                      DefaultIonCaster caster,
                                      StatsAccumulator stats) throws TransformException {
        if (bucket.skip) {
            return null;
        }
        IonStruct output = IonValueUtils.system().newEmptyStruct();
        for (String field : groupByFields) {
            IonValue value = bucket.groupValues.get(field);
            output.put(field, IonValueUtils.cloneValue(value));
        }

        for (AggregateMapping mapping : mappings) {
            AggregateState state = bucket.states.get(mapping.targetField);
            IonValue aggregated = state.result();
            if (IonValueUtils.isNull(aggregated)) {
                output.put(mapping.targetField, IonValueUtils.nullValue());
                continue;
            }
            try {
                IonValue casted = aggregated;
                if (mapping.type != null) {
                    casted = caster.cast(aggregated, mapping.type);
                }
                output.put(mapping.targetField, IonValueUtils.cloneValue(casted));
            } catch (CastException e) {
                stats.failed++;
                if (onError == TransformOptions.OnErrorMode.FAIL) {
                    throw new TransformException(e.getMessage(), e);
                }
                if (onError == TransformOptions.OnErrorMode.SKIP) {
                    return null;
                }
                if (onError == TransformOptions.OnErrorMode.NULL) {
                    output.put(mapping.targetField, IonValueUtils.nullValue());
                }
            }
        }
        stats.groups++;
        return output;
    }

    private void groupRecord(IonStruct record,
                             List<String> groupByFields,
                             List<AggregateMapping> mappings,
                             Map<GroupKey, GroupBucket> grouped,
                             StatsAccumulator stats,
                             DefaultExpressionEngine expressionEngine) throws TransformException {
        stats.processed++;
        GroupKey key = buildGroupKey(record, groupByFields);
        GroupBucket bucket = grouped.computeIfAbsent(key, k -> new GroupBucket(record, groupByFields, mappings));
        if (bucket.skip) {
            return;
        }
        for (AggregateMapping mapping : mappings) {
            AggregateState state = bucket.states.get(mapping.targetField);
            if (state.isFinal()) {
                continue;
            }
            try {
                state.update(record, expressionEngine);
            } catch (ExpressionException | TransformException | CastException e) {
                stats.failed++;
                if (onError == TransformOptions.OnErrorMode.FAIL) {
                    throw new TransformException(e.getMessage(), e);
                }
                if (onError == TransformOptions.OnErrorMode.SKIP) {
                    bucket.skip = true;
                    return;
                }
                if (onError == TransformOptions.OnErrorMode.NULL) {
                    state.forceNull();
                }
            }
        }
    }

    private AggregateFunction parseAggregateFunction(String expression) throws TransformException {
        String trimmed = expression == null ? "" : expression.trim();
        if ("count()".equals(trimmed)) {
            return new AggregateFunction(AggregateFunctionType.COUNT, null);
        }
        if (trimmed.startsWith("sum(")) {
            return new AggregateFunction(AggregateFunctionType.SUM, extractArgument(expression, "sum"));
        }
        if (trimmed.startsWith("min(")) {
            return new AggregateFunction(AggregateFunctionType.MIN, extractArgument(expression, "min"));
        }
        if (trimmed.startsWith("max(")) {
            return new AggregateFunction(AggregateFunctionType.MAX, extractArgument(expression, "max"));
        }
        if (trimmed.startsWith("avg(")) {
            return new AggregateFunction(AggregateFunctionType.AVG, extractArgument(expression, "avg"));
        }
        if (trimmed.startsWith("first(")) {
            return new AggregateFunction(AggregateFunctionType.FIRST, extractArgument(expression, "first"));
        }
        if (trimmed.startsWith("last(")) {
            return new AggregateFunction(AggregateFunctionType.LAST, extractArgument(expression, "last"));
        }
        throw new TransformException("Unsupported aggregate expression: " + expression);
    }

    private String extractArgument(String expression, String name) throws TransformException {
        int start = expression.indexOf('(');
        int end = expression.lastIndexOf(')');
        if (start < 0 || end < 0 || end < start) {
            throw new TransformException(name + " requires an argument");
        }
        String arg = expression.substring(start + 1, end).trim();
        if (arg.isEmpty()) {
            throw new TransformException(name + " requires an argument");
        }
        return arg;
    }

    private GroupKey buildGroupKey(IonStruct record, List<String> groupByFields) throws TransformException {
        List<Object> key = new ArrayList<>();
        for (String field : groupByFields) {
            IonValue value = record.get(field);
            key.add(IonValueUtils.toJavaValue(value));
        }
        return new GroupKey(key);
    }


    private Object unwrapIonList(IonList list) {
        if (list == null || list.isEmpty()) {
            return List.of();
        }
        if (list.size() == 1) {
            IonValue value = list.get(0);
            if (value instanceof IonStruct || value instanceof IonList) {
                return value;
            }
        }
        return list;
    }

    private IonStruct asStruct(IonValue value) throws TransformException {
        if (value instanceof IonStruct struct) {
            return struct;
        }
        throw new TransformException("Expected struct record, got " + (value == null ? "null" : value.getType()));
    }

    @Builder
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AggregateDefinition {
        @Schema(title = "Expression")
        private String expr;

        @Schema(title = "Ion type")
        private IonTypeName type;

        @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
        public static AggregateDefinition from(Object value) {
            if (value == null) {
                return null;
            }
            if (value instanceof String stringValue) {
                return AggregateDefinition.builder().expr(stringValue).build();
            }
            if (value instanceof Map<?, ?> map) {
                Object exprValue = map.get("expr");
                Object typeValue = map.get("type");
                IonTypeName type = null;
                if (typeValue instanceof IonTypeName ionTypeName) {
                    type = ionTypeName;
                } else if (typeValue instanceof String typeString) {
                    type = IonTypeName.valueOf(typeString);
                }
                return AggregateDefinition.builder()
                    .expr(exprValue == null ? null : String.valueOf(exprValue))
                    .type(type)
                    .build();
            }
            throw new IllegalArgumentException("Unsupported aggregate definition: " + value);
        }
    }

    public enum OutputMode {
        AUTO,
        RECORDS,
        STORE
    }

    private record AggregateMapping(String targetField, AggregateFunction function, IonTypeName type) {
    }

    private enum AggregateFunctionType {
        COUNT,
        SUM,
        MIN,
        MAX,
        AVG,
        FIRST,
        LAST
    }

    private record AggregateFunction(AggregateFunctionType type, String arg) {
    }

    private record GroupKey(List<Object> values) {
        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof GroupKey that)) {
                return false;
            }
            return Objects.equals(values, that.values);
        }

        @Override
        public int hashCode() {
            return Objects.hash(values);
        }
    }

    private static final class GroupBucket {
        private final Map<String, IonValue> groupValues = new LinkedHashMap<>();
        private final Map<String, AggregateState> states = new LinkedHashMap<>();
        private boolean skip;

        private GroupBucket(IonStruct record, List<String> groupByFields, List<AggregateMapping> mappings) {
            for (String field : groupByFields) {
                groupValues.put(field, record.get(field));
            }
            for (AggregateMapping mapping : mappings) {
                states.put(mapping.targetField, new AggregateState(mapping.function));
            }
        }
    }

    private static final class AggregateState {
        private final AggregateFunction function;
        private BigDecimal sum;
        private ComparableValue min;
        private ComparableValue max;
        private long count;
        private IonValue first;
        private IonValue last;
        private boolean forceNull;

        private AggregateState(AggregateFunction function) {
            this.function = function;
        }

        private boolean isFinal() {
            return forceNull;
        }

        private void forceNull() {
            forceNull = true;
        }

        private void update(IonStruct record, DefaultExpressionEngine expressionEngine) throws ExpressionException, TransformException, CastException {
            switch (function.type) {
                case COUNT -> count++;
                case SUM -> applyDecimal(record, expressionEngine, this::addToSum);
                case MIN -> applyComparable(record, expressionEngine, this::updateMin);
                case MAX -> applyComparable(record, expressionEngine, this::updateMax);
                case AVG -> applyDecimal(record, expressionEngine, this::addToAvg);
                case FIRST -> applyValue(record, expressionEngine, this::updateFirst);
                case LAST -> applyValue(record, expressionEngine, this::updateLast);
            }
        }

        private IonValue result() {
            if (forceNull) {
                return IonValueUtils.nullValue();
            }
            return switch (function.type) {
                case COUNT -> IonValueUtils.system().newInt(count);
                case SUM -> IonValueUtils.system().newDecimal(sum == null ? BigDecimal.ZERO : sum);
                case MIN -> min == null ? IonValueUtils.nullValue() : IonValueUtils.cloneValue(min.value);
                case MAX -> max == null ? IonValueUtils.nullValue() : IonValueUtils.cloneValue(max.value);
                case AVG -> {
                    if (count == 0) {
                        yield IonValueUtils.nullValue();
                    }
                    BigDecimal total = sum == null ? BigDecimal.ZERO : sum;
                    yield IonValueUtils.system().newDecimal(total.divide(BigDecimal.valueOf(count), java.math.RoundingMode.HALF_UP));
                }
                case FIRST -> first == null ? IonValueUtils.nullValue() : IonValueUtils.cloneValue(first);
                case LAST -> last == null ? IonValueUtils.nullValue() : IonValueUtils.cloneValue(last);
            };
        }

        private void applyDecimal(IonStruct record,
                                  DefaultExpressionEngine expressionEngine,
                                  java.util.function.BiConsumer<BigDecimal, AggregateState> consumer) throws ExpressionException, TransformException, CastException {
            IonValue value = expressionEngine.evaluate(function.arg, record);
            if (IonValueUtils.isNull(value)) {
                return;
            }
            BigDecimal decimal = IonValueUtils.asDecimal(value);
            if (decimal == null) {
                return;
            }
            consumer.accept(decimal, this);
        }

        private void addToSum(BigDecimal value, AggregateState state) {
            state.sum = state.sum == null ? value : state.sum.add(value);
        }

        private void updateMin(ComparableValue value, AggregateState state) throws CastException {
            if (state.min == null) {
                state.min = value;
                return;
            }
            state.min = state.min.compareTo(value) <= 0 ? state.min : value;
        }

        private void updateMax(ComparableValue value, AggregateState state) throws CastException {
            if (state.max == null) {
                state.max = value;
                return;
            }
            state.max = state.max.compareTo(value) >= 0 ? state.max : value;
        }

        private void addToAvg(BigDecimal value, AggregateState state) {
            state.sum = state.sum == null ? value : state.sum.add(value);
            state.count++;
        }

        private void applyValue(IonStruct record,
                                DefaultExpressionEngine expressionEngine,
                                java.util.function.BiConsumer<IonValue, AggregateState> consumer) throws ExpressionException, TransformException {
            IonValue value = expressionEngine.evaluate(function.arg, record);
            if (IonValueUtils.isNull(value)) {
                return;
            }
            consumer.accept(value, this);
        }

        private void applyComparable(IonStruct record,
                                     DefaultExpressionEngine expressionEngine,
                                     ComparableConsumer consumer) throws ExpressionException, TransformException, CastException {
            IonValue value = expressionEngine.evaluate(function.arg, record);
            if (IonValueUtils.isNull(value)) {
                return;
            }
            consumer.accept(toComparable(value), this);
        }

        private ComparableValue toComparable(IonValue value) throws CastException {
            if (value instanceof com.amazon.ion.IonTimestamp ionTimestamp) {
                return ComparableValue.forInstant(
                    value,
                    java.time.Instant.ofEpochMilli(ionTimestamp.timestampValue().getMillis())
                );
            }
            if (value instanceof com.amazon.ion.IonString ionString) {
                return ComparableValue.forString(value, ionString.stringValue());
            }
            BigDecimal decimal = IonValueUtils.asDecimal(value);
            if (decimal == null) {
                throw new CastException("Expected numeric, timestamp, or string value, got " + value.getType());
            }
            return ComparableValue.forDecimal(IonValueUtils.system().newDecimal(decimal), decimal);
        }

        private void updateFirst(IonValue value, AggregateState state) {
            if (state.first == null) {
                state.first = IonValueUtils.cloneValue(value);
            }
        }

        private void updateLast(IonValue value, AggregateState state) {
            state.last = IonValueUtils.cloneValue(value);
        }
    }

    private interface ComparableConsumer {
        void accept(ComparableValue value, AggregateState state) throws CastException;
    }

    private static final class ComparableValue {
        private final ComparableType type;
        private final IonValue value;
        private final Object key;

        private ComparableValue(ComparableType type, IonValue value, Object key) {
            this.type = type;
            this.value = value;
            this.key = key;
        }

        static ComparableValue forDecimal(IonValue value, BigDecimal key) {
            return new ComparableValue(ComparableType.DECIMAL, value, key);
        }

        static ComparableValue forInstant(IonValue value, java.time.Instant key) {
            return new ComparableValue(ComparableType.TIMESTAMP, value, key);
        }

        static ComparableValue forString(IonValue value, String key) {
            return new ComparableValue(ComparableType.STRING, value, key);
        }

        int compareTo(ComparableValue other) throws CastException {
            if (other == null) {
                return 1;
            }
            if (this.type != other.type) {
                throw new CastException("Mismatched types for min/max: " + this.type + " vs " + other.type);
            }
            return switch (this.type) {
                case DECIMAL -> ((BigDecimal) this.key).compareTo((BigDecimal) other.key);
                case TIMESTAMP -> ((java.time.Instant) this.key).compareTo((java.time.Instant) other.key);
                case STRING -> ((String) this.key).compareTo((String) other.key);
            };
        }
    }

    private enum ComparableType {
        DECIMAL,
        TIMESTAMP,
        STRING
    }

    private static final class StatsAccumulator {
        private int processed;
        private int groups;
        private int failed;
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
            title = "Aggregated records",
            description = "JSON-safe records when output mode is RECORDS or AUTO resolves to RECORDS."
        )
        private final List<Object> records;
    }
}
