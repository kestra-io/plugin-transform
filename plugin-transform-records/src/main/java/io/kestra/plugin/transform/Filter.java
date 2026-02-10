package io.kestra.plugin.transform;

import com.amazon.ion.IonBool;
import com.amazon.ion.IonList;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.transform.expression.DefaultExpressionEngine;
import io.kestra.plugin.transform.expression.ExpressionException;
import io.kestra.plugin.transform.ion.IonValueUtils;
import io.kestra.plugin.transform.util.OutputFormat;
import io.kestra.plugin.transform.util.TransformProfiler;
import io.kestra.plugin.transform.util.TransformTaskSupport;
import io.kestra.plugin.transform.util.TransformException;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Filter records",
    description = "Keep or drop records based on a boolean expression."
)
@Plugin(
    examples = {
        @io.kestra.core.models.annotations.Example(
            title = "Keep active customers",
            code = {
                "from: \"{{ outputs.normalize.records }}\"",
                "where: is_active && total_spent > 100",
                "onError: SKIP"
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
public class Filter extends Task implements RunnableTask<Filter.Output> {
    @Schema(
        title = "Input records",
        description = "Ion list or struct to transform, or a storage URI pointing to an Ion file."
    )
    private Property<Object> from;

    @Schema(
        title = "Filter expression",
        description = "Boolean expression evaluated on each record."
    )
    private Property<String> where;

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
        description = "AUTO stores to internal storage when the input is a storage URI; otherwise it returns records."
    )
    @Builder.Default
    private OutputMode output = OutputMode.AUTO;

    @Override
    public Output run(RunContext runContext) throws Exception {
        TransformTaskSupport.ResolvedInput resolvedInput = TransformTaskSupport.resolveInput(runContext, from);

        String whereExpr = runContext.render(where).as(String.class).orElse(null);
        if (whereExpr == null || whereExpr.isBlank()) {
            throw new TransformException("where is required");
        }

        DefaultExpressionEngine expressionEngine = new DefaultExpressionEngine();
        StatsAccumulator stats = new StatsAccumulator();

        OutputMode effectiveOutput = output == OutputMode.AUTO
            ? (resolvedInput.fromStorage() ? OutputMode.STORE : OutputMode.RECORDS)
            : output;

        if (resolvedInput.fromStorage() && effectiveOutput == OutputMode.STORE) {
            URI storedUri = filterStreamToStorage(runContext, resolvedInput.storageUri(), whereExpr, expressionEngine, stats);
            runContext.metric(Counter.of("processed", stats.processed))
                .metric(Counter.of("passed", stats.passed))
                .metric(Counter.of("dropped", stats.dropped))
                .metric(Counter.of("failed", stats.failed));
            return Output.builder()
                .uri(storedUri.toString())
                .build();
        }

        List<IonStruct> records = TransformTaskSupport.normalizeRecords(resolveInMemory(runContext, resolvedInput));
        if (effectiveOutput == OutputMode.STORE) {
            URI storedUri = storeRecords(runContext, records, whereExpr, expressionEngine, stats);
            runContext.metric(Counter.of("processed", stats.processed))
                .metric(Counter.of("passed", stats.passed))
                .metric(Counter.of("dropped", stats.dropped))
                .metric(Counter.of("failed", stats.failed));
            return Output.builder()
                .uri(storedUri.toString())
                .build();
        }

        List<Object> rendered = filterToRecords(records, whereExpr, expressionEngine, stats);
        runContext.metric(Counter.of("processed", stats.processed))
            .metric(Counter.of("passed", stats.passed))
            .metric(Counter.of("dropped", stats.dropped))
            .metric(Counter.of("failed", stats.failed));
        return Output.builder()
            .records(rendered)
            .build();
    }

    private Object resolveInMemory(RunContext runContext, TransformTaskSupport.ResolvedInput resolvedInput) throws TransformException {
        if (!resolvedInput.fromStorage()) {
            return resolvedInput.value();
        }
        return TransformTaskSupport.loadIonFromStorage(runContext, resolvedInput.storageUri());
    }

    private List<Object> filterToRecords(List<IonStruct> records,
                                         String whereExpr,
                                         DefaultExpressionEngine expressionEngine,
                                         StatsAccumulator stats) throws TransformException {
        List<Object> outputRecords = new ArrayList<>();
        for (int i = 0; i < records.size(); i++) {
            IonStruct record = records.get(i);
            stats.processed++;
            try {
                Boolean decision = evaluateBoolean(whereExpr, record, expressionEngine);
                if (decision) {
                    stats.passed++;
                    outputRecords.add(IonValueUtils.toJavaValue(record));
                } else {
                    stats.dropped++;
                }
            } catch (ExpressionException | TransformException e) {
                stats.failed++;
                if (onError == OnErrorMode.FAIL) {
                    throw new TransformException(e.getMessage(), e);
                }
                if (onError == OnErrorMode.SKIP) {
                    stats.dropped++;
                    continue;
                }
                if (onError == OnErrorMode.KEEP) {
                    stats.passed++;
                    outputRecords.add(IonValueUtils.toJavaValue(record));
                }
            }
        }
        return outputRecords;
    }

    private URI storeRecords(RunContext runContext,
                             List<IonStruct> records,
                             String whereExpr,
                             DefaultExpressionEngine expressionEngine,
                             StatsAccumulator stats) throws TransformException {
        String name = "filter-" + UUID.randomUUID() + ".ion";
        try {
            java.nio.file.Path outputPath = runContext.workingDir().createTempFile(".ion");
            try (OutputStream outputStream = TransformTaskSupport.wrapCompression(
                TransformTaskSupport.bufferedOutput(outputPath));
                 IonWriter writer = TransformTaskSupport.createWriter(outputStream, outputFormat)) {
                boolean profile = TransformProfiler.isEnabled();
                for (int i = 0; i < records.size(); i++) {
                    IonStruct record = records.get(i);
                    stats.processed++;
                    try {
                        long transformStart = profile ? System.nanoTime() : 0L;
                        Boolean decision = evaluateBoolean(whereExpr, record, expressionEngine);
                        if (profile) {
                            TransformProfiler.addTransformNs(System.nanoTime() - transformStart);
                        }
                        if (decision) {
                            stats.passed++;
                            long writeStart = profile ? System.nanoTime() : 0L;
                            record.writeTo(writer);
                            TransformTaskSupport.writeDelimiter(outputStream, outputFormat);
                            if (profile) {
                                TransformProfiler.addWriteNs(System.nanoTime() - writeStart);
                            }
                        } else {
                            stats.dropped++;
                        }
                    } catch (ExpressionException | TransformException e) {
                        stats.failed++;
                        if (onError == OnErrorMode.FAIL) {
                            throw new TransformException(e.getMessage(), e);
                        }
                        if (onError == OnErrorMode.SKIP) {
                            stats.dropped++;
                            continue;
                        }
                        if (onError == OnErrorMode.KEEP) {
                            stats.passed++;
                            long writeStart = profile ? System.nanoTime() : 0L;
                            record.writeTo(writer);
                            TransformTaskSupport.writeDelimiter(outputStream, outputFormat);
                            if (profile) {
                                TransformProfiler.addWriteNs(System.nanoTime() - writeStart);
                            }
                        }
                    }
                }
                writer.finish();
            }
            return runContext.storage().putFile(outputPath.toFile(), name);
        } catch (IOException e) {
            throw new TransformException("Unable to store filtered records", e);
        }
    }

    private URI filterStreamToStorage(RunContext runContext,
                                      URI uri,
                                      String whereExpr,
                                      DefaultExpressionEngine expressionEngine,
                                      StatsAccumulator stats) throws TransformException {
        String name = "filter-" + UUID.randomUUID() + ".ion";
        InputStream inputStream;
        try {
            inputStream = runContext.storage().getFile(uri);
        } catch (IOException e) {
            throw new TransformException("Unable to read Ion file from storage: " + uri, e);
        }

        try (InputStream stream = inputStream) {
            java.nio.file.Path outputPath = runContext.workingDir().createTempFile(".ion");
            try (OutputStream outputStream = TransformTaskSupport.wrapCompression(
                TransformTaskSupport.bufferedOutput(outputPath));
                 IonWriter writer = TransformTaskSupport.createWriter(outputStream, outputFormat)) {
                boolean profile = TransformProfiler.isEnabled();
                Iterator<IonValue> iterator = IonValueUtils.system().iterate(stream);
                while (iterator.hasNext()) {
                    IonValue value = iterator.next();
                    if (value instanceof IonList list) {
                        for (IonValue element : list) {
                            filterStreamRecord(element, whereExpr, expressionEngine, stats, writer, outputStream, profile);
                        }
                    } else {
                        filterStreamRecord(value, whereExpr, expressionEngine, stats, writer, outputStream, profile);
                    }
                }
                writer.finish();
            }
            return runContext.storage().putFile(outputPath.toFile(), name);
        } catch (IOException e) {
            throw new TransformException("Unable to store filtered records", e);
        }
    }

    private void filterStreamRecord(IonValue value,
                                    String whereExpr,
                                    DefaultExpressionEngine expressionEngine,
                                    StatsAccumulator stats,
                                    IonWriter writer,
                                    OutputStream outputStream,
                                    boolean profile) throws TransformException, IOException {
        IonStruct record = asStruct(value);
        stats.processed++;
        try {
            long transformStart = profile ? System.nanoTime() : 0L;
            Boolean decision = evaluateBoolean(whereExpr, record, expressionEngine);
            if (profile) {
                TransformProfiler.addTransformNs(System.nanoTime() - transformStart);
            }
            if (decision) {
                stats.passed++;
                long writeStart = profile ? System.nanoTime() : 0L;
                record.writeTo(writer);
                TransformTaskSupport.writeDelimiter(outputStream, outputFormat);
                if (profile) {
                    TransformProfiler.addWriteNs(System.nanoTime() - writeStart);
                }
            } else {
                stats.dropped++;
            }
        } catch (ExpressionException | TransformException e) {
            stats.failed++;
            if (onError == OnErrorMode.FAIL) {
                throw new TransformException(e.getMessage(), e);
            }
            if (onError == OnErrorMode.SKIP) {
                stats.dropped++;
                return;
            }
            if (onError == OnErrorMode.KEEP) {
                stats.passed++;
                long writeStart = profile ? System.nanoTime() : 0L;
                record.writeTo(writer);
                TransformTaskSupport.writeDelimiter(outputStream, outputFormat);
                if (profile) {
                    TransformProfiler.addWriteNs(System.nanoTime() - writeStart);
                }
            }
        }
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


    private IonStruct asStruct(IonValue value) throws TransformException {
        if (value instanceof IonStruct struct) {
            return struct;
        }
        throw new TransformException("Expected struct record, got " + (value == null ? "null" : value.getType()));
    }

    public enum OnErrorMode {
        FAIL,
        SKIP,
        KEEP
    }

    public enum OutputMode {
        AUTO,
        RECORDS,
        STORE
    }

    private static final class StatsAccumulator {
        private int processed;
        private int passed;
        private int dropped;
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
            title = "Filtered records",
            description = "JSON-safe records when output mode is RECORDS or AUTO resolves to RECORDS."
        )
        private final List<Object> records;
    }
}
