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
import io.kestra.plugin.transform.engine.TransformStats;
import io.kestra.plugin.transform.expression.DefaultExpressionEngine;
import io.kestra.plugin.transform.expression.ExpressionException;
import io.kestra.plugin.transform.ion.IonValueUtils;
import io.kestra.plugin.transform.util.OutputFormat;
import io.kestra.plugin.transform.util.TransformProfiler;
import io.kestra.plugin.transform.util.TransformTaskSupport;
import io.kestra.plugin.transform.util.TransformException;
import io.kestra.plugin.transform.util.TransformOptions;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
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
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Explode records",
    description = """
        Expand array fields into multiple records without scripts.
        """
)
@Plugin(
    examples = {
        @io.kestra.core.models.annotations.Example(
            title = "Explode items into rows",
            full = true,
            code = """
                id: unnest_items
                namespace: company.team

                tasks:
                  - id: fetch
                    type: io.kestra.plugin.core.output.OutputValues
                    values:
                      records:
                        - order_id: o1
                          items:
                            - sku: a
                            - sku: b

                  - id: unnest
                    type: io.kestra.plugin.transform.Unnest
                    from: "{{ outputs.fetch.values.records }}"
                    path: items[]
                    as: item
                    keepOriginalFields: true
                    onError: FAIL
                """
        )
    },
    metrics = {
        @Metric(name = "processed", type = Counter.TYPE),
        @Metric(name = "failed", type = Counter.TYPE),
        @Metric(name = "dropped", type = Counter.TYPE)
    }
)
public class Unnest extends Task implements RunnableTask<Unnest.Output> {
    @NotNull
    @Schema(
        title = "Input records",
        description = """
        Ion list or struct to transform, or a storage URI pointing to an Ion file.
        """
    )
    private Property<Object> from;

    @NotNull
    @Schema(
        title = "Array path",
        description = """
        Path expression to the array to explode (e.g., items[]).
        """
    )
    private Property<String> path;

    @NotNull
    @Schema(
        title = "Output field name",
        description = """
        Field name that holds the exploded element.
        """
    )
    private Property<String> as;

    @Builder.Default
    @Schema(
        title = "Keep original fields",
        description = """
        Keeps original fields other than the exploded array field.
        """
    )
    private Property<Boolean> keepOriginalFields = Property.ofValue(true);

    @Builder.Default
    @Schema(
        title = "On error behavior",
        description = """
        FAIL stops the task on unnest errors, SKIP drops the current record, and NULL emits the record with a null exploded field.
        """
    )
    private Property<TransformOptions.OnErrorMode> onError = Property.ofValue(TransformOptions.OnErrorMode.FAIL);

    @Builder.Default
    @Schema(
        title = "Output format",
        description = """
        Experimental: TEXT or BINARY. Only transform tasks can read binary Ion. Use TEXT as the final step.
        """
    )
    private Property<OutputFormat> outputFormat = Property.ofValue(OutputFormat.TEXT);

    @Schema(
        title = "Output type",
        description = """
        AUTO stores to internal storage when the input is a storage URI; otherwise it returns records.
        """
    )
    @Builder.Default
    private Property<OutputMode> outputType = Property.ofValue(OutputMode.AUTO);

    @Override
    public Output run(RunContext runContext) throws Exception {
        if (from == null) {
            throw new TransformException("from is required");
        }
        var resolvedInput = TransformTaskSupport.resolveInput(runContext, from);
        var rOnError = runContext.render(this.onError).as(TransformOptions.OnErrorMode.class).orElseThrow();
        var rOutputFormat = runContext.render(this.outputFormat).as(OutputFormat.class).orElseThrow();
        var rOutputType = runContext.render(this.outputType).as(OutputMode.class).orElseThrow();
        var rKeepOriginalFields = runContext.render(this.keepOriginalFields).as(Boolean.class).orElse(true);

        var pathExpr = runContext.render(path).as(String.class).orElse(null);
        if (pathExpr == null || pathExpr.isBlank()) {
            throw new TransformException("path is required");
        }
        var asField = runContext.render(as).as(String.class).orElse(null);
        if (asField == null || asField.isBlank()) {
            throw new TransformException("as is required");
        }

        var expressionEngine = new DefaultExpressionEngine();
        var stats = new StatsAccumulator();

        var effectiveOutput = rOutputType == OutputMode.AUTO
            ? (resolvedInput.fromStorage() ? OutputMode.STORE : OutputMode.RECORDS)
            : rOutputType;

        var pathSegments = parsePathSegments(pathExpr);

        if (resolvedInput.fromStorage() && effectiveOutput == OutputMode.STORE) {
            var storedUri = unnestStreamToStorage(
                runContext,
                resolvedInput.storageUri(),
                pathExpr,
                asField,
                pathSegments,
                expressionEngine,
                stats,
                rOnError,
                rOutputFormat,
                rKeepOriginalFields
            );
            runContext.metric(Counter.of("processed", stats.processed))
                .metric(Counter.of("failed", stats.failed))
                .metric(Counter.of("dropped", stats.dropped));
            return Output.builder()
                .uri(storedUri.toString())
                .build();
        }

        var records = TransformTaskSupport.normalizeRecords(resolveInMemory(runContext, resolvedInput));
        if (effectiveOutput == OutputMode.STORE) {
            var storedUri = storeRecords(
                runContext,
                records,
                pathExpr,
                asField,
                pathSegments,
                expressionEngine,
                stats,
                rOnError,
                rOutputFormat,
                rKeepOriginalFields
            );
            runContext.metric(Counter.of("processed", stats.processed))
                .metric(Counter.of("failed", stats.failed))
                .metric(Counter.of("dropped", stats.dropped));
            return Output.builder()
                .uri(storedUri.toString())
                .build();
        }

        var rendered = expandToRecords(records, pathExpr, asField, pathSegments, expressionEngine, stats, rOnError, rKeepOriginalFields);
        runContext.metric(Counter.of("processed", stats.processed))
            .metric(Counter.of("failed", stats.failed))
            .metric(Counter.of("dropped", stats.dropped));
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

    private List<Object> expandToRecords(List<IonStruct> records,
                                         String pathExpr,
                                         String asField,
                                         List<String> pathSegments,
                                         DefaultExpressionEngine expressionEngine,
                                         StatsAccumulator stats,
                                         TransformOptions.OnErrorMode onError,
                                         boolean keepOriginalFields) throws TransformException {
        List<Object> outputRecords = new ArrayList<>();
        for (int i = 0; i < records.size(); i++) {
            IonStruct record = records.get(i);
            stats.processed++;
            try {
                IonValue evaluated = expressionEngine.evaluate(pathExpr, record);
                if (IonValueUtils.isNull(evaluated)) {
                    continue;
                }
                if (!(evaluated instanceof IonList list)) {
                    throw new TransformException("Expected list at path: " + pathExpr);
                }
                if (list.isEmpty()) {
                    continue;
                }
                for (IonValue element : list) {
                    IonStruct output = buildOutputRecord(record, asField, element, keepOriginalFields, pathSegments);
                    outputRecords.add(IonValueUtils.toJavaValue(output));
                }
            } catch (ExpressionException | TransformException e) {
                stats.fail(i, "path", e.getMessage());
                if (onError == TransformOptions.OnErrorMode.FAIL) {
                    throw new TransformException(e.getMessage(), e);
                }
                if (onError == TransformOptions.OnErrorMode.SKIP) {
                    stats.dropped++;
                    continue;
                }
                if (onError == TransformOptions.OnErrorMode.NULL) {
                    IonStruct output = buildOutputRecord(record, asField, IonValueUtils.nullValue(), keepOriginalFields, pathSegments);
                    outputRecords.add(IonValueUtils.toJavaValue(output));
                }
            }
        }
        return outputRecords;
    }

    private URI storeRecords(RunContext runContext,
                             List<IonStruct> records,
                             String pathExpr,
                             String asField,
                             List<String> pathSegments,
                             DefaultExpressionEngine expressionEngine,
                             StatsAccumulator stats,
                             TransformOptions.OnErrorMode onError,
                             OutputFormat outputFormat,
                             boolean keepOriginalFields) throws TransformException {
        String name = "unnest-" + UUID.randomUUID() + ".ion";
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
                        IonValue evaluated = expressionEngine.evaluate(pathExpr, record);
                        if (profile) {
                            TransformProfiler.addTransformNs(System.nanoTime() - transformStart);
                        }
                        if (IonValueUtils.isNull(evaluated)) {
                            continue;
                        }
                        if (!(evaluated instanceof IonList list)) {
                            throw new TransformException("Expected list at path: " + pathExpr);
                        }
                        if (list.isEmpty()) {
                            continue;
                        }
                        for (IonValue element : list) {
                            long elementStart = profile ? System.nanoTime() : 0L;
                            IonStruct output = buildOutputRecord(record, asField, element, keepOriginalFields, pathSegments);
                            if (profile) {
                                TransformProfiler.addTransformNs(System.nanoTime() - elementStart);
                            }
                            long writeStart = profile ? System.nanoTime() : 0L;
                            output.writeTo(writer);
                            TransformTaskSupport.writeDelimiter(outputStream, outputFormat);
                            if (profile) {
                                TransformProfiler.addWriteNs(System.nanoTime() - writeStart);
                            }
                        }
                    } catch (ExpressionException | TransformException e) {
                        stats.fail(i, "path", e.getMessage());
                        if (onError == TransformOptions.OnErrorMode.FAIL) {
                            throw new TransformException(e.getMessage(), e);
                        }
                        if (onError == TransformOptions.OnErrorMode.SKIP) {
                            stats.dropped++;
                            continue;
                        }
                        if (onError == TransformOptions.OnErrorMode.NULL) {
                            long elementStart = profile ? System.nanoTime() : 0L;
                            IonStruct output = buildOutputRecord(record, asField, IonValueUtils.nullValue(), keepOriginalFields, pathSegments);
                            if (profile) {
                                TransformProfiler.addTransformNs(System.nanoTime() - elementStart);
                            }
                            long writeStart = profile ? System.nanoTime() : 0L;
                            output.writeTo(writer);
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
            throw new TransformException("Unable to store unnested records", e);
        }
    }

    private URI unnestStreamToStorage(RunContext runContext,
                                      URI uri,
                                      String pathExpr,
                                      String asField,
                                      List<String> pathSegments,
                                      DefaultExpressionEngine expressionEngine,
                                      StatsAccumulator stats,
                                      TransformOptions.OnErrorMode onError,
                                      OutputFormat outputFormat,
                                      boolean keepOriginalFields) throws TransformException {
        String name = "unnest-" + UUID.randomUUID() + ".ion";
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
                int index = 0;
                while (iterator.hasNext()) {
                    IonValue value = iterator.next();
                    if (value instanceof IonList list) {
                        for (IonValue element : list) {
                            index = processStreamRecord(
                                element,
                                index,
                                pathExpr,
                                asField,
                                pathSegments,
                                expressionEngine,
                                stats,
                                writer,
                                outputStream,
                                profile,
                                onError,
                                outputFormat,
                                keepOriginalFields
                            );
                        }
                    } else {
                        index = processStreamRecord(
                            value,
                            index,
                            pathExpr,
                            asField,
                            pathSegments,
                            expressionEngine,
                            stats,
                            writer,
                            outputStream,
                            profile,
                            onError,
                            outputFormat,
                            keepOriginalFields
                        );
                    }
                }
                writer.finish();
            }
            return runContext.storage().putFile(outputPath.toFile(), name);
        } catch (IOException e) {
            throw new TransformException("Unable to store unnested records", e);
        }
    }

    private int processStreamRecord(IonValue value,
                                    int index,
                                    String pathExpr,
                                    String asField,
                                    List<String> pathSegments,
                                    DefaultExpressionEngine expressionEngine,
                                    StatsAccumulator stats,
                                    IonWriter writer,
                                    OutputStream outputStream,
                                    boolean profile,
                                    TransformOptions.OnErrorMode onError,
                                    OutputFormat outputFormat,
                                    boolean keepOriginalFields) throws TransformException, IOException {
        IonStruct record = asStruct(value);
        stats.processed++;
        try {
            long transformStart = profile ? System.nanoTime() : 0L;
            IonValue evaluated = expressionEngine.evaluate(pathExpr, record);
            if (profile) {
                TransformProfiler.addTransformNs(System.nanoTime() - transformStart);
            }
            if (IonValueUtils.isNull(evaluated)) {
                return index + 1;
            }
            if (!(evaluated instanceof IonList list)) {
                throw new TransformException("Expected list at path: " + pathExpr);
            }
            if (list.isEmpty()) {
                return index + 1;
            }
            for (IonValue element : list) {
                long elementStart = profile ? System.nanoTime() : 0L;
                IonStruct output = buildOutputRecord(record, asField, element, keepOriginalFields, pathSegments);
                if (profile) {
                    TransformProfiler.addTransformNs(System.nanoTime() - elementStart);
                }
                long writeStart = profile ? System.nanoTime() : 0L;
                output.writeTo(writer);
                TransformTaskSupport.writeDelimiter(outputStream, outputFormat);
                if (profile) {
                    TransformProfiler.addWriteNs(System.nanoTime() - writeStart);
                }
            }
        } catch (ExpressionException | TransformException e) {
            stats.fail(index, "path", e.getMessage());
            if (onError == TransformOptions.OnErrorMode.FAIL) {
                throw new TransformException(e.getMessage(), e);
            }
            if (onError == TransformOptions.OnErrorMode.SKIP) {
                stats.dropped++;
                return index + 1;
            }
            if (onError == TransformOptions.OnErrorMode.NULL) {
                long elementStart = profile ? System.nanoTime() : 0L;
                IonStruct output = buildOutputRecord(record, asField, IonValueUtils.nullValue(), keepOriginalFields, pathSegments);
                if (profile) {
                    TransformProfiler.addTransformNs(System.nanoTime() - elementStart);
                }
                long writeStart = profile ? System.nanoTime() : 0L;
                output.writeTo(writer);
                TransformTaskSupport.writeDelimiter(outputStream, outputFormat);
                if (profile) {
                    TransformProfiler.addWriteNs(System.nanoTime() - writeStart);
                }
            }
        }
        return index + 1;
    }

    private IonStruct buildOutputRecord(IonStruct input,
                                        String asField,
                                        IonValue element,
                                        boolean keepOriginalFields,
                                        List<String> pathSegments) {
        IonStruct output = IonValueUtils.system().newEmptyStruct();
        if (keepOriginalFields) {
            for (IonValue value : input) {
                output.put(value.getFieldName(), IonValueUtils.cloneValue(value));
            }
            removeArrayField(output, pathSegments);
        }
        output.put(asField, IonValueUtils.cloneValue(element == null ? IonValueUtils.nullValue() : element));
        return output;
    }

    private void removeArrayField(IonStruct output, List<String> pathSegments) {
        if (pathSegments == null || pathSegments.isEmpty()) {
            return;
        }
        IonStruct current = output;
        for (int i = 0; i < pathSegments.size() - 1; i++) {
            IonValue next = current.get(pathSegments.get(i));
            if (!(next instanceof IonStruct struct)) {
                return;
            }
            current = struct;
        }
        current.remove(pathSegments.get(pathSegments.size() - 1));
    }

    private List<String> parsePathSegments(String pathExpr) {
        if (pathExpr == null) {
            return null;
        }
        String trimmed = pathExpr.trim();
        if (trimmed.isEmpty() || trimmed.contains("(") || trimmed.contains(" ")) {
            return null;
        }
        String[] segments = trimmed.split("\\.");
        List<String> names = new ArrayList<>();
        for (int i = 0; i < segments.length; i++) {
            String segment = segments[i].trim();
            if (segment.isEmpty()) {
                return null;
            }
            boolean isArray = segment.endsWith("[]");
            String name = isArray ? segment.substring(0, segment.length() - 2) : segment;
            if (name.isEmpty()) {
                return null;
            }
            if (isArray && i != segments.length - 1) {
                return null;
            }
            names.add(name);
        }
        return names;
    }

    private static final class StatsAccumulator {
        private int processed;
        private int failed;
        private int dropped;
        private final Map<String, String> fieldErrors = new HashMap<>();

        private void fail(int index, String field, String message) {
            failed++;
            fieldErrors.put(index + "." + field, message);
        }

        private TransformStats snapshot() {
            return new TransformStats(processed, failed, dropped, new HashMap<>(fieldErrors));
        }
    }


    private IonStruct asStruct(IonValue value) throws TransformException {
        if (value instanceof IonStruct struct) {
            return struct;
        }
        throw new TransformException("Expected struct record, got " + (value == null ? "null" : value.getType()));
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
            return OutputMode.valueOf(raw.toUpperCase(Locale.ROOT));
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Stored Ion file URI",
            description = """
        URI to the stored Ion file when output mode is STORE or AUTO resolves to STORE.
        """
        )
        private final String uri;

        @Schema(
            title = "Unnested records",
            description = """
        JSON-safe records when output mode is RECORDS or AUTO resolves to RECORDS.
        """
        )
        private final List<Object> records;
    }
}
