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
import io.kestra.plugin.transform.engine.DefaultRecordTransformer;
import io.kestra.plugin.transform.engine.DefaultTransformTaskEngine;
import io.kestra.plugin.transform.engine.FieldMapping;
import io.kestra.plugin.transform.engine.TransformResult;
import io.kestra.plugin.transform.engine.TransformStats;
import io.kestra.plugin.transform.expression.DefaultExpressionEngine;
import io.kestra.plugin.transform.ion.DefaultIonCaster;
import io.kestra.plugin.transform.ion.IonTypeName;
import io.kestra.plugin.transform.ion.IonValueUtils;
import io.kestra.plugin.transform.util.OutputFormat;
import io.kestra.plugin.transform.util.TransformProfiler;
import io.kestra.plugin.transform.util.TransformTaskSupport;
import io.kestra.plugin.transform.util.TransformException;
import io.kestra.plugin.transform.util.TransformOptions;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
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
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.UUID;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Typed record mapping",
    description = """
        Declaratively map, cast, and derive Ion fields without scripts.
        """
)
@Plugin(
    examples = {
        @io.kestra.core.models.annotations.Example(
            title = "Normalize records",
            full = true,
            code = """
                id: map_normalize_records
                namespace: company.team

                tasks:
                  - id: fetch
                    type: io.kestra.plugin.core.output.OutputValues
                    values:
                      records:
                        - user:
                            id: c1
                          createdAt: "2026-01-01T00:00:00Z"
                          items:
                            - price: 10.5

                  - id: map
                    type: io.kestra.plugin.transform.Map
                    from: "{{ outputs.fetch.values.records }}"
                    fields:
                      customer_id:
                        expr: user.id
                        type: STRING
                      created_at:
                        expr: createdAt
                        type: TIMESTAMP
                      total:
                        expr: sum(items[].price)
                        type: DECIMAL
                    dropNulls: true
                    onError: SKIP
                """
        ),
        @io.kestra.core.models.annotations.Example(
            title = "Map DuckDB stored results",
            full = true,
            code = """
                id: map_duckdb_stored
                namespace: company.team

                tasks:
                  - id: query1
                    type: io.kestra.plugin.jdbc.duckdb.Query
                    sql: SELECT now() as "ts";
                    fetchType: STORE

                  - id: map
                    type: io.kestra.plugin.transform.Map
                    from: "{{ outputs.query1.uri }}"
                    outputType: RECORDS
                    fields:
                      ts:
                        expr: ts
                        type: TIMESTAMP
                """
        ),
        @io.kestra.core.models.annotations.Example(
            title = "Map direct output values",
            full = true,
            code = """
                id: map_direct_outputs
                namespace: company.team

                tasks:
                  - id: query1
                    type: io.kestra.plugin.jdbc.duckdb.Query
                    sql: SELECT 1 as "value" UNION ALL SELECT 2 as "value";
                    fetchType: FETCH

                  - id: map
                    type: io.kestra.plugin.transform.Map
                    from: "{{ outputs.query1.records }}"
                    fields:
                      value:
                        expr: value
                        type: INT
                """
        ),
        @io.kestra.core.models.annotations.Example(
            title = "Download JSON and transform",
            full = true,
            code = """
                id: map_http_download
                namespace: company.team

                tasks:
                  - id: download
                    type: io.kestra.plugin.core.http.Download
                    uri: https://dummyjson.com/products

                  - id: map
                    type: io.kestra.plugin.transform.Map
                    from: "{{ outputs.download.uri }}"
                    outputType: RECORDS
                    fields:
                      first_title:
                        expr: products[0].title
                        type: STRING
                """
        )
    },
    metrics = {
        @Metric(name = "processed", type = Counter.TYPE),
        @Metric(name = "failed", type = Counter.TYPE),
        @Metric(name = "dropped", type = Counter.TYPE)
    }
)
public class Map extends Task implements RunnableTask<Map.Output> {
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
        title = "Field mappings",
        description = """
        Target fields with expressions and types.
        """
    )
    private Property<java.util.Map<String, FieldDefinition>> fields;

    @Builder.Default
    @Schema(
        title = "Keep original fields",
        description = """
        Keeps input fields not explicitly mapped. If you map a_new: a, the original a is still kept.
        """
    )
    private Property<Boolean> keepOriginalFields = Property.ofValue(false);

    @Builder.Default
    @Schema(title = "Drop null fields")
    private Property<Boolean> dropNulls = Property.ofValue(true);

    @Builder.Default
    @Schema(
        title = "On error behavior",
        description = """
        FAIL stops the task on the first field error, SKIP drops the current record, and NULL writes null for failing fields.
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
        var rFields = runContext.render(this.fields).asMap(String.class, FieldDefinition.class);
        if (rFields == null || rFields.isEmpty()) {
            throw new TransformException("fields is required");
        }
        var rOnError = runContext.render(this.onError).as(TransformOptions.OnErrorMode.class).orElseThrow();
        var rOutputFormat = runContext.render(this.outputFormat).as(OutputFormat.class).orElseThrow();
        var rOutputType = runContext.render(this.outputType).as(OutputMode.class).orElseThrow();
        var rKeepOriginalFields = runContext.render(this.keepOriginalFields).as(Boolean.class).orElse(false);
        var rDropNulls = runContext.render(this.dropNulls).as(Boolean.class).orElse(true);

        var mappings = new ArrayList<FieldMapping>();
        for (Entry<String, FieldDefinition> entry : rFields.entrySet()) {
            var definition = entry.getValue();
            if (definition == null) {
                throw new TransformException("Field definition is required for '" + entry.getKey() + "'");
            }
            mappings.add(new FieldMapping(
                entry.getKey(),
                Objects.requireNonNull(definition.expr, "expr is required"),
                definition.type,
                definition.optional
            ));
        }

        var transformOptions = new TransformOptions(rKeepOriginalFields, rDropNulls, rOnError);
        var transformer = new DefaultRecordTransformer(
            mappings,
            new DefaultExpressionEngine(),
            new DefaultIonCaster(),
            transformOptions
        );
        var effectiveOutput = rOutputType == OutputMode.AUTO
            ? (resolvedInput.fromStorage() ? OutputMode.STORE : OutputMode.RECORDS)
            : rOutputType;

        if (resolvedInput.fromStorage() && effectiveOutput == OutputMode.STORE) {
            var streamResult = transformStreamToStorage(
                runContext,
                resolvedInput.storageUri(),
                transformer,
                rOutputFormat
            );
            runContext.metric(Counter.of("processed", streamResult.stats().processed()))
                .metric(Counter.of("failed", streamResult.stats().failed()))
                .metric(Counter.of("dropped", streamResult.stats().dropped()));
            return Output.builder()
                .uri(streamResult.uri().toString())
                .build();
        }

        var records = TransformTaskSupport.normalizeRecords(resolveInMemory(runContext, resolvedInput));
        var engine = new DefaultTransformTaskEngine(transformer);
        var result = engine.execute(records);

        runContext.metric(Counter.of("processed", result.stats().processed()))
            .metric(Counter.of("failed", result.stats().failed()))
            .metric(Counter.of("dropped", result.stats().dropped()));

        if (effectiveOutput == OutputMode.STORE) {
            var storedUri = storeRecords(runContext, result.records(), rOutputFormat);
            return Output.builder()
                .uri(storedUri.toString())
                .build();
        }

        var renderedRecords = new ArrayList<>();
        for (IonStruct record : result.records()) {
            renderedRecords.add(IonValueUtils.toJavaValue(record));
        }

        return Output.builder()
            .records(renderedRecords)
            .build();
    }

    private Object resolveInMemory(RunContext runContext, TransformTaskSupport.ResolvedInput resolvedInput) throws TransformException {
        if (!resolvedInput.fromStorage()) {
            return resolvedInput.value();
        }
        return TransformTaskSupport.loadIonFromStorage(runContext, resolvedInput.storageUri());
    }

    private StreamResult transformStreamToStorage(RunContext runContext,
                                                  URI uri,
                                                  DefaultRecordTransformer transformer,
                                                  OutputFormat outputFormat) throws TransformException {
        String name = "transform-" + UUID.randomUUID() + ".ion";
        java.util.Map<String, String> fieldErrors = new java.util.HashMap<>();
        int processed = 0;
        int failed = 0;
        int dropped = 0;
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
                Iterator<IonValue> iterator = IonValueUtils.system().iterate(stream);
                int index = 0;
                while (iterator.hasNext()) {
                    IonValue value = iterator.next();
                    if (value instanceof IonList list) {
                        for (IonValue element : list) {
                            StreamCounters counters = processStreamRecord(element, transformer, writer, outputStream, fieldErrors, index, outputFormat);
                            processed++;
                            if (counters.failed) {
                                failed++;
                            }
                            if (counters.dropped) {
                                dropped++;
                            }
                            index = counters.nextIndex;
                        }
                    } else {
                        StreamCounters counters = processStreamRecord(value, transformer, writer, outputStream, fieldErrors, index, outputFormat);
                        processed++;
                        if (counters.failed) {
                            failed++;
                        }
                        if (counters.dropped) {
                            dropped++;
                        }
                        index = counters.nextIndex;
                    }
                }
                writer.finish();
            }
            URI storedUri = runContext.storage().putFile(outputPath.toFile(), name);
            return new StreamResult(
                new TransformStats(processed, failed, dropped, fieldErrors),
                storedUri
            );
        } catch (IOException e) {
            throw new TransformException("Unable to store transformed records", e);
        }
    }

    private StreamCounters processStreamRecord(IonValue value,
                                               DefaultRecordTransformer transformer,
                                               IonWriter writer,
                                               OutputStream outputStream,
                                               java.util.Map<String, String> fieldErrors,
                                               int index,
                                               OutputFormat outputFormat) throws TransformException, IOException {
        IonStruct record = asStruct(value);
        boolean profile = TransformProfiler.isEnabled();
        long transformStart = profile ? System.nanoTime() : 0L;
        DefaultRecordTransformer.TransformOutcome outcome = transformer.transformWithErrors(record);
        if (profile) {
            TransformProfiler.addTransformNs(System.nanoTime() - transformStart);
        }
        if (outcome.failed) {
            for (Entry<String, String> entry : outcome.fieldErrors.entrySet()) {
                fieldErrors.put(index + "." + entry.getKey(), entry.getValue());
            }
        }
        if (!outcome.dropped) {
            IonStruct output = outcome.record;
            if (output != null) {
                long writeStart = profile ? System.nanoTime() : 0L;
                output.writeTo(writer);
                TransformTaskSupport.writeDelimiter(outputStream, outputFormat);
                if (profile) {
                    TransformProfiler.addWriteNs(System.nanoTime() - writeStart);
                }
            }
        }
        return new StreamCounters(index + 1, outcome.failed, outcome.dropped);
    }

    private record StreamCounters(int nextIndex, boolean failed, boolean dropped) {
    }

    private record StreamResult(TransformStats stats, URI uri) {
    }

    private URI storeRecords(RunContext runContext, List<IonStruct> records, OutputFormat outputFormat) throws TransformException {
        String name = "transform-" + UUID.randomUUID() + ".ion";
        try {
            java.nio.file.Path outputPath = runContext.workingDir().createTempFile(".ion");
            try (OutputStream outputStream = TransformTaskSupport.wrapCompression(
                TransformTaskSupport.bufferedOutput(outputPath));
                 IonWriter writer = TransformTaskSupport.createWriter(outputStream, outputFormat)) {
                boolean profile = TransformProfiler.isEnabled();
                for (IonStruct record : records) {
                    long writeStart = profile ? System.nanoTime() : 0L;
                    if (record == null) {
                        writer.writeNull();
                    } else {
                        record.writeTo(writer);
                    }
                    TransformTaskSupport.writeDelimiter(outputStream, outputFormat);
                    if (profile) {
                        TransformProfiler.addWriteNs(System.nanoTime() - writeStart);
                    }
                }
                writer.finish();
            }
            return runContext.storage().putFile(outputPath.toFile(), name);
        } catch (IOException e) {
            throw new TransformException("Unable to store transformed records", e);
        }
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
    public static class FieldDefinition {
        @Schema(title = "Expression")
        private String expr;

        @Schema(title = "Ion type")
        private IonTypeName type;

        @Builder.Default
        @Schema(title = "Optional")
        private boolean optional = false;

        @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
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
                    type = IonTypeName.valueOf(typeString.toUpperCase(Locale.ROOT));
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
            title = "Transformed records",
            description = """
        JSON-safe records when output mode is RECORDS or AUTO resolves to RECORDS.
        """
        )
        private final List<Object> records;
    }
}
