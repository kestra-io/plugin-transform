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
import io.kestra.plugin.transform.ion.IonValueUtils;
import io.kestra.plugin.transform.util.OutputFormat;
import io.kestra.plugin.transform.util.TransformException;
import io.kestra.plugin.transform.util.TransformProfiler;
import io.kestra.plugin.transform.util.TransformTaskSupport;
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
import java.util.List;
import java.util.Locale;
import java.util.UUID;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Zip records",
    description = """
        Merge multiple record streams by position (record i with record i).
        """
)
@Plugin(
    examples = {
        @io.kestra.core.models.annotations.Example(
            title = "Zip records from multiple sources",
            full = true,
            code = """
                id: zip_two_sources
                namespace: company.team

                tasks:
                  - id: left
                    type: io.kestra.plugin.core.output.OutputValues
                    values:
                      records:
                        - id: 1
                          left_value: L1
                        - id: 2
                          left_value: L2

                  - id: right
                    type: io.kestra.plugin.core.output.OutputValues
                    values:
                      records:
                        - id: 1
                          right_value: R1
                        - id: 2
                          right_value: R2

                  - id: zip
                    type: io.kestra.plugin.transform.Zip
                    inputs:
                      - "{{ outputs.left.values.records }}"
                      - "{{ outputs.right.values.records }}"
                    onConflict: RIGHT
                """
        )
    },
    metrics = {
        @Metric(name = "processed", type = Counter.TYPE),
        @Metric(name = "failed", type = Counter.TYPE),
        @Metric(name = "dropped", type = Counter.TYPE)
    }
)
public class Zip extends Task implements RunnableTask<Zip.Output> {
    @NotNull
    @Schema(
        title = "Input records",
        description = """
        List of two or more inputs (Ion list/struct or storage URIs) to align by row position.
        """
    )
    private List<Property<Object>> inputs;

    @Builder.Default
    @Schema(
        title = "On error behavior",
        description = """
        FAIL stops the task on merge errors (such as field conflicts), SKIP drops the current row.
        """
    )
    private Property<OnErrorMode> onError = Property.ofValue(OnErrorMode.FAIL);

    @Builder.Default
    @Schema(
        title = "On conflict behavior",
        description = """
        FAIL errors on duplicate field names, LEFT keeps the first value, RIGHT overwrites with the later value.
        """
    )
    private Property<ConflictMode> onConflict = Property.ofValue(ConflictMode.FAIL);

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
        AUTO stores to internal storage when any input is a storage URI; otherwise it returns records.
        """
    )
    @Builder.Default
    private Property<OutputMode> outputType = Property.ofValue(OutputMode.AUTO);

    @Override
    public Output run(RunContext runContext) throws Exception {
        if (inputs == null || inputs.size() < 2) {
            throw new TransformException("inputs must contain at least two elements");
        }
        var rOnError = runContext.render(this.onError).as(OnErrorMode.class).orElseThrow();
        var rOnConflict = runContext.render(this.onConflict).as(ConflictMode.class).orElseThrow();
        var rOutputFormat = runContext.render(this.outputFormat).as(OutputFormat.class).orElseThrow();
        var rOutputType = runContext.render(this.outputType).as(OutputMode.class).orElseThrow();

        var resolvedInputs = new ArrayList<TransformTaskSupport.ResolvedInput>(inputs.size());
        boolean anyFromStorage = false;
        for (Property<Object> input : inputs) {
            var resolved = TransformTaskSupport.resolveInput(runContext, input);
            resolvedInputs.add(resolved);
            anyFromStorage = anyFromStorage || resolved.fromStorage();
        }

        var effectiveOutput = rOutputType == OutputMode.AUTO
            ? (anyFromStorage ? OutputMode.STORE : OutputMode.RECORDS)
            : rOutputType;

        var stats = new StatsAccumulator();

        if (effectiveOutput == OutputMode.STORE && anyFromStorage) {
            var storedUri = zipStreamToStorage(runContext, resolvedInputs, stats, rOnError, rOnConflict, rOutputFormat);
            runContext.metric(Counter.of("processed", stats.processed))
                .metric(Counter.of("failed", stats.failed))
                .metric(Counter.of("dropped", stats.dropped));
            return Output.builder()
                .uri(storedUri.toString())
                .build();
        }

        var inputRecords = new ArrayList<List<IonStruct>>(resolvedInputs.size());
        for (TransformTaskSupport.ResolvedInput resolved : resolvedInputs) {
            inputRecords.add(TransformTaskSupport.normalizeRecords(resolveInMemory(runContext, resolved)));
        }
        validateSameLength(inputRecords);

        if (effectiveOutput == OutputMode.STORE) {
            var storedUri = storeRecords(runContext, inputRecords, stats, rOnError, rOnConflict, rOutputFormat);
            runContext.metric(Counter.of("processed", stats.processed))
                .metric(Counter.of("failed", stats.failed))
                .metric(Counter.of("dropped", stats.dropped));
            return Output.builder()
                .uri(storedUri.toString())
                .build();
        }

        var rendered = zipToRecords(inputRecords, stats, rOnError, rOnConflict);
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

    private List<Object> zipToRecords(List<List<IonStruct>> inputRecords,
                                      StatsAccumulator stats,
                                      OnErrorMode onError,
                                      ConflictMode onConflict) throws TransformException {
        List<Object> outputRecords = new ArrayList<>();
        int length = inputRecords.getFirst().size();
        for (int i = 0; i < length; i++) {
            stats.processed++;
            try {
                IonStruct merged = mergeRecords(rowAt(inputRecords, i), onConflict);
                outputRecords.add(IonValueUtils.toJavaValue(merged));
            } catch (TransformException e) {
                stats.failed++;
                if (onError == OnErrorMode.FAIL) {
                    throw e;
                }
                if (onError == OnErrorMode.SKIP) {
                    stats.dropped++;
                }
            }
        }
        return outputRecords;
    }

    private URI storeRecords(RunContext runContext,
                             List<List<IonStruct>> inputRecords,
                             StatsAccumulator stats,
                             OnErrorMode onError,
                             ConflictMode onConflict,
                             OutputFormat outputFormat) throws TransformException {
        String name = "zip-" + UUID.randomUUID() + ".ion";
        try {
            java.nio.file.Path outputPath = runContext.workingDir().createTempFile(".ion");
            try (OutputStream outputStream = TransformTaskSupport.wrapCompression(
                TransformTaskSupport.bufferedOutput(outputPath));
                 IonWriter writer = TransformTaskSupport.createWriter(outputStream, outputFormat)) {
                boolean profile = TransformProfiler.isEnabled();
                int length = inputRecords.getFirst().size();
                for (int i = 0; i < length; i++) {
                    stats.processed++;
                    try {
                        long transformStart = profile ? System.nanoTime() : 0L;
                        IonStruct merged = mergeRecords(rowAt(inputRecords, i), onConflict);
                        if (profile) {
                            TransformProfiler.addTransformNs(System.nanoTime() - transformStart);
                        }
                        long writeStart = profile ? System.nanoTime() : 0L;
                        merged.writeTo(writer);
                        TransformTaskSupport.writeDelimiter(outputStream, outputFormat);
                        if (profile) {
                            TransformProfiler.addWriteNs(System.nanoTime() - writeStart);
                        }
                    } catch (TransformException e) {
                        stats.failed++;
                        if (onError == OnErrorMode.FAIL) {
                            throw e;
                        }
                        if (onError == OnErrorMode.SKIP) {
                            stats.dropped++;
                        }
                    }
                }
            }
            return runContext.storage().putFile(outputPath.toFile(), name);
        } catch (IOException e) {
            throw new TransformException("Unable to store zipped records", e);
        }
    }

    private URI zipStreamToStorage(RunContext runContext,
                                   List<TransformTaskSupport.ResolvedInput> inputs,
                                   StatsAccumulator stats,
                                   OnErrorMode onError,
                                   ConflictMode onConflict,
                                   OutputFormat outputFormat) throws TransformException {
        String name = "zip-" + UUID.randomUUID() + ".ion";
        try (MultiCursor cursor = openCursors(runContext, inputs)) {
            java.nio.file.Path outputPath = runContext.workingDir().createTempFile(".ion");
            try (OutputStream outputStream = TransformTaskSupport.wrapCompression(
                TransformTaskSupport.bufferedOutput(outputPath));
                 IonWriter writer = TransformTaskSupport.createWriter(outputStream, outputFormat)) {
                boolean profile = TransformProfiler.isEnabled();
                while (true) {
                    if (!cursor.hasAlignedNext()) {
                        break;
                    }
                    stats.processed++;
                    try {
                        long transformStart = profile ? System.nanoTime() : 0L;
                        IonStruct merged = mergeRecords(cursor.nextRow(), onConflict);
                        if (profile) {
                            TransformProfiler.addTransformNs(System.nanoTime() - transformStart);
                        }
                        long writeStart = profile ? System.nanoTime() : 0L;
                        merged.writeTo(writer);
                        TransformTaskSupport.writeDelimiter(outputStream, outputFormat);
                        if (profile) {
                            TransformProfiler.addWriteNs(System.nanoTime() - writeStart);
                        }
                    } catch (TransformException e) {
                        stats.failed++;
                        if (onError == OnErrorMode.FAIL) {
                            throw e;
                        }
                        if (onError == OnErrorMode.SKIP) {
                            stats.dropped++;
                        }
                    }
                }
            }
            return runContext.storage().putFile(outputPath.toFile(), name);
        } catch (IOException e) {
            throw new TransformException("Unable to store zipped records", e);
        }
    }

    private IonStruct mergeRecords(List<IonStruct> records, ConflictMode onConflict) throws TransformException {
        IonStruct merged = IonValueUtils.system().newEmptyStruct();
        for (IonStruct record : records) {
            for (IonValue value : record) {
                String fieldName = value.getFieldName();
                if (merged.get(fieldName) != null) {
                    if (onConflict == ConflictMode.FAIL) {
                        throw new TransformException("Field conflict on '" + fieldName + "'");
                    }
                    if (onConflict == ConflictMode.LEFT) {
                        continue;
                    }
                }
                merged.put(fieldName, IonValueUtils.cloneValue(value));
            }
        }
        return merged;
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

    private List<IonStruct> rowAt(List<List<IonStruct>> inputRecords, int index) {
        List<IonStruct> row = new ArrayList<>(inputRecords.size());
        for (List<IonStruct> records : inputRecords) {
            row.add(records.get(index));
        }
        return row;
    }

    private void validateSameLength(List<List<IonStruct>> inputRecords) throws TransformException {
        int expected = inputRecords.getFirst().size();
        for (int i = 1; i < inputRecords.size(); i++) {
            if (inputRecords.get(i).size() != expected) {
                StringBuilder message = new StringBuilder("inputs must have same length: ");
                for (int j = 0; j < inputRecords.size(); j++) {
                    if (j > 0) {
                        message.append(", ");
                    }
                    message.append("input").append(j + 1).append("=").append(inputRecords.get(j).size());
                }
                throw new TransformException(message.toString());
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
        public void close() throws IOException {
            if (closeable != null) {
                try {
                    closeable.close();
                } catch (Exception e) {
                    if (e instanceof IOException ioException) {
                        throw ioException;
                    }
                }
            }
        }
    }

    private static final class MultiCursor implements AutoCloseable {
        private final List<RecordCursor> cursors;

        private MultiCursor(List<RecordCursor> cursors) {
            this.cursors = cursors;
        }

        boolean hasAlignedNext() throws TransformException {
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
                throw new TransformException("inputs must have same length");
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

    public enum ConflictMode {
        FAIL,
        LEFT,
        RIGHT
    }

    public enum OnErrorMode {
        FAIL,
        SKIP
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
            title = "Zipped records",
            description = """
        JSON-safe records when output mode is RECORDS or AUTO resolves to RECORDS.
        """
        )
        private final List<Object> records;
    }

    private static final class StatsAccumulator {
        private long processed;
        private long failed;
        private long dropped;
    }
}
