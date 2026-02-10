package io.kestra.plugin.transform;

import com.amazon.ion.IonValue;
import com.amazon.ion.IonList;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonBinaryWriterBuilder;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.transform.ion.IonTypeName;
import io.kestra.plugin.transform.ion.IonValueUtils;
import io.kestra.plugin.transform.util.TransformProfiler;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

@KestraTest
@EnabledIfSystemProperty(named = "bench", matches = "true")
class BenchTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void runBenchmarks() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        List<Integer> recordCounts = parseRecordCounts();
        OutputFormat format = parseFormat();
        Compression compression = parseCompression();
        io.kestra.plugin.transform.util.OutputFormat outputFormat = parseOutputFormat();
        Path benchDir = Path.of("build", "bench");
        Files.createDirectories(benchDir);
        Path reportPath = benchDir.resolve("report.md");

        try (BufferedWriter writer = Files.newBufferedWriter(reportPath, StandardCharsets.UTF_8)) {
            java.util.Map<String, java.util.Map<Integer, BenchResult>> resultsByScenario = new java.util.LinkedHashMap<>();
            java.util.Map<String, java.util.Map<Integer, ProfileResult>> profilesByScenario = new java.util.LinkedHashMap<>();
            boolean profileEnabled = isProfileEnabled();

            List<Scenario> scenarios = List.of(
                new Scenario("read", "Read", (context, uri) -> runReadOnly(context, uri), true),
                new Scenario("copy", "Copy", (context, uri) -> runCopy(context, uri, compression, outputFormat), true),
                new Scenario("map", "Map", (context, uri) -> runMap(context, uri, outputFormat), true),
                new Scenario("unnest", "Unnest", (context, uri) -> runUnnest(context, uri, outputFormat), true),
                new Scenario("filter", "Filter", (context, uri) -> runFilter(context, uri, outputFormat), true),
                new Scenario("aggregate", "Aggregate", (context, uri) -> runAggregate(context, uri, outputFormat), true),
                new Scenario("zip", "Zip", (context, uri) -> runZip(context, uri, outputFormat), true),
                new Scenario("select_zip_only", "Select (zip-only)", (context, uri) -> runSelectZipOnly(context, uri, outputFormat), true),
                new Scenario("select_filter_only", "Select (filter-only)", (context, uri) -> runSelectFilterOnly(context, uri, outputFormat), true),
                new Scenario("select_map_only", "Select (map-only)", (context, uri) -> runSelectMapOnly(context, uri, outputFormat), true),
                new Scenario("select_combined", "Select (combined)", (context, uri) -> runSelectCombined(context, uri, outputFormat), true),
                new Scenario("pipeline_zip_filter_map", "Pipeline (zip→filter→map)", (context, uri) -> runZipFilterMap(context, uri, outputFormat), true)
            );

            writer.write("# Kestra transform benchmark\n\n");
            writer.write("- Records: " + formatCountList(recordCounts) + "\n");
            writer.write("- Format: " + format.name().toLowerCase(java.util.Locale.ROOT) + "\n");
            writer.write("- Compression: " + compression.label + "\n");
            writer.write("- Profile: " + (profileEnabled ? "tasks" : "off") + "\n");
            writer.write("- Output format: " + outputFormat.name().toLowerCase(java.util.Locale.ROOT) + "\n");
            writer.write("- Output: build/bench\n\n");

            java.util.Map<Integer, Long> inputBytesByRecordCount = new java.util.LinkedHashMap<>();
            java.util.Map<Integer, String> inputSizeByRecordCount = new java.util.LinkedHashMap<>();
            java.util.Map<Integer, GenerationResult> generationByRecordCount = new java.util.LinkedHashMap<>();

            for (int recordCount : recordCounts) {
                Path inputPath = benchDir.resolve("input-" + recordCount + "." + format.fileExtension);
                GenerationResult generation = ensureIonFile(inputPath, recordCount, format);
                long inputBytes = generation.bytes();
                inputBytesByRecordCount.put(recordCount, inputBytes);
                inputSizeByRecordCount.put(recordCount, formatSize(inputBytes));
                generationByRecordCount.put(recordCount, generation);

                String inputUri = storeInput(runContext, inputPath);

                for (Scenario scenario : scenarios) {
                    BenchResult result = timeTaskWithMemory(() -> scenario.runner.run(runContext, inputUri));
                    resultsByScenario.computeIfAbsent(scenario.id, ignored -> new java.util.LinkedHashMap<>())
                        .put(recordCount, result);
                    if (profileEnabled && scenario.profileable) {
                        ProfileResult profile = profileTask(() -> scenario.runner.run(runContext, inputUri));
                        profilesByScenario.computeIfAbsent(scenario.id, ignored -> new java.util.LinkedHashMap<>())
                            .put(recordCount, profile);
                    }
                }
            }

            writer.write("## Input\n\n");
            writeTransposedHeader(writer, "Metric", recordCounts);
            writeTransposedSeparator(writer, recordCounts.size());
            writeTransposedRow(writer, "Input size", recordCounts, count -> inputSizeByRecordCount.get(count));
            writeTransposedRow(writer, "Generate (ms)", recordCounts, count -> {
                GenerationResult generation = generationByRecordCount.get(count);
                if (generation == null) {
                    return "-";
                }
                return formatDuration(generation.durationMs()) + (generation.generated() ? "" : " (cached)");
            });

            writer.write("\n## Durations (ms)\n\n");
            writeTransposedHeader(writer, "Task", recordCounts);
            writeTransposedSeparator(writer, recordCounts.size());
            for (Scenario scenario : scenarios) {
                writeTransposedRow(writer, scenario.label, recordCounts, count -> {
                    BenchResult result = getResult(resultsByScenario, scenario.id, count);
                    return result == null ? "-" : formatDuration(result.durationMs);
                });
            }

            writer.write("\n## Throughput (MiB/s)\n\n");
            writeTransposedHeader(writer, "Task", recordCounts);
            writeTransposedSeparator(writer, recordCounts.size());
            for (Scenario scenario : scenarios) {
                writeTransposedRow(writer, scenario.label, recordCounts, count -> {
                    BenchResult result = getResult(resultsByScenario, scenario.id, count);
                    Long bytes = inputBytesByRecordCount.get(count);
                    if (result == null || bytes == null) {
                        return "-";
                    }
                    return formatThroughput(bytes, result.durationMs);
                });
            }

            writer.write("\n## Memory Delta (MiB)\n\n");
            writeTransposedHeader(writer, "Task", recordCounts);
            writeTransposedSeparator(writer, recordCounts.size());
            for (Scenario scenario : scenarios) {
                writeTransposedRow(writer, scenario.label, recordCounts, count -> {
                    BenchResult result = getResult(resultsByScenario, scenario.id, count);
                    return result == null ? "-" : formatDeltaMiB(result);
                });
            }

            if (profileEnabled) {
                writer.write("\n## Profile (ms)\n\n");
                writeTransposedHeader(writer, "Task · phase", recordCounts);
                writeTransposedSeparator(writer, recordCounts.size());
                for (Scenario scenario : scenarios) {
                    if (!scenario.profileable) {
                        continue;
                    }
                    writeTransposedRow(writer, scenario.label + " · total", recordCounts, count -> formatProfile(profilesByScenario, scenario.id, count, Phase.TOTAL));
                    writeTransposedRow(writer, scenario.label + " · read/parse", recordCounts, count -> formatProfile(profilesByScenario, scenario.id, count, Phase.READ));
                    writeTransposedRow(writer, scenario.label + " · transform", recordCounts, count -> formatProfile(profilesByScenario, scenario.id, count, Phase.TRANSFORM));
                    writeTransposedRow(writer, scenario.label + " · write", recordCounts, count -> formatProfile(profilesByScenario, scenario.id, count, Phase.WRITE));
                }
            }
        }
    }

    private BenchResult getResult(java.util.Map<String, java.util.Map<Integer, BenchResult>> resultsByScenario,
                                  String scenarioId,
                                  int recordCount) {
        java.util.Map<Integer, BenchResult> perCount = resultsByScenario.get(scenarioId);
        if (perCount == null) {
            return null;
        }
        return perCount.get(recordCount);
    }

    private enum Phase {
        TOTAL,
        READ,
        TRANSFORM,
        WRITE
    }

    private String formatProfile(java.util.Map<String, java.util.Map<Integer, ProfileResult>> profilesByScenario,
                                 String scenarioId,
                                 int recordCount,
                                 Phase phase) {
        java.util.Map<Integer, ProfileResult> perCount = profilesByScenario.get(scenarioId);
        if (perCount == null) {
            return "-";
        }
        ProfileResult profile = perCount.get(recordCount);
        if (profile == null) {
            return "-";
        }
        return switch (phase) {
            case TOTAL -> formatDuration(profile.totalMs);
            case READ -> formatDuration(profile.readMs);
            case TRANSFORM -> formatDuration(profile.transformMs);
            case WRITE -> formatDuration(profile.writeMs);
        };
    }

    private void writeTransposedHeader(BufferedWriter writer, String firstColumnLabel, List<Integer> recordCounts) throws IOException {
        writer.write("| " + firstColumnLabel);
        for (Integer count : recordCounts) {
            writer.write(" | " + formatCount(count));
        }
        writer.write(" |\n");
    }

    private void writeTransposedSeparator(BufferedWriter writer, int recordCountColumns) throws IOException {
        writer.write("| ---");
        for (int i = 0; i < recordCountColumns; i++) {
            writer.write(" | ---");
        }
        writer.write(" |\n");
    }

    private void writeTransposedRow(BufferedWriter writer,
                                    String rowLabel,
                                    List<Integer> recordCounts,
                                    java.util.function.Function<Integer, String> valueForRecordCount) throws IOException {
        writer.write("| " + rowLabel);
        for (Integer count : recordCounts) {
            String value = valueForRecordCount.apply(count);
            writer.write(" | " + (value == null ? "-" : value));
        }
        writer.write(" |\n");
    }

    private record Scenario(String id, String label, ScenarioRunner runner, boolean profileable) {
    }

    private interface ScenarioRunner {
        void run(RunContext runContext, String uri) throws Exception;
    }

    private List<Integer> parseRecordCounts() {
        String raw = System.getProperty("bench.records");
        if (raw == null || raw.isBlank()) {
            return List.of(10_000, 100_000, 1_000_000);
        }
        String[] parts = raw.split(",");
        List<Integer> sizes = new ArrayList<>();
        for (String part : parts) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                sizes.add(Integer.parseInt(trimmed));
            }
        }
        return sizes;
    }

    private GenerationResult ensureIonFile(Path path, int recordCount, OutputFormat format) throws IOException {
        if (Files.exists(path)) {
            return new GenerationResult(false, 0L, Files.size(path), recordCount);
        }
        long start = System.nanoTime();
        int index = 0;
        int writtenRecords = 0;

        try (OutputStream fileStream = Files.newOutputStream(path);
             OutputStream outputStream = new java.io.BufferedOutputStream(fileStream);
             CountingOutputStream countingStream = new CountingOutputStream(outputStream)) {
            if (format == OutputFormat.BINARY) {
                java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream(64 * 1024);
                while (writtenRecords < recordCount) {
                    buffer.reset();
                    try (IonWriter writer = IonBinaryWriterBuilder.standard().build(buffer)) {
                        int batch = 0;
                        while (batch < 1000 && writtenRecords < recordCount) {
                            writeRecord(writer, index++, true);
                            batch++;
                            writtenRecords++;
                        }
                        writer.finish();
                    }
                    buffer.writeTo(countingStream);
                }
            } else {
                try (IonWriter writer = IonValueUtils.system().newTextWriter(countingStream)) {
                    while (writtenRecords < recordCount) {
                        writeRecord(writer, index++, false);
                        writtenRecords++;
                        countingStream.write('\n');
                        if (writtenRecords % 1000 == 0) {
                            writer.flush();
                        }
                    }
                    writer.finish();
                }
            }
        }

        long durationMs = Duration.ofNanos(System.nanoTime() - start).toMillis();
        long bytes = Files.size(path);
        return new GenerationResult(true, durationMs, bytes, writtenRecords);
    }

    private void writeRecord(IonWriter writer, int index, boolean binary) throws IOException {
        writer.stepIn(IonType.STRUCT);
        writer.setFieldName("customer_id");
        writer.writeString("c" + (index % 100));
        writer.setFieldName("country");
        writer.writeString("US");
        writer.setFieldName("total_spent");
        if (binary) {
            writer.writeInt(index % 100);
        } else {
            writer.writeDecimal(BigDecimal.valueOf(index % 100));
        }
        writer.setFieldName("active");
        writer.writeBool(index % 2 == 0);
        writer.setFieldName("items");
        writer.stepIn(IonType.LIST);
        writeItem(writer, "sku-" + index + "-a", 10, binary);
        writeItem(writer, "sku-" + index + "-b", 20, binary);
        writer.stepOut();
        writer.stepOut();
    }

    private void writeItem(IonWriter writer, String sku, int price, boolean binary) throws IOException {
        writer.stepIn(IonType.STRUCT);
        writer.setFieldName("sku");
        writer.writeString(sku);
        writer.setFieldName("price");
        if (binary) {
            writer.writeInt(price);
        } else {
            writer.writeDecimal(BigDecimal.valueOf(price));
        }
        writer.stepOut();
    }

    private String storeInput(RunContext runContext, Path path) throws IOException {
        String name = "bench-" + UUID.randomUUID() + ".ion";
        try (FileInputStream inputStream = new FileInputStream(path.toFile())) {
            return runContext.storage().putFile(inputStream, name).toString();
        }
    }

    private void runMap(RunContext runContext, String uri, io.kestra.plugin.transform.util.OutputFormat outputFormat) throws Exception {
        io.kestra.plugin.transform.Map task = io.kestra.plugin.transform.Map.builder()
            .from(Property.ofValue(uri))
            .outputType(Property.ofValue(io.kestra.plugin.transform.Map.OutputMode.STORE))
            .outputFormat(Property.ofValue(outputFormat))
            .fields(Property.ofValue(Map.of(
                "customer_id", io.kestra.plugin.transform.Map.FieldDefinition.builder().expr("customer_id").type(IonTypeName.STRING).build(),
                "total_spent", io.kestra.plugin.transform.Map.FieldDefinition.builder().expr("total_spent").type(IonTypeName.DECIMAL).build()
            )))
            .build();
        task.run(runContext);
    }

    private ProfileResult profileTask(ThrowingRunnable runnable) throws Exception {
        TransformProfiler.reset();
        long totalMs = timeTask(runnable);
        TransformProfiler.Snapshot snapshot = TransformProfiler.snapshot();
        long transformMs = Duration.ofNanos(snapshot.transformNs()).toMillis();
        long writeMs = Duration.ofNanos(snapshot.writeNs()).toMillis();
        long readMs = Math.max(0L, totalMs - transformMs - writeMs);
        return new ProfileResult(totalMs, readMs, transformMs, writeMs);
    }

    private void runReadOnly(RunContext runContext, String uri) throws Exception {
        try (InputStream inputStream = runContext.storage().getFile(java.net.URI.create(uri))) {
            Iterator<IonValue> iterator = IonValueUtils.system().iterate(inputStream);
            while (iterator.hasNext()) {
                IonValue value = iterator.next();
                if (value instanceof IonList list) {
                    for (IonValue element : list) {
                        element.getType();
                    }
                } else {
                    value.getType();
                }
            }
        }
    }

    private void runCopy(RunContext runContext,
                         String uri,
                         Compression compression,
                         io.kestra.plugin.transform.util.OutputFormat outputFormat) throws Exception {
        try (InputStream inputStream = runContext.storage().getFile(java.net.URI.create(uri))) {
            Path outputPath = runContext.workingDir().createTempFile(".ion");
            try (OutputStream fileStream = java.nio.file.Files.newOutputStream(outputPath);
                 OutputStream baseStream = new java.io.BufferedOutputStream(fileStream);
                 OutputStream outputStream = wrapCompression(baseStream, compression);
                 IonWriter writer = io.kestra.plugin.transform.util.TransformTaskSupport.createWriter(outputStream, outputFormat)) {
                Iterator<IonValue> iterator = IonValueUtils.system().iterate(inputStream);
                while (iterator.hasNext()) {
                    IonValue value = iterator.next();
                    if (value instanceof IonList list) {
                        for (IonValue element : list) {
                            element.writeTo(writer);
                            io.kestra.plugin.transform.util.TransformTaskSupport.writeDelimiter(outputStream, outputFormat);
                        }
                    } else {
                        value.writeTo(writer);
                        io.kestra.plugin.transform.util.TransformTaskSupport.writeDelimiter(outputStream, outputFormat);
                    }
                }
                writer.finish();
            }
        }
    }

    private void runUnnest(RunContext runContext, String uri, io.kestra.plugin.transform.util.OutputFormat outputFormat) throws Exception {
        Unnest task = Unnest.builder()
            .from(Property.ofValue(uri))
            .outputType(Property.ofValue(Unnest.OutputMode.STORE))
            .outputFormat(Property.ofValue(outputFormat))
            .path(Property.ofValue("items[]"))
            .as(Property.ofValue("item"))
            .build();
        task.run(runContext);
    }

    private void runFilter(RunContext runContext, String uri, io.kestra.plugin.transform.util.OutputFormat outputFormat) throws Exception {
        Filter task = Filter.builder()
            .from(Property.ofValue(uri))
            .outputType(Property.ofValue(Filter.OutputMode.STORE))
            .outputFormat(Property.ofValue(outputFormat))
            .where(Property.ofValue("active"))
            .build();
        task.run(runContext);
    }

    private void runAggregate(RunContext runContext, String uri, io.kestra.plugin.transform.util.OutputFormat outputFormat) throws Exception {
        Aggregate task = Aggregate.builder()
            .from(Property.ofValue(uri))
            .outputType(Property.ofValue(Aggregate.OutputMode.STORE))
            .outputFormat(Property.ofValue(outputFormat))
            .groupBy(Property.ofValue(List.of("customer_id")))
            .aggregates(Property.ofValue(Map.of(
                "order_count", Aggregate.AggregateDefinition.builder().expr("count()").type(IonTypeName.INT).build(),
                "total_spent", Aggregate.AggregateDefinition.builder().expr("sum(total_spent)").type(IonTypeName.DECIMAL).build()
            )))
            .build();
        task.run(runContext);
    }

    private void runZip(RunContext runContext, String uri, io.kestra.plugin.transform.util.OutputFormat outputFormat) throws Exception {
        Zip task = Zip.builder()
            .inputs(List.of(
                Property.ofValue(uri),
                Property.ofValue(uri)
            ))
            .onConflict(Property.ofValue(Zip.ConflictMode.RIGHT))
            .outputType(Property.ofValue(Zip.OutputMode.STORE))
            .outputFormat(Property.ofValue(outputFormat))
            .build();
        task.run(runContext);
    }

    private void runSelectZipOnly(RunContext runContext, String uri, io.kestra.plugin.transform.util.OutputFormat outputFormat) throws Exception {
        Select task = Select.builder()
            .inputs(List.of(
                Property.ofValue(uri),
                Property.ofValue(uri)
            ))
            .outputType(Property.ofValue(Select.OutputMode.STORE))
            .outputFormat(Property.ofValue(outputFormat))
            .build();
        task.run(runContext);
    }

    private void runSelectFilterOnly(RunContext runContext, String uri, io.kestra.plugin.transform.util.OutputFormat outputFormat) throws Exception {
        Select task = Select.builder()
            .inputs(List.of(Property.ofValue(uri)))
            .where(Property.ofValue("active"))
            .outputType(Property.ofValue(Select.OutputMode.STORE))
            .outputFormat(Property.ofValue(outputFormat))
            .build();
        task.run(runContext);
    }

    private void runSelectMapOnly(RunContext runContext, String uri, io.kestra.plugin.transform.util.OutputFormat outputFormat) throws Exception {
        Select task = Select.builder()
            .inputs(List.of(Property.ofValue(uri)))
            .fields(Property.ofValue(Map.of(
                "customer_id", Select.FieldDefinition.builder().expr("customer_id").type(IonTypeName.STRING).build(),
                "total_spent", Select.FieldDefinition.builder().expr("total_spent").type(IonTypeName.DECIMAL).build()
            )))
            .outputType(Property.ofValue(Select.OutputMode.STORE))
            .outputFormat(Property.ofValue(outputFormat))
            .build();
        task.run(runContext);
    }

    private void runSelectCombined(RunContext runContext, String uri, io.kestra.plugin.transform.util.OutputFormat outputFormat) throws Exception {
        Select task = Select.builder()
            .inputs(List.of(
                Property.ofValue(uri),
                Property.ofValue(uri)
            ))
            .where(Property.ofValue("$1.active && $2.active"))
            .fields(Property.ofValue(Map.of(
                "customer_id", Select.FieldDefinition.builder().expr("customer_id").type(IonTypeName.STRING).build(),
                "total_spent", Select.FieldDefinition.builder().expr("total_spent").type(IonTypeName.DECIMAL).build()
            )))
            .outputType(Property.ofValue(Select.OutputMode.STORE))
            .outputFormat(Property.ofValue(outputFormat))
            .build();
        task.run(runContext);
    }

    private void runZipFilterMap(RunContext runContext, String uri, io.kestra.plugin.transform.util.OutputFormat outputFormat) throws Exception {
        Zip zip = Zip.builder()
            .inputs(List.of(
                Property.ofValue(uri),
                Property.ofValue(uri)
            ))
            .onConflict(Property.ofValue(Zip.ConflictMode.RIGHT))
            .outputType(Property.ofValue(Zip.OutputMode.STORE))
            .outputFormat(Property.ofValue(outputFormat))
            .build();
        Zip.Output zipped = zip.run(runContext);

        Filter filter = Filter.builder()
            .from(Property.ofValue(zipped.getUri()))
            .where(Property.ofValue("active"))
            .outputType(Property.ofValue(Filter.OutputMode.STORE))
            .outputFormat(Property.ofValue(outputFormat))
            .build();
        Filter.Output filtered = filter.run(runContext);

        io.kestra.plugin.transform.Map map = io.kestra.plugin.transform.Map.builder()
            .from(Property.ofValue(filtered.getUri()))
            .outputType(Property.ofValue(io.kestra.plugin.transform.Map.OutputMode.STORE))
            .outputFormat(Property.ofValue(outputFormat))
            .fields(Property.ofValue(Map.of(
                "customer_id", io.kestra.plugin.transform.Map.FieldDefinition.builder().expr("customer_id").type(IonTypeName.STRING).build(),
                "total_spent", io.kestra.plugin.transform.Map.FieldDefinition.builder().expr("total_spent").type(IonTypeName.DECIMAL).build()
            )))
            .build();
        map.run(runContext);
    }

    private long timeTask(ThrowingRunnable runnable) throws Exception {
        long start = System.nanoTime();
        runnable.run();
        return Duration.ofNanos(System.nanoTime() - start).toMillis();
    }

    private BenchResult timeTaskWithMemory(ThrowingRunnable runnable) throws Exception {
        long before = usedMemoryBytes();
        long start = System.nanoTime();
        runnable.run();
        long durationMs = Duration.ofNanos(System.nanoTime() - start).toMillis();
        long after = usedMemoryBytes();
        return new BenchResult(durationMs, before, after);
    }

    private long usedMemoryBytes() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    private record BenchResult(long durationMs, long beforeBytes, long afterBytes) {
    }

    private record ProfileResult(long totalMs, long readMs, long transformMs, long writeMs) {
    }

    private record GenerationResult(boolean generated, long durationMs, long bytes, long records) {
    }

    private String formatCountList(List<Integer> counts) {
        List<String> formatted = new ArrayList<>();
        for (Integer count : counts) {
            formatted.add(formatCount(count));
        }
        return formatted.toString();
    }

    private String formatCount(long count) {
        return String.format(java.util.Locale.ROOT, "%,d", count);
    }

    private String formatSize(long bytes) {
        double mib = bytes / (1024.0 * 1024.0);
        return String.format(java.util.Locale.ROOT, "%.2f MiB (%d bytes)", mib, bytes);
    }

    private String formatDuration(long durationMs) {
        return Long.toString(durationMs);
    }

    private String formatThroughput(long bytes, long durationMs) {
        if (durationMs <= 0) {
            return "-";
        }
        double seconds = durationMs / 1000.0;
        double mib = bytes / (1024.0 * 1024.0);
        return String.format(java.util.Locale.ROOT, "%.2f MiB/s", mib / seconds);
    }

    private String formatDeltaMiB(BenchResult result) {
        double delta = (result.afterBytes - result.beforeBytes) / (1024.0 * 1024.0);
        return String.format(java.util.Locale.ROOT, "%.2f", delta);
    }

    private OutputFormat parseFormat() {
        String raw = System.getProperty("bench.format");
        if (raw == null || raw.isBlank()) {
            return OutputFormat.TEXT;
        }
        if ("binary".equalsIgnoreCase(raw.trim())) {
            return OutputFormat.BINARY;
        }
        return OutputFormat.TEXT;
    }

    private io.kestra.plugin.transform.util.OutputFormat parseOutputFormat() {
        String raw = System.getProperty("bench.outputFormat");
        if (raw == null || raw.isBlank()) {
            return io.kestra.plugin.transform.util.OutputFormat.TEXT;
        }
        if ("binary".equalsIgnoreCase(raw.trim())) {
            return io.kestra.plugin.transform.util.OutputFormat.BINARY;
        }
        return io.kestra.plugin.transform.util.OutputFormat.TEXT;
    }

    private Compression parseCompression() {
        String raw = System.getProperty("bench.compression");
        if (raw == null || raw.isBlank()) {
            return Compression.NONE;
        }
        if ("gzip".equalsIgnoreCase(raw.trim())) {
            return Compression.GZIP;
        }
        return Compression.NONE;
    }

    private boolean isProfileEnabled() {
        String raw = System.getProperty("bench.profile");
        return raw != null && "true".equalsIgnoreCase(raw.trim());
    }

    private enum OutputFormat {
        TEXT("ion"),
        BINARY("ionb");

        private final String fileExtension;

        OutputFormat(String fileExtension) {
            this.fileExtension = fileExtension;
        }
    }

    private enum Compression {
        NONE("none"),
        GZIP("gzip");

        private final String label;

        Compression(String label) {
            this.label = label;
        }
    }

    private OutputStream wrapCompression(OutputStream outputStream, Compression compression) throws IOException {
        if (compression == Compression.GZIP) {
            return new GZIPOutputStream(outputStream, true);
        }
        return outputStream;
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private static final class CountingOutputStream extends OutputStream implements AutoCloseable {
        private final OutputStream delegate;
        private long count;

        private CountingOutputStream(OutputStream delegate) {
            this.delegate = delegate;
        }

        @Override
        public void write(int b) throws IOException {
            delegate.write(b);
            count++;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            delegate.write(b, off, len);
            count += len;
        }

        @Override
        public void flush() throws IOException {
            delegate.flush();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        private long getCount() {
            return count;
        }
    }
}
