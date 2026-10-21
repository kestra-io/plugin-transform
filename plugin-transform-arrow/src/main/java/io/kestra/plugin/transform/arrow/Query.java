package io.kestra.plugin.transform.arrow;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.PebbleUtil;
import io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper;
import io.kestra.plugin.scripts.runner.docker.Docker;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query a Parquet, Arrow IPC, CSV or NDJSON file with a jq expression",
    description = """
        Runs [aq](https://github.com/Anaethelion/aq) in a container to apply a jq expression to each row of a columnar file. \
        Each row is converted to a JSON object before the expression runs, see the aq documentation for the type mapping and the \
        supported jq syntax."""
)
@Plugin(
    examples = {
        @Example(
            title = "Keep the returned orders of a Parquet file.",
            full = true,
            code = """
                id: arrow_query
                namespace: company.team

                tasks:
                  - id: download
                    type: io.kestra.plugin.core.http.Download
                    uri: https://huggingface.co/datasets/kestra/datasets/resolve/main/attribution/raw_orders.parquet

                  - id: returned_orders
                    type: io.kestra.plugin.transform.arrow.Query
                    from: "{{ outputs.download.uri }}"
                    expression: 'select(.status == "returned") | {id, user_id, order_date}'
                """
        ),
        @Example(
            title = "Compute the total revenue of a CSV file, using `slurp` to aggregate all the rows at once.",
            full = true,
            code = """
                id: arrow_query_slurp
                namespace: company.team

                tasks:
                  - id: download
                    type: io.kestra.plugin.core.http.Download
                    uri: https://huggingface.co/datasets/kestra/datasets/resolve/main/csv/orders.csv

                  - id: total_revenue
                    type: io.kestra.plugin.transform.arrow.Query
                    from: "{{ outputs.download.uri }}"
                    slurp: true
                    expression: '{orders: length, revenue: (map(.total) | add)}'
                """
        ),
        @Example(
            title = "Convert a Parquet file to CSV, keeping only some columns.",
            full = true,
            code = """
                id: arrow_query_csv
                namespace: company.team

                tasks:
                  - id: download
                    type: io.kestra.plugin.core.http.Download
                    uri: https://huggingface.co/datasets/kestra/datasets/resolve/main/attribution/raw_orders.parquet

                  - id: to_csv
                    type: io.kestra.plugin.transform.arrow.Query
                    from: "{{ outputs.download.uri }}"
                    expression: '{id, status}'
                    outputFormat: CSV
                """
        )
    }
)
public class Query extends Task implements RunnableTask<Query.Output> {
    // bumped together with AQ_REVISION in dockerfiles/aq.Dockerfile
    static final String DEFAULT_IMAGE = "ghcr.io/kestra-io/aq:38a8d1f";
    private static final ObjectMapper MAPPER = JacksonMapper.ofJson();

    @Schema(
        title = "The file to query",
        description = """
            Must be a `kestra://` internal storage URI."""
    )
    @NotNull
    @PluginProperty(internalStorageURI = true, group = "main")
    private Property<String> from;

    @Schema(
        title = "The jq expression to apply",
        description = """
            Applied to each row, or to the array of all rows when `slurp` is true. Defaults to passing each row through."""
    )
    @PluginProperty(group = "main")
    private Property<String> expression;

    @Schema(
        title = "The format of the output file",
        description = """
            `ION` writes each result as a record, like the other transform tasks. The other formats write the file produced by aq."""
    )
    @NotNull
    @Builder.Default
    @PluginProperty(group = "main")
    private Property<OutputFormat> outputFormat = Property.ofValue(OutputFormat.ION);

    @Schema(
        title = "The format of the input file",
        description = """
            `AUTO` detects it from the file extension: `.parquet`, `.arrow`, `.ipc`, `.feather`, `.csv`, `.json`, `.jsonl` or `.ndjson`."""
    )
    @NotNull
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<InputFormat> inputFormat = Property.ofValue(InputFormat.AUTO);

    @Schema(
        title = "Whether to collect all the rows into an array before applying the expression",
        description = """
            Enables aggregations such as `length` or `map(.amount) | add`. All the rows are loaded in memory."""
    )
    @NotNull
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> slurp = Property.ofValue(false);

    @Schema(
        title = "The field delimiter of CSV input and output",
        description = """
            A single character. Defaults to `,`."""
    )
    @PluginProperty(group = "advanced")
    private Property<String> delimiter;

    @Schema(
        title = "The task runner to use",
        description = """
            Task runners are provided by plugins, each have their own properties."""
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    @Valid
    private TaskRunner<?> taskRunner = Docker.instance();

    @Schema(
        title = "The task runner container image, only used if the task runner is container-based",
        description = """
            The image must provide the `aq` CLI."""
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<String> containerImage = Property.ofValue(DEFAULT_IMAGE);

    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private transient volatile Logger runLogger;

    @Override
    public Output run(RunContext runContext) throws Exception {
        this.runLogger = runContext.logger();

        var rFrom = runContext.render(this.from).as(String.class)
            .filter(value -> !value.isBlank())
            .orElseThrow(() -> new IllegalArgumentException("`from` must be a Kestra internal storage URI"));
        // the task runner renders the input files again, so only an internal storage URI is accepted
        if (!rFrom.startsWith("kestra://") || PebbleUtil.containsOpeningBlockDelimiter(rFrom)) {
            throw new IllegalArgumentException("`from` must be a Kestra internal storage URI, got '" + rFrom + "'");
        }
        var rInputFormat = inputFormat(rFrom, runContext.render(this.inputFormat).as(InputFormat.class).orElse(InputFormat.AUTO));
        var rOutputFormat = runContext.render(this.outputFormat).as(OutputFormat.class).orElse(OutputFormat.ION);
        var rContainerImage = runContext.render(this.containerImage).as(String.class).orElse(DEFAULT_IMAGE);

        var inputFile = "input." + rInputFormat.extension;
        var resultFile = "result." + rOutputFormat.extension;

        // user values are passed as environment variables so the shell never interprets them
        var env = new HashMap<String, String>();
        var command = new ArrayList<>(List.of("aq", "-c", "-o", rOutputFormat.aqFormat, "-f", rInputFormat.aqFormat));

        if (runContext.render(this.slurp).as(Boolean.class).orElse(false)) {
            command.add("-s");
        }

        var rDelimiter = runContext.render(this.delimiter).as(String.class);
        if (rDelimiter.isPresent()) {
            if (rDelimiter.get().length() != 1) {
                throw new IllegalArgumentException("`delimiter` must be a single character, got '" + rDelimiter.get() + "'");
            }
            env.put("AQ_DELIMITER", rDelimiter.get());
            command.add("-d \"$AQ_DELIMITER\"");
        }

        var rExpression = runContext.render(this.expression).as(String.class);
        if (rExpression.isPresent()) {
            // Base64 so the task runner does not render it again, parentheses so aq never reads it as a path
            var wrapped = "(\n" + rExpression.get() + "\n)";
            env.put("AQ_EXPRESSION_B64", Base64.getEncoder().encodeToString(wrapped.getBytes(StandardCharsets.UTF_8)));
            command.add("\"$(printf '%s' \"$AQ_EXPRESSION_B64\" | base64 -d)\"");
        }

        // the command runs in the task working directory, where the input file is created
        command.add(inputFile + " > " + resultFile);

        var scriptOutput = new CommandsWrapper(runContext)
            .withEnv(env)
            .withInputFiles(Map.of(inputFile, rFrom))
            .withOutputFiles(List.of(resultFile))
            .withTaskRunner(this.taskRunner)
            .withContainerImage(rContainerImage)
            .withInterpreter(Property.ofValue(List.of("/bin/sh", "-c")))
            .withCommands(Property.ofValue(List.of(String.join(" ", command))))
            .run();

        var resultUri = scriptOutput.getOutputFiles().get(resultFile);

        if (rOutputFormat != OutputFormat.ION) {
            return Output.builder()
                .uri(resultUri)
                .build();
        }

        var ionFile = runContext.workingDir().createTempFile(".ion");
        Long processedItemsTotal;
        try (
            var reader = new BufferedReader(new InputStreamReader(runContext.storage().getFile(resultUri), StandardCharsets.UTF_8), FileSerde.BUFFER_SIZE);
            var writer = new BufferedWriter(new OutputStreamWriter(Files.newOutputStream(ionFile)), FileSerde.BUFFER_SIZE)
        ) {
            var values = Flux.fromStream(reader.lines())
                .filter(line -> !line.isBlank())
                .map(throwFunction(line -> MAPPER.readValue(line, Object.class)));
            processedItemsTotal = FileSerde.writeAll(writer, values).block();
        }
        runContext.storage().deleteFile(resultUri);

        return Output.builder()
            .uri(runContext.storage().putFile(ionFile.toFile()))
            .processedItemsTotal(processedItemsTotal)
            .build();
    }

    @Override
    public void kill() {
        try {
            if (this.taskRunner != null) {
                this.taskRunner.kill();
            }
        } catch (Exception e) {
            if (this.runLogger != null) {
                this.runLogger.warn("Unable to kill the aq container: {}", e.getMessage());
            }
        }
    }

    private static InputFormat inputFormat(String from, InputFormat inputFormat) {
        if (inputFormat != InputFormat.AUTO) {
            return inputFormat;
        }

        var path = URI.create(from).getPath();
        var extension = path == null || path.lastIndexOf('.') < 0 ? "" : path.substring(path.lastIndexOf('.') + 1).toLowerCase(Locale.ROOT);

        return switch (extension) {
            case "parquet" -> InputFormat.PARQUET;
            case "arrow", "ipc", "feather" -> InputFormat.ARROW;
            case "csv" -> InputFormat.CSV;
            case "json", "jsonl", "ndjson" -> InputFormat.NDJSON;
            default -> throw new IllegalArgumentException(
                "Unable to detect the format of '" + from + "' from its extension, set `inputFormat` to PARQUET, ARROW, CSV or NDJSON"
            );
        };
    }

    public enum InputFormat {
        AUTO(null, null),
        PARQUET("parquet", "parquet"),
        ARROW("arrow", "arrow"),
        CSV("csv", "csv"),
        NDJSON("json", "ndjson");

        private final String aqFormat;
        private final String extension;

        InputFormat(String aqFormat, String extension) {
            this.aqFormat = aqFormat;
            this.extension = extension;
        }
    }

    public enum OutputFormat {
        ION("ndjson", "ndjson"),
        NDJSON("ndjson", "ndjson"),
        JSON("json", "json"),
        CSV("csv", "csv"),
        ARROW("arrow", "arrow");

        private final String aqFormat;
        private final String extension;

        OutputFormat(String aqFormat, String extension) {
            this.aqFormat = aqFormat;
            this.extension = extension;
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "File URI containing the result of the query"
        )
        private final URI uri;

        @Schema(
            title = "The total number of records written",
            description = """
                Only set when `outputFormat` is `ION`."""
        )
        private final Long processedItemsTotal;
    }
}
