package io.kestra.plugin.transform.arrow;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;
import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Transform Arrow, Parquet, CSV, or NDJSON data using jq-style queries via aq."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Execute a query on an Arrow or Parquet file",
            code = """
id: arrow_transform
namespace: company.team

tasks:
  - id: transform
    type: io.kestra.plugin.transform.arrow.ArrowTransform
    from: "{{ inputs.file }}"
    query: ".[] | select(.age > 30)"
            """
        )
    }
)
public class ArrowTransform extends Task implements RunnableTask<ArrowTransform.Output> {

    @Schema(
        title = "Internal storage URI of the file to transform"
    )
    @PluginProperty(group = "main", internalStorageURI = true)
    @NotNull
    private Property<String> from;

    @Schema(
        title = "The jq-style transformation query to execute using aq"
    )
    @PluginProperty(group = "main")
    @NotNull
    private Property<String> query;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String renderFrom = runContext.render(this.from).as(String.class).orElseThrow();
        String renderQuery = runContext.render(this.query).as(String.class).orElseThrow();

        URI uri = URI.create(renderFrom);
        File inputFile = runContext.workingDir().createTempFile(".data").toFile();
        try (FileOutputStream out = new FileOutputStream(inputFile);
             var in = runContext.storage().getFile(uri)) {
            in.transferTo(out);
        }

        File outputFile = runContext.workingDir().createTempFile(".arrow").toFile();

        ProcessBuilder processBuilder = new ProcessBuilder("aq", renderQuery, inputFile.getAbsolutePath());
        processBuilder.redirectOutput(outputFile);
        
        Process process = processBuilder.start();
        int exitCode = process.waitFor();

        if (exitCode != 0) {
            throw new RuntimeException("aq execution failed with exit code: " + exitCode + ". Please check if the query is valid.");
        }

        URI outputUri;
        try (FileInputStream in = new FileInputStream(outputFile)) {
            outputUri = runContext.storage().put(uri, in);
        }

        return Output.builder()
            .uri(outputUri)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "URI of the transformed Arrow output file"
        )
        private final URI uri;
    }
            }
            
