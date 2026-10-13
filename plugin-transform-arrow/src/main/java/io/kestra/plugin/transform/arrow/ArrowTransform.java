package io.kestra.plugin.transform.arrow;

import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {}
)
@Schema(
    title = "Transform Arrow, Parquet, or data files using queries."
)
public class ArrowTransform extends Task implements RunnableTask<ArrowTransform.Output> {

    @Schema(
        title = "The source file URI"
    )
    @PluginProperty(group = "main")
    @NotNull
    private Property<String> from;

    @Schema(
        title = "The query to execute"
    )
    @PluginProperty(group = "main")
    @NotNull
    private Property<String> query;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String renderedFrom = runContext.render(this.from).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("'from' must render to a kestra:// URI"));
        String renderedQuery = runContext.render(this.query).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("'query' must render to a valid jq string"));

        // Use working directory for temp files to prevent leaks
        File tempFile = runContext.workingDir().createTempFile("arrow-", ".data");
        try (InputStream inputStream = runContext.storage().getFile(URI.create(renderedFrom))) {
            Files.copy(inputStream, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }

        // TODO: Implement actual jq / arrow transform logic using renderedQuery on tempFile,
        // write to a result file, and return its URI. For now, returning proper structure safely.
        File resultFile = runContext.workingDir().createTempFile("output-", ".json");
        URI outputUri = runContext.storage().putFile(resultFile);

        return Output.builder()
            .uri(outputUri)
            .build();
    }

    @Builder
    @Data
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The URI of the transformed file in internal storage"
        )
        private URI uri;
    }
            }
