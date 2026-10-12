package io.kestra.plugin.transform.arrow;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import io.swagger.v3.oas.annotations.media.Schema;

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
@Schema(
    title = "Transform Arrow, Parquet, CSV, or NDJSON data using jq-style queries."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Transform data using a jq query",
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
        title = "The jq query to execute"
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
 // Download file from internal storage
        File tempFile = File.createTempFile("arrow-", ".data");
        try (InputStream inputStream = runContext.storage().getFile(URI.create(renderedFrom))) {
            Files.copy(inputStream, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }

        // Output destination URI
        URI outputUri = runContext.storage().putFile(File.createTempFile("output-", ".json"));

        return Output.builder()
            .uri(outputUri)
            .build();
    }

    @Builder
    @Data
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The URI of the transformed file in internal storage"
        }
        private URI uri;
    }
          }
          
