package io.kestra.plugin.transform.grok;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Output;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Parse and structure items from a UTF-8 text or Ion file using Grok expressions",
    description = """
        The `TransformItems` task is similar to the famous Logstash Grok filter from the ELK stack.
        It is particularly useful for transforming unstructured data such as logs into a structured, indexable, and queryable data structure.

        The source file is read line by line and each line is evaluated independently. Plain-text files are supported alongside Ion/JSON content,
        while non-matching rows are ignored. Multi-line records such as stack traces are therefore treated as separate entries rather than being preserved as a single item.

        The `TransformItems` ships with all the default patterns as defined. You can find them here: https://github.com/kestra-io/plugin-transform/tree/main/plugin-transform-grok/src/main/resources/patterns.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Read a plain-text log file from internal storage and parse each line with Grok.",
            full = true,
            code = """
                id: grok_transform_items
                namespace: company.team

                tasks:
                  - id: read_log
                    type: io.kestra.plugin.core.storage.Read
                    uri: "kestra://company.team/production/app.log"

                  - id: transform_items
                    type: io.kestra.plugin.transform.grok.TransformItems
                    pattern: "%{TIMESTAMP_ISO8601:logdate} %{LOGLEVEL:loglevel} %{GREEDYDATA:message}"
                    from: "{{ outputs.read_log.uri }}"
                """
        )
    }
)
public class TransformItems extends Transform implements GrokInterface, RunnableTask<Output> {

    private static final ObjectMapper ION_MAPPER = JacksonMapper.ofIon();

    @Schema(
        title = "The file to be transformed",
        description = "Must be a `kestra://` internal storage URI pointing to a UTF-8 plain-text or Ion/JSON file. Each line is matched independently, so plain-text logs are processed one physical line at a time."
    )
    @NotNull
    @PluginProperty(internalStorageURI = true, group = "main")
    private Property<String> from;

    /**
     * {@inheritDoc}
     **/
    @Override
    public Output run(RunContext runContext) throws Exception {
        init(runContext);

        String from = runContext.render(this.from).as(String.class).orElseThrow();

        URI objectURI = new URI(from);
        try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(runContext.storage().getFile(objectURI), StandardCharsets.UTF_8),
            FileSerde.BUFFER_SIZE
        )) {
            Flux<String> flux = Flux.fromStream(reader::lines).map(TransformItems::decodeItem);
            final Path ouputFilePath = runContext.workingDir().createTempFile(".ion");
            try (Writer writer = new BufferedWriter(new OutputStreamWriter(Files.newOutputStream(ouputFilePath), StandardCharsets.UTF_8))) {

                // transform
                Flux<Map<String, Object>> values = flux.map(data -> {
                    try {
                        return matches(data.getBytes(StandardCharsets.UTF_8), runContext);
                    } catch (IllegalVariableEvaluationException e) {
                        throw new RuntimeException(e);
                    }
                }).filter(result -> result != null && !result.isEmpty());

                Long processedItemsTotal = FileSerde.writeAll(writer, values).block();
                URI uri = runContext.storage().putFile(ouputFilePath.toFile());

                // output
                return Output
                    .builder()
                    .uri(uri)
                    .processedItemsTotal(processedItemsTotal)
                    .build();
            } finally {
                Files.deleteIfExists(ouputFilePath); // ensure temp file is deleted in case of error
            }
        }
    }

    /**
     * Decodes an Ion/JSON quoted string for backward compatibility; otherwise returns the raw line.
     */
    static String decodeItem(String line) {
        int i = 0;
        while (i < line.length() && Character.isWhitespace(line.charAt(i))) {
            i++;
        }
        if (i < line.length() && line.charAt(i) == '"') {
            try (JsonParser parser = ION_MAPPER.getFactory().createParser(line.substring(i))) {
                JsonToken token = parser.nextToken();
                if (token == JsonToken.VALUE_STRING) {
                    String value = parser.getValueAsString();
                    return parser.nextToken() == null ? value : line;
                }
            } catch (IOException ignored) {
                // not a valid Ion/JSON string — treat as plain text
            }
        }
        return line;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The transformed file URI"
        )
        private final URI uri;

        @Schema(
            title = "The total number of items that was processed by the task"
        )
        private final Long processedItemsTotal;
    }
}
