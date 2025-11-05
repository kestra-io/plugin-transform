package io.kestra.plugin.transform.grok;

import com.fasterxml.jackson.core.type.TypeReference;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Output;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
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
    title = "Parse and structure items from a JSON internal storage file using Grok expressions.",
    description = """
        The `TransformItems` task is similar to the famous Logstash Grok filter from the ELK stack.
        It is particularly useful for transforming unstructured data such as logs into a structured, indexable, and queryable data structure.

        The `TransformItems` ships with all the default patterns as defined You can find them here: https://github.com/kestra-io/plugin-transform/tree/main/plugin-transform-grok/src/main/resources/patterns.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Consume, parse, and structure logs events from Kafka topic.",
            full = true,
            code = """
                id: grok_transform_items
                namespace: company.team

                tasks:
                  - id: transform_items
                    type: io.kestra.plugin.transform.grok.TransformItems
                    pattern: "%{TIMESTAMP_ISO8601:logdate} %{LOGLEVEL:loglevel} %{GREEDYDATA:message}"
                    from: "{{ trigger.uri }}"

                triggers:
                  - id: trigger
                    type: io.kestra.plugin.kafka.Trigger
                    topic: test_kestra
                    properties:
                      bootstrap.servers: localhost:9092
                    serdeProperties:
                      schema.registry.url: http://localhost:8085
                      keyDeserializer: STRING
                      valueDeserializer: STRING
                    groupId: kafkaConsumerGroupId
                    interval: PT30S
                    maxRecords: 5
                """
        )
    }
)
public class TransformItems extends Transform implements GrokInterface, RunnableTask<Output> {

    @Schema(
        title = "The file to be transformed.",
        description = "Must be a `kestra://` internal storage URI."
    )
    @NotNull
    @PluginProperty(internalStorageURI = true)
    private Property<String> from;

    /**
     * {@inheritDoc}
     **/
    @Override
    public Output run(RunContext runContext) throws Exception {
        init(runContext);

        String from = runContext.render(this.from).as(String.class).orElseThrow();

        URI objectURI = new URI(from);
        try (Reader reader = new BufferedReader(new InputStreamReader(runContext.storage().getFile(objectURI)), FileSerde.BUFFER_SIZE)) {
            Flux<String> flux = FileSerde.readAll(reader, new TypeReference<>() {
            });
            final Path ouputFilePath = runContext.workingDir().createTempFile(".ion");
            try (Writer writer = new BufferedWriter(new OutputStreamWriter(Files.newOutputStream(ouputFilePath)))) {

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

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The transformed file URI."
        )
        private final URI uri;

        @Schema(
            title = "The total number of items that was processed by the task."
        )
        private final Long processedItemsTotal;
    }
}
