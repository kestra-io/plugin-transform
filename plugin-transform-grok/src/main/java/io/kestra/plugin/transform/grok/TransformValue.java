package io.kestra.plugin.transform.grok;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Output;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.nio.charset.StandardCharsets;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Parse arbitrary text and structure it using Grok expressions.",
    description = """
        The `TransformValue` task is similar to the famous Logstash Grok filter from the ELK stack.
        It is particularly useful for transforming unstructured data such as logs into a structured, indexable, and queryable data structure.

        The `TransformValue` ships with all the default patterns as defined You can find them here: https://github.com/kestra-io/plugin-transform/tree/main/plugin-transform-grok/src/main/resources/patterns.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Consume, parse, and structure logs events from Kafka topic.",
            full = true,
            code = """
                id: grok_transform_value
                namespace: company.team

                tasks:
                  - id: transform_value
                    type: io.kestra.plugin.transform.grok.TransformValue
                    pattern: "%{TIMESTAMP_ISO8601:logdate} %{LOGLEVEL:loglevel} %{GREEDYDATA:message}"
                    from: "{{ trigger.value }}"

                  - id: log_on_warn
                    type: io.kestra.plugin.core.flow.If
                    condition: "{{ grok.value['LOGLEVEL'] == 'ERROR' }}"
                    then:
                      - id: when_true
                        type: io.kestra.plugin.core.log.Log
                        message: "{{ outputs.transform_value.value }}"

                triggers:
                  - id: realtime_trigger
                    type: io.kestra.plugin.kafka.RealtimeTrigger
                    topic: test_kestra
                    properties:
                      bootstrap.servers: localhost:9092
                    serdeProperties:
                      schema.registry.url: http://localhost:8085
                      keyDeserializer: STRING
                      valueDeserializer: STRING
                    groupId: kafkaConsumerGroupId
                """
        )
    }
)
public class TransformValue extends Transform implements GrokInterface, RunnableTask<Output> {

    private static final ObjectMapper ION_OBJECT_MAPPER = JacksonMapper.ofIon();

    @Schema(title = "The value to parse.")
    @NotNull
    private Property<String> from;

    /**
     * {@inheritDoc}
     **/
    @Override
    public Output run(RunContext runContext) throws Exception {
        init(runContext);

        String from = runContext.render(this.from).as(String.class).orElseThrow();

        // transform
        Map<String, Object> values = matches(from.getBytes(StandardCharsets.UTF_8), runContext);

        // output
        return Output.builder().value(values).build();
    }


    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The transformed value."
        )
        private final Map<String, Object> value;
    }
}
