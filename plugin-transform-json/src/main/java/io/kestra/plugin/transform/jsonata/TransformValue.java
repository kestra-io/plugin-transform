package io.kestra.plugin.transform.jsonata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
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

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Transform or query a JSON data using JSONata language.",
    description = "[JSONata](https://jsonata.org/) is a sophisticated query and transformation language for JSON data."
)
@Plugin(
    examples = {
        @Example(
            title = "Transform JSON data using JSONata expression",
            full = true,
            code = """
                id: jsonata_transform_value
                namespace: company.team

                tasks:
                  - id: transform_json
                    type: io.kestra.plugin.transform.jsonata.TransformValue
                    from: |
                      {
                        "order_id": "ABC123",
                        "first_name": "John",
                        "last_name": "Doe",
                        "address": {
                          "city": "Paris",
                          "country": "France"
                        }, 
                        "items": [
                          {
                            "product_id": "001",
                            "name": "Apple",
                            "quantity": 5,
                            "price_per_unit": 0.5
                          },
                          {
                            "product_id": "002",
                            "name": "Banana",
                            "quantity": 3,
                            "price_per_unit": 0.3
                          },
                          {
                            "product_id": "003",
                            "name": "Orange",
                            "quantity": 2,
                            "price_per_unit": 0.4
                          }
                        ]
                      }
                    expression: |
                      {
                        "order_id": order_id,
                        "customer_name": first_name & ' ' & last_name,
                        "address": address.city & ', ' & address.country,
                        "total_price": $sum(items.(quantity * price_per_unit))
                      }
                """
        )
    }
)
public class TransformValue extends Transform implements RunnableTask<Output> {

    public static final ObjectMapper JSON_OBJECT_MAPPER = JacksonMapper.ofJson();

    @Schema(
        title = "The value to be transformed.",
        description = "Must be a valid JSON string."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String from;


    /**
     * {@inheritDoc}
     **/
    @Override
    public Output run(RunContext runContext) throws Exception {
        init(runContext);

        final JsonNode from = parseJson(runContext.render(this.from));

        // transform
        JsonNode transformed = evaluateExpression(from);

        // output
        return Output.builder().value(transformed).build();
    }

    private static JsonNode parseJson(String from) {
        try {
            return JSON_OBJECT_MAPPER.readTree(from);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to parse the JSON object passed through the `from` property.", e);
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The transformed value."
        )
        private final Object value;
    }
}
