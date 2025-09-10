package io.kestra.plugin.transform.jsonata;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.StreamSupport;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Transform or query a JSON or ION file using JSONata.",
    description = "[JSONata](https://jsonata.org/) is a query and transformation language for JSON data."
)
@Plugin(
    examples = {
        @Example(
            title = "Transform a JSON file using a JSONata expression.",
            full = true,
            code = """
                id: jsonata_example
                namespace: company.team

                tasks:
                  - id: http_download
                    type: io.kestra.plugin.core.http.Download
                    uri: https://dummyjson.com/products

                  - id: get_product_and_brand_name
                    description: "String Transformation"
                    type: io.kestra.plugin.transform.jsonata.TransformItems
                    from: "{{ outputs.http_download.uri }}"
                    expression: products.(title & ' by ' & brand)

                  - id: get_total_price
                    description: "Number Transformation"
                    type: io.kestra.plugin.transform.jsonata.TransformItems
                    from: "{{ outputs.http_download.uri }}"
                    expression: $sum(products.price)

                  - id: sum_up
                    description: "Writing out results in the form of JSON"
                    type: io.kestra.plugin.transform.jsonata.TransformItems
                    from: "{{ outputs.http_download.uri }}"
                    expression: |
                      {
                        "total_products": $count(products),
                        "total_price": $sum(products.price),
                        "total_discounted_price": $sum(products.(price-(price*discountPercentage/100)))
                      }
                """
        ),
        @Example(
            title = "Calculate the total price per order from a JSON file.",
            full = true,
            code = """
                id: jsonata
                namespace: company.team

                tasks:
                  - id: extract
                    type: io.kestra.plugin.core.http.Download
                    uri: https://huggingface.co/datasets/kestra/datasets/resolve/main/json/orders.json

                  - id: jsonata
                    type: io.kestra.plugin.transform.jsonata.TransformItems
                    from: "{{ outputs.extract.uri }}"
                    expression: |
                      Account.Order.{
                        "order_id": OrderID,
                        "total": $round($sum(Product.(Price * Quantity)), 2)
                      }
                """
        )
    }
)
public class TransformItems extends Transform<TransformItems.Output> implements RunnableTask<TransformItems.Output> {

    private static final ObjectMapper ION_OBJECT_MAPPER = JacksonMapper.ofIon();

    @Schema(
        title = "The file to be transformed",
        description = "Must be a `kestra://` internal storage URI."
    )
    @NotNull
    @PluginProperty(internalStorageURI = true)
    private Property<String> from;

    @Schema(
        title = "Specifies whether to split arrays into separate records",
        description = "If the JSONata expression results in a JSON array and this property is set to `true`, then a record will be written for each element. Otherwise, the JSON array is kept as a single record."
    )
    @NotNull
    @Builder.Default
    private Property<Boolean> explodeArray = Property.ofValue(true);

    /**
     * {@inheritDoc}
     **/
    @Override
    public Output run(RunContext runContext) throws Exception {

        init(runContext);

        final URI from = new URI(runContext.render(this.from).as(String.class).orElseThrow());

        try (Reader reader = new BufferedReader(new InputStreamReader(runContext.storage().getFile(from)), FileSerde.BUFFER_SIZE)) {
            Flux<JsonNode> flux = FileSerde.readAll(reader, new TypeReference<>() {
            });
            final Path ouputFilePath = runContext.workingDir().createTempFile(".ion");
            try(Writer writer = new BufferedWriter(new OutputStreamWriter(Files.newOutputStream(ouputFilePath)))) {

                // transform
                Flux<JsonNode> values = flux.map(node -> this.evaluateExpression(runContext, node));

                if (runContext.render(explodeArray).as(Boolean.class).orElseThrow()) {
                    values = values.flatMap(jsonNode -> {
                        if (jsonNode.isArray()) {
                            Iterable<JsonNode> iterable = jsonNode::elements;
                            return Flux.fromStream(StreamSupport.stream(iterable.spliterator(), false));
                        }
                        return Mono.just(jsonNode);
                    });
                }

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
            title = "File URI containing the result of transformation."
        )
        private final URI uri;

        @Schema(
            title = "The total number of items that was processed by the task."
        )
        private final Long processedItemsTotal;
    }
}
