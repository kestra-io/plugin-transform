package io.kestra.plugin.transform.jsonata;

import static com.dashjoin.jsonata.Jsonata.jsonata;

import com.dashjoin.jsonata.JException;
import com.dashjoin.jsonata.Jsonata;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Output;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.time.Duration;

import io.kestra.core.models.enums.MonacoLanguages;
import io.kestra.core.models.annotations.PluginProperty;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class Transform<T extends Output> extends Task implements JSONataInterface, RunnableTask<T> {

    private static final ObjectMapper MAPPER = JacksonMapper.ofJson();

    @PluginProperty(language = MonacoLanguages.JAVASCRIPT, group = "advanced")
    private Property<String> expression;

    @Builder.Default
    private Property<Integer> maxDepth = Property.ofValue(1000);

    @Getter(AccessLevel.PRIVATE)
    private Jsonata parsedExpression;

    public void init(RunContext runContext) throws Exception {
        var exprString = runContext.render(this.expression).as(String.class).orElseThrow();
        try {
            this.parsedExpression = jsonata(exprString);
        } catch (JException e) {
            throw new IllegalArgumentException("Invalid JSONata expression. Error: " + e.getMessage(), e);
        }
    }

    protected JsonNode evaluateExpression(RunContext runContext, JsonNode jsonNode) {
        try {
            var timeoutInMilli = runContext.render(getTimeout()).as(Duration.class)
                .map(Duration::toMillis)
                .orElse(Long.MAX_VALUE);
            var rMaxDepth = runContext.render(getMaxDepth()).as(Integer.class).orElseThrow();

            var data = MAPPER.convertValue(jsonNode, Object.class);
            var frame = this.parsedExpression.createFrame();
            frame.setRuntimeBounds(timeoutInMilli, rMaxDepth);

            var result = this.parsedExpression.evaluate(data, frame);
            if (result == null) {
                return NullNode.getInstance();
            }
            return MAPPER.valueToTree(result);
        } catch (JException | IllegalVariableEvaluationException e) {
            throw new RuntimeException("Failed to evaluate expression", e);
        }
    }
}
