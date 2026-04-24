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
    private Property<Integer> maxDepth = Property.ofValue(200);

    @Getter(AccessLevel.NONE)
    private String parsedExpressionSource;

    @Getter(AccessLevel.NONE)
    private Jsonata parsedExpression;

    public void init(RunContext runContext) throws Exception {
        this.parsedExpressionSource = runContext.render(this.expression).as(String.class).orElseThrow();
        try {
            this.parsedExpression = jsonata(this.parsedExpressionSource);
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
        } catch (StackOverflowError e) {
            // Jsonata instance has mutable fields (errors, environment) that may be in a partial state
            // after overflow. Re-parse to get a clean instance so subsequent evaluations are unaffected.
            try {
                this.parsedExpression = jsonata(this.parsedExpressionSource);
            } catch (JException ignored) {
                // expression was valid before; ignore re-parse failure
            }
            throw new RuntimeException("JSONata expression exceeded JVM stack depth — reduce expression complexity or lower maxDepth", e);
        }
    }
}
