package io.kestra.plugin.transform.jsonata;

import com.api.jsonata4java.expressions.EvaluateException;
import com.api.jsonata4java.expressions.Expressions;
import com.api.jsonata4java.expressions.ParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.tasks.Output;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class Transform<T extends Output> extends Task implements JSONataInterface, RunnableTask<T> {

    private static final ObjectMapper ION_OBJECT_MAPPER = JacksonMapper.ofIon();

    private String expression;

    @Builder.Default
    private Integer maxDepth = 1000;

    @Getter(AccessLevel.PRIVATE)
    private Expressions expressions;

    public void init(RunContext runContext) throws Exception {
        this.expressions = parseExpression(runContext);
    }

    protected JsonNode evaluateExpression(RunContext runContext, JsonNode jsonNode) {
        try {
            long timeoutInMilli = runContext.render(getTimeout()).as(Duration.class)
                .map(Duration::toMillis)
                .orElse(Long.MAX_VALUE);

            return this.expressions.evaluate(jsonNode, timeoutInMilli, getMaxDepth());
        } catch (EvaluateException | IllegalVariableEvaluationException e) {
            throw new RuntimeException("Failed to evaluate expression", e);
        }
    }

    private Expressions parseExpression(RunContext runContext) throws IllegalVariableEvaluationException {
        try {
            return Expressions.parse(runContext.render(this.expression));
        } catch (ParseException | IOException e) {
            throw new IllegalArgumentException("Invalid JSONata expression. Error: " + e.getMessage(), e);
        }
    }
}
