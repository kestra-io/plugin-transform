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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.kestra.core.models.enums.MonacoLanguages;
import io.kestra.core.models.annotations.PluginProperty;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class Transform<T extends Output> extends Task implements JSONataInterface, RunnableTask<T> {

    private static final ObjectMapper MAPPER = JacksonMapper.ofJson();
    // 4 MB: fits default maxDepth=50 × ~8 JVM frames/level with large headroom.
    // Also isolates StackOverflowError inside the eval thread so the worker thread never crashes.
    private static final long EVAL_THREAD_STACK_SIZE = 4 * 1024 * 1024;

    @PluginProperty(language = MonacoLanguages.JAVASCRIPT, group = "advanced")
    private Property<String> expression;

    // Default 50: each JSONata recursion level pushes ~8 JVM frames; 256 KB worker stacks
    // (~300 usable frames) overflow before maxDepth fires at 200. 50 × 8 = 400 frames — safe.
    // Users needing deeper recursion should increase both this value and the JVM stack size.
    @Builder.Default
    private Property<Integer> maxDepth = Property.ofValue(50);

    @Getter(AccessLevel.PRIVATE)
    private Jsonata parsedExpression;

    // Lazy-initialized; lifecycle managed by evalExecutor() / shutdownEvalExecutor().
    // Assumption: Flux pipelines in subclasses are sequential (no parallel()/publishOn).
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private transient ExecutorService evalExecutor;

    private ExecutorService evalExecutor() {
        if (this.evalExecutor == null) {
            this.evalExecutor = Executors.newSingleThreadExecutor(r -> {
                var t = new Thread(null, r, "jsonata-eval", EVAL_THREAD_STACK_SIZE);
                t.setDaemon(true);
                return t;
            });
        }
        return this.evalExecutor;
    }

    protected void shutdownEvalExecutor() {
        if (this.evalExecutor != null) {
            this.evalExecutor.shutdown();
            try {
                this.evalExecutor.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            this.evalExecutor = null;
        }
    }

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

            var resultRef = new AtomicReference<JsonNode>();
            var errorRef = new AtomicReference<Throwable>();

            // Eval runs on a dedicated executor thread (4 MB stack) that is reused across all records
            // in the same task run. This serves two purposes:
            // 1. Normal case: worker stack size (e.g. 256 KB on Windows) cannot constrain the evaluator.
            // 2. Edge case (user sets very high maxDepth): if a StackOverflowError occurs in the eval
            //    thread, it is contained there. The worker thread reads the stored error and throws a
            //    clean RuntimeException — the worker never crashes.
            // The catch is intentionally Throwable: this is a throwaway-thread sandbox, so every escape
            // (including Errors like StackOverflowError and OutOfMemoryError) must land in errorRef.
            // A narrower catch would let some Errors escape, leaving both refs null and producing a
            // silent-null return after future.get().
            var future = evalExecutor().submit(() -> {
                try {
                    var result = this.parsedExpression.evaluate(data, frame);
                    resultRef.set(result != null ? MAPPER.valueToTree(result) : NullNode.getInstance());
                } catch (Throwable t) {
                    errorRef.set(t);
                }
                return null;
            });

            try {
                future.get();
            } catch (ExecutionException e) {
                throw new RuntimeException("Failed to evaluate expression", e.getCause());
            }

            if (errorRef.get() != null) {
                throw new RuntimeException("Failed to evaluate expression", errorRef.get());
            }
            return resultRef.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("JSONata evaluation interrupted", e);
        } catch (IllegalVariableEvaluationException e) {
            throw new RuntimeException("Failed to evaluate expression", e);
        }
    }
}
