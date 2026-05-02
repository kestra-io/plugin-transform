package io.kestra.plugin.transform.jsonata;

import com.dashjoin.jsonata.JException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@KestraTest
class TransformValueTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldGetOutputForValidExprReturningStringForFromJSON() throws Exception {
        // Given
        RunContext runContext = runContextFactory.of();
        TransformValue task = TransformValue.builder()
            .from(Property.ofValue(Features.DATASET_ACCOUNT_ORDER_JSON))
            .expression(Property.ofValue(Features.DATASET_ACCOUNT_ORDER_EXPR))
            .build();

        // When
        TransformValue.Output output = task.run(runContext);

        // Then
        Assertions.assertNotNull(output);

        Assertions.assertEquals(Features.DATASET_ACCOUNT_ORDER_EXPR_RESULT, output.getValue().toString());
    }

    @Test
    void shouldGetOutputForValidExprReturningObjectForFromJSON() throws Exception {
        // Given
        RunContext runContext = runContextFactory.of();
        TransformValue task = TransformValue.builder()
            .from(Property.ofValue("""
                {
                  "order_id": "ABC123",
                  "customer_name": "John Doe",
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
                """))
            .expression(Property.ofValue("""
                     {
                        "order_id": order_id,
                        "customer_name": customer_name,
                        "total_price": $sum(items.(quantity * price_per_unit))
                     }
                """))
            .build();

        // When
        TransformValue.Output output = task.run(runContext);

        // Then
        Assertions.assertNotNull(output);
        Assertions.assertEquals("{\"order_id\":\"ABC123\",\"customer_name\":\"John Doe\",\"total_price\":4.2}", output.getValue().toString());
    }

    @Test
    void shouldHandleNestedArrayExpressionFromIssue40() throws Exception {
        String input = """
            {
              "filterTuples": [
                {
                  "filter": [
                    {"parent": {"parent": {"hybrisId": "8796977876513"}, "hybrisId": "8796995440161"}, "hybrisId": "8796998946337"},
                    {"parent": {"parent": {"hybrisId": "8796977876513"}, "hybrisId": "8796995472929"}, "hybrisId": "8797002583585"},
                    {"parent": {"parent": {"hybrisId": "8796977843745"}, "hybrisId": "8796995341857"}, "hybrisId": "8796999798305"}
                  ]
                },
                {
                  "filter": [
                    {"parent": {"parent": {"hybrisId": "8796977876513"}, "hybrisId": "8796995440161"}, "hybrisId": "8796998946337"},
                    {"parent": {"parent": {"hybrisId": "8796977876513"}, "hybrisId": "8796995472929"}, "hybrisId": "8797002583585"},
                    {"parent": {"parent": {"hybrisId": "8796977843745"}, "hybrisId": "8796995341857"}, "hybrisId": "8796999765537"}
                  ]
                }
              ]
            }
            """;
        var expression = "[filterTuples.[filter.(parent.parent.hybrisId & \"/\" & parent.hybrisId & \"/\" & hybrisId)]]";

        var task = TransformValue.builder()
            .id("test")
            .type(TransformValue.class.getName())
            .from(Property.ofValue(input))
            .expression(Property.ofValue(expression))
            .build();

        var runContext = runContextFactory.of();
        var output = task.run(runContext);

        // Verify it's a nested array (not flattened)
        assertThat(output.getValue()).isNotNull();
        // The result should be a JSON array of arrays
        var mapper = new ObjectMapper();
        JsonNode result = mapper.valueToTree(output.getValue());
        assertThat(result.isArray()).isTrue();
        assertThat(result.size()).isEqualTo(2);
        assertThat(result.get(0).isArray()).isTrue();
        assertThat(result.get(0).size()).isEqualTo(3);
        assertThat(result.get(0).get(0).asText()).isEqualTo("8796977876513/8796995440161/8796998946337");
        assertThat(result.get(0).get(1).asText()).isEqualTo("8796977876513/8796995472929/8797002583585");
        assertThat(result.get(0).get(2).asText()).isEqualTo("8796977843745/8796995341857/8796999798305");
        assertThat(result.get(1).isArray()).isTrue();
        assertThat(result.get(1).get(2).asText()).isEqualTo("8796977843745/8796995341857/8796999765537");
    }

    // Regression tests for StackOverflow protection in evaluateExpression().
    //
    // Root cause: each JSONata recursion level pushes ~8 JVM frames. On 256 KB worker stacks
    // (~300 usable frames), even maxDepth=200 allows 200 × 8 = 1600 frames — far past overflow.
    //
    // Fix (two layers):
    //   1. Default maxDepth lowered to 50 (50 × 8 = 400 frames — safe on 256 KB stacks).
    //      Bounds check fires and throws JException before any stack risk.
    //   2. Evaluation runs on a dedicated thread with a 4 MB stack. If the user sets a high
    //      maxDepth that allows overflow, the StackOverflowError is caught as Throwable inside
    //      the throwaway eval thread. The worker thread reads the stored error and throws a
    //      clean RuntimeException — the worker never crashes.
    //
    // Production crash: Windows worker default stack ~256 KB, crashed at depth=999.
    // Test JVM is pinned to -Xss512k (see build.gradle).
    // "+ 0" makes the expression non-tail-recursive, preventing TCO, so frames stay live.

    @ParameterizedTest
    @ValueSource(ints = {50, 200, 500, 1000})
    void shouldNeverThrowStackOverflowForCommonMaxDepthValues(int maxDepth) throws Exception {
        // Each maxDepth value runs on a 4 MB eval thread. The bounds check fires at maxDepth
        // (JException) well before the stack could overflow, regardless of worker stack size.
        RunContext runContext = runContextFactory.of();
        TransformValue task = TransformValue.builder()
            .from(Property.ofValue("{}"))
            .expression(Property.ofValue(
                "($f := function($n) { $n > 0 ? $f($n - 1) + 0 : 0 }; $f(10000))"
            ))
            .maxDepth(Property.ofValue(maxDepth))
            .build();

        assertThatThrownBy(() -> task.run(runContext))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("Failed to evaluate expression")
            .hasCauseInstanceOf(JException.class);
    }

    @Test
    void shouldIsolateStackOverflowInEvalThreadWhenMaxDepthExceedsStackCapacity() throws Exception {
        // User sets maxDepth high enough that bounds check never fires before stack exhaustion.
        // On 4 MB eval thread (~40k safe levels), $f(49999) overflows the eval thread.
        // StackOverflowError is caught as Throwable inside the eval thread; worker thread gets
        // a clean RuntimeException instead of crashing.
        RunContext runContext = runContextFactory.of();
        TransformValue task = TransformValue.builder()
            .from(Property.ofValue("{}"))
            .expression(Property.ofValue(
                "($f := function($n) { $n > 0 ? $f($n - 1) + 0 : 0 }; $f(49999))"
            ))
            .maxDepth(Property.ofValue(50000))
            .build();

        assertThatThrownBy(() -> task.run(runContext))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("Failed to evaluate expression")
            .hasCauseInstanceOf(StackOverflowError.class);
    }
}