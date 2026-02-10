package io.kestra.plugin.transform;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.transform.ion.IonTypeName;
import io.kestra.plugin.transform.util.TransformException;
import io.kestra.plugin.transform.util.TransformOptions;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@KestraTest
class AggregateTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void aggregatesByGroup() throws Exception {
        java.util.Map<String, Object> first = java.util.Map.of(
            "customer_id", "c1",
            "country", "FR",
            "total_spent", new BigDecimal("10.00")
        );
        java.util.Map<String, Object> second = java.util.Map.of(
            "customer_id", "c1",
            "country", "FR",
            "total_spent", new BigDecimal("5.50")
        );
        java.util.Map<String, Object> third = java.util.Map.of(
            "customer_id", "c2",
            "country", "US",
            "total_spent", new BigDecimal("7.00")
        );

        Aggregate task = Aggregate.builder()
            .from(Property.ofValue(List.of(first, second, third)))
            .groupBy(Property.ofValue(List.of("customer_id", "country")))
            .aggregates(java.util.Map.of(
                "order_count", Aggregate.AggregateDefinition.builder().expr("count()").type(IonTypeName.INT).build(),
                "total_spent", Aggregate.AggregateDefinition.builder().expr("sum(total_spent)").type(IonTypeName.DECIMAL).build()
            ))
            .build();

        RunContext runContext = runContextFactory.of(java.util.Map.of());
        Aggregate.Output output = task.run(runContext);

        assertThat(output.getRecords(), hasSize(2));
        assertThat(output.getRecords(), hasSize(2));
    }

    @Test
    void nullOnErrorSetsAggregateToNull() throws Exception {
        java.util.Map<String, Object> record = java.util.Map.of(
            "customer_id", "c1",
            "total_spent", "bad"
        );

        Aggregate task = Aggregate.builder()
            .from(Property.ofValue(List.of(record)))
            .groupBy(Property.ofValue(List.of("customer_id")))
            .aggregates(java.util.Map.of(
                "total_spent", Aggregate.AggregateDefinition.builder().expr("sum(total_spent)").type(IonTypeName.DECIMAL).build()
            ))
            .onError(TransformOptions.OnErrorMode.NULL)
            .build();

        RunContext runContext = runContextFactory.of(java.util.Map.of());
        Aggregate.Output output = task.run(runContext);

        java.util.Map<String, Object> mapped = (java.util.Map<String, Object>) output.getRecords().getFirst();
        assertThat(mapped.get("total_spent"), is((Object) null));
    }

    @Test
    void aggregatesMaxTimestamp() throws Exception {
        java.util.Map<String, Object> first = java.util.Map.of(
            "customer_id", "c1",
            "created_at", "2024-01-01T00:00:00Z"
        );
        java.util.Map<String, Object> second = java.util.Map.of(
            "customer_id", "c1",
            "created_at", "2024-01-02T00:00:00Z"
        );

        Aggregate task = Aggregate.builder()
            .from(Property.ofValue(List.of(first, second)))
            .groupBy(Property.ofValue(List.of("customer_id")))
            .aggregates(java.util.Map.of(
                "last_order_at", Aggregate.AggregateDefinition.builder()
                    .expr("max(parseTimestamp(created_at))")
                    .type(IonTypeName.TIMESTAMP)
                    .build()
            ))
            .build();

        RunContext runContext = runContextFactory.of(java.util.Map.of());
        Aggregate.Output output = task.run(runContext);

        java.util.Map<String, Object> mapped = (java.util.Map<String, Object>) output.getRecords().getFirst();
        assertThat(mapped.get("last_order_at"), is("2024-01-02T00:00:00Z"));
    }

    @Test
    void rejectsMissingGroupBy() {
        Aggregate task = Aggregate.builder()
            .from(Property.ofValue(List.of()))
            .aggregates(java.util.Map.of(
                "order_count", Aggregate.AggregateDefinition.builder().expr("count()").build()
            ))
            .build();

        RunContext runContext = runContextFactory.of(java.util.Map.of());

        TransformException exception = org.junit.jupiter.api.Assertions.assertThrows(
            TransformException.class,
            () -> task.run(runContext)
        );

        assertThat(exception.getMessage(), is("groupBy is required"));
    }
}
