package io.kestra.plugin.transform;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.transform.util.TransformException;
import io.kestra.plugin.transform.util.TransformOptions;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@KestraTest
class UnnestTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void explodesArrayField() throws Exception {
        java.util.Map<String, Object> record = java.util.Map.of(
            "order_id", "o1",
            "items", List.of(
                java.util.Map.of("sku", "A"),
                java.util.Map.of("sku", "B")
            )
        );

        Unnest task = Unnest.builder()
            .from(Property.ofValue(List.of(record)))
            .path(Property.ofValue("items[]"))
            .as(Property.ofValue("item"))
            .build();

        RunContext runContext = runContextFactory.of(java.util.Map.of());
        Unnest.Output output = task.run(runContext);

        assertThat(output.getRecords(), hasSize(2));
        java.util.Map<String, Object> first = (java.util.Map<String, Object>) output.getRecords().getFirst();
        java.util.Map<String, Object> second = (java.util.Map<String, Object>) output.getRecords().get(1);
        assertThat(((java.util.Map<String, Object>) first.get("item")).get("sku"), is("A"));
        assertThat(((java.util.Map<String, Object>) second.get("item")).get("sku"), is("B"));
        assertThat(first.containsKey("items"), is(false));
        assertThat(second.containsKey("items"), is(false));
    }

    @Test
    void nullOnErrorProducesSingleRecord() throws Exception {
        java.util.Map<String, Object> record = java.util.Map.of(
            "items", java.util.Map.of("sku", "A")
        );

        Unnest task = Unnest.builder()
            .from(Property.ofValue(List.of(record)))
            .path(Property.ofValue("items[]"))
            .as(Property.ofValue("item"))
            .onError(TransformOptions.OnErrorMode.NULL)
            .build();

        RunContext runContext = runContextFactory.of(java.util.Map.of());
        Unnest.Output output = task.run(runContext);

        assertThat(output.getRecords(), hasSize(1));
        java.util.Map<String, Object> mapped = (java.util.Map<String, Object>) output.getRecords().getFirst();
        assertThat(mapped.containsKey("item"), is(true));
        assertThat(mapped.get("item"), is((Object) null));
    }

    @Test
    void skipOnErrorDropsRecord() throws Exception {
        java.util.Map<String, Object> record = java.util.Map.of(
            "items", java.util.Map.of("sku", "A")
        );

        Unnest task = Unnest.builder()
            .from(Property.ofValue(List.of(record)))
            .path(Property.ofValue("items[]"))
            .as(Property.ofValue("item"))
            .onError(TransformOptions.OnErrorMode.SKIP)
            .build();

        RunContext runContext = runContextFactory.of(java.util.Map.of());
        Unnest.Output output = task.run(runContext);

        assertThat(output.getRecords(), hasSize(0));
    }

    @Test
    void rejectsMissingPath() {
        Unnest task = Unnest.builder()
            .from(Property.ofValue(List.of()))
            .as(Property.ofValue("item"))
            .build();

        RunContext runContext = runContextFactory.of(java.util.Map.of());

        TransformException exception = org.junit.jupiter.api.Assertions.assertThrows(
            TransformException.class,
            () -> task.run(runContext)
        );

        assertThat(exception.getMessage(), is("path is required"));
    }
}
