package io.kestra.plugin.transform;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.transform.util.TransformException;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@KestraTest
class FilterTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void filtersRecords() throws Exception {
        java.util.Map<String, Object> keep = java.util.Map.of("active", true, "total", 10);
        java.util.Map<String, Object> drop = java.util.Map.of("active", false, "total", 20);

        Filter task = Filter.builder()
            .from(Property.ofValue(List.of(keep, drop)))
            .where(Property.ofValue("active"))
            .build();

        RunContext runContext = runContextFactory.of(java.util.Map.of());
        Filter.Output output = task.run(runContext);

        assertThat(output.getRecords(), hasSize(1));
        java.util.Map<String, Object> record = (java.util.Map<String, Object>) output.getRecords().getFirst();
        assertThat(record.get("active"), is(true));
        assertThat(output.getRecords(), hasSize(1));
    }

    @Test
    void keepOnErrorPreservesRecord() throws Exception {
        java.util.Map<String, Object> record = java.util.Map.of("total", new BigDecimal("10"));

        Filter task = Filter.builder()
            .from(Property.ofValue(List.of(record)))
            .where(Property.ofValue("total"))
            .onError(Property.ofValue(Filter.OnErrorMode.KEEP))
            .build();

        RunContext runContext = runContextFactory.of(java.util.Map.of());
        Filter.Output output = task.run(runContext);

        assertThat(output.getRecords(), hasSize(1));
    }

    @Test
    void skipOnErrorDropsRecord() throws Exception {
        java.util.Map<String, Object> record = java.util.Map.of("total", new BigDecimal("10"));

        Filter task = Filter.builder()
            .from(Property.ofValue(List.of(record)))
            .where(Property.ofValue("total"))
            .onError(Property.ofValue(Filter.OnErrorMode.SKIP))
            .build();

        RunContext runContext = runContextFactory.of(java.util.Map.of());
        Filter.Output output = task.run(runContext);

        assertThat(output.getRecords(), hasSize(0));
    }

    @Test
    void rejectsMissingWhere() {
        Filter task = Filter.builder()
            .from(Property.ofValue(List.of()))
            .build();

        RunContext runContext = runContextFactory.of(java.util.Map.of());

        TransformException exception = org.junit.jupiter.api.Assertions.assertThrows(
            TransformException.class,
            () -> task.run(runContext)
        );

        assertThat(exception.getMessage(), is("where is required"));
    }

    @Test
    void outputModeUriIsRejected() {
        org.junit.jupiter.api.Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> Filter.OutputMode.from("URI")
        );
    }
}
