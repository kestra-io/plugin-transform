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
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@KestraTest
class ZipTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void zipsRecordsByPosition() throws Exception {
        List<Map<String, Object>> left = List.of(
            Map.of("id", "a", "value", 1),
            Map.of("id", "b", "value", 2)
        );
        List<Map<String, Object>> right = List.of(
            Map.of("status", "ok"),
            Map.of("status", "fail")
        );

        Zip task = Zip.builder()
            .inputs(List.of(
                Property.ofValue(left),
                Property.ofValue(right)
            ))
            .build();

        RunContext runContext = runContextFactory.of(Map.of());
        Zip.Output output = task.run(runContext);

        assertThat(output.getRecords(), hasSize(2));
        Map<String, Object> first = (Map<String, Object>) output.getRecords().getFirst();
        Map<String, Object> second = (Map<String, Object>) output.getRecords().get(1);
        assertThat(first.get("id"), is("a"));
        assertThat(first.get("status"), is("ok"));
        assertThat(second.get("id"), is("b"));
        assertThat(second.get("status"), is("fail"));
    }

    @Test
    void rejectsLengthMismatch() {
        List<Map<String, Object>> left = List.of(Map.of("id", "a"));
        List<Map<String, Object>> right = List.of(Map.of("status", "ok"), Map.of("status", "fail"));

        Zip task = Zip.builder()
            .inputs(List.of(
                Property.ofValue(left),
                Property.ofValue(right)
            ))
            .build();

        RunContext runContext = runContextFactory.of(Map.of());
        TransformException exception = org.junit.jupiter.api.Assertions.assertThrows(
            TransformException.class,
            () -> task.run(runContext)
        );

        assertThat(exception.getMessage(), is("inputs must have same length: input1=1, input2=2"));
    }

    @Test
    void resolvesConflictsWithRightOverride() throws Exception {
        List<Map<String, Object>> left = List.of(Map.of("id", "a", "status", "pending"));
        List<Map<String, Object>> right = List.of(Map.of("status", "ok"));

        Zip task = Zip.builder()
            .inputs(List.of(
                Property.ofValue(left),
                Property.ofValue(right)
            ))
            .onConflict(Zip.ConflictMode.RIGHT)
            .build();

        RunContext runContext = runContextFactory.of(Map.of());
        Zip.Output output = task.run(runContext);

        Map<String, Object> record = (Map<String, Object>) output.getRecords().getFirst();
        assertThat(record.get("status"), is("ok"));
    }

    @Test
    void skipOnConflictDropsPair() throws Exception {
        List<Map<String, Object>> left = List.of(Map.of("id", "a"));
        List<Map<String, Object>> right = List.of(Map.of("id", "b"));

        Zip task = Zip.builder()
            .inputs(List.of(
                Property.ofValue(left),
                Property.ofValue(right)
            ))
            .onError(TransformOptions.OnErrorMode.SKIP)
            .onConflict(Zip.ConflictMode.FAIL)
            .build();

        RunContext runContext = runContextFactory.of(Map.of());
        Zip.Output output = task.run(runContext);

        assertThat(output.getRecords(), hasSize(0));
    }
}
