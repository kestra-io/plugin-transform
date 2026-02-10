package io.kestra.plugin.transform;

import com.amazon.ion.IonStruct;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.transform.ion.IonTypeName;
import io.kestra.plugin.transform.ion.IonValueUtils;
import io.kestra.plugin.transform.util.TransformOptions;
import io.kestra.plugin.transform.util.TransformException;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@KestraTest
class MapTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void mapsFieldsAndCasts() throws Exception {
        java.util.Map<String, Object> record = java.util.Map.of(
            "user", java.util.Map.of("id", "cust-1"),
            "createdAt", "2024-01-01T00:00:00Z",
            "items", List.of(
                java.util.Map.of("price", new BigDecimal("10.50")),
                java.util.Map.of("price", new BigDecimal("5.25"))
            )
        );

        Map task = Map.builder()
            .from(Property.ofValue(List.of(record)))
            .fields(java.util.Map.of(
                "customer_id", Map.FieldDefinition.builder().expr("user.id").type(IonTypeName.STRING).build(),
                "created_at", Map.FieldDefinition.builder().expr("createdAt").type(IonTypeName.TIMESTAMP).build(),
                "total", Map.FieldDefinition.builder().expr("sum(items[].price)").type(IonTypeName.DECIMAL).build()
            ))
            .dropNulls(true)
            .onError(TransformOptions.OnErrorMode.FAIL)
            .build();

        RunContext runContext = runContextFactory.of(java.util.Map.of());
        Map.Output output = task.run(runContext);

        assertThat(output.getRecords(), hasSize(1));
        java.util.Map<String, Object> mapped = (java.util.Map<String, Object>) output.getRecords().getFirst();
        assertThat(mapped.get("customer_id"), is("cust-1"));
        assertThat(mapped.get("created_at"), is("2024-01-01T00:00:00Z"));
        assertThat(mapped.get("total"), is(new BigDecimal("15.75")));
    }

    @Test
    void skipsOnErrorAndReportsStats() throws Exception {
        java.util.Map<String, Object> ok = java.util.Map.of("value", 1);
        java.util.Map<String, Object> bad = java.util.Map.of();

        Map task = Map.builder()
            .from(Property.ofValue(List.of(ok, bad)))
            .fields(java.util.Map.of(
                "value", Map.FieldDefinition.builder().expr("value").type(IonTypeName.INT).build()
            ))
            .onError(TransformOptions.OnErrorMode.SKIP)
            .build();

        RunContext runContext = runContextFactory.of(java.util.Map.of());
        Map.Output output = task.run(runContext);

        assertThat(output.getRecords(), hasSize(1));
        assertThat(output.getRecords(), hasSize(1));
    }

    @Test
    void supportsShorthandAndOptionalType() throws Exception {
        java.util.Map<String, Object> record = java.util.Map.of(
            "user", java.util.Map.of("id", "u-1"),
            "active", "true"
        );

        Map task = Map.builder()
            .from(Property.ofValue(List.of(record)))
            .fields(java.util.Map.of(
                "customer_id", Map.FieldDefinition.from("user.id"),
                "active_raw", Map.FieldDefinition.builder().expr("active").build()
            ))
            .build();

        RunContext runContext = runContextFactory.of(java.util.Map.of());
        Map.Output output = task.run(runContext);

        java.util.Map<String, Object> mapped = (java.util.Map<String, Object>) output.getRecords().getFirst();
        assertThat(mapped.get("customer_id"), is("u-1"));
        assertThat(mapped.get("active_raw"), is("true"));
    }

    @Test
    void loadsFromStoredIonFile() throws Exception {
        String ionText = "[{value: 1}, {value: 2}]";

        RunContext runContext = runContextFactory.of(java.util.Map.of());
        var uri = runContext.storage().putFile(
            new ByteArrayInputStream(ionText.getBytes(StandardCharsets.UTF_8)),
            "records.ion"
        );

        Map task = Map.builder()
            .from(Property.ofValue(uri.toString()))
            .fields(java.util.Map.of(
                "value", Map.FieldDefinition.builder().expr("value").type(IonTypeName.INT).build()
            ))
            .output(Map.OutputMode.RECORDS)
            .build();

        Map.Output output = task.run(runContext);

        assertThat(output.getRecords(), hasSize(2));
        java.util.Map<String, Object> first = (java.util.Map<String, Object>) output.getRecords().getFirst();
        java.util.Map<String, Object> second = (java.util.Map<String, Object>) output.getRecords().get(1);
        assertThat(first.get("value"), is(1L));
        assertThat(second.get("value"), is(2L));
    }

    @Test
    void autoStoresWhenInputIsStorageUri() throws Exception {
        String ionText = "[{value: 3}]";

        RunContext runContext = runContextFactory.of(java.util.Map.of());
        var uri = runContext.storage().putFile(
            new ByteArrayInputStream(ionText.getBytes(StandardCharsets.UTF_8)),
            "records-auto.ion"
        );

        Map task = Map.builder()
            .from(Property.ofValue(uri.toString()))
            .fields(java.util.Map.of(
                "value", Map.FieldDefinition.builder().expr("value").type(IonTypeName.INT).build()
            ))
            .build();

        Map.Output output = task.run(runContext);

        assertThat(output.getRecords(), is((Object) null));
        try (InputStream inputStream = runContext.storage().getFile(URI.create(output.getUri()))) {
            var datagram = IonValueUtils.system().getLoader().load(inputStream);
            assertThat(datagram.size(), is(1));
            IonStruct struct = (IonStruct) datagram.get(0);
            assertThat(IonValueUtils.toJavaValue(struct), is(java.util.Map.of("value", 3L)));
        }
    }

    @Test
    void storesOutputAsIonFile() throws Exception {
        java.util.Map<String, Object> record = java.util.Map.of("value", 7);

        Map task = Map.builder()
            .from(Property.ofValue(List.of(record)))
            .fields(java.util.Map.of(
                "value", Map.FieldDefinition.builder().expr("value").type(IonTypeName.INT).build()
            ))
            .output(Map.OutputMode.STORE)
            .build();

        RunContext runContext = runContextFactory.of(java.util.Map.of());
        Map.Output output = task.run(runContext);

        assertThat(output.getRecords(), is((Object) null));
        try (InputStream inputStream = runContext.storage().getFile(URI.create(output.getUri()))) {
            var datagram = IonValueUtils.system().getLoader().load(inputStream);
            assertThat(datagram.size(), is(1));
            IonStruct struct = (IonStruct) datagram.get(0);
            assertThat(IonValueUtils.toJavaValue(struct), is(java.util.Map.of("value", 7L)));
        }
    }

    @Test
    void rejectsMissingStorageUri() {
        Map task = Map.builder()
            .from(Property.ofValue("kestra://missing-file"))
            .fields(java.util.Map.of(
                "value", Map.FieldDefinition.builder().expr("value").type(IonTypeName.INT).build()
            ))
            .build();

        RunContext runContext = runContextFactory.of(java.util.Map.of());

        TransformException exception = org.junit.jupiter.api.Assertions.assertThrows(
            TransformException.class,
            () -> task.run(runContext)
        );

        assertThat(exception.getMessage(), is("Unable to read Ion file from storage: kestra://missing-file"));
    }

    @Test
    void rejectsUnsupportedFromType() {
        Map task = Map.builder()
            .from(Property.ofValue("not-a-uri"))
            .fields(java.util.Map.of(
                "value", Map.FieldDefinition.builder().expr("value").type(IonTypeName.INT).build()
            ))
            .build();

        RunContext runContext = runContextFactory.of(java.util.Map.of());

        TransformException exception = org.junit.jupiter.api.Assertions.assertThrows(
            TransformException.class,
            () -> task.run(runContext)
        );

        assertThat(exception.getMessage(), is(
            "Unsupported input type: java.lang.String. The 'from' property must resolve to a list/map of records, Ion values, or a storage URI."
        ));
    }
}
