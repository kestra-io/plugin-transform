package io.kestra.plugin.transform;

import com.amazon.ion.IonStruct;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.transform.ion.IonValueUtils;
import io.kestra.plugin.transform.util.OutputFormat;
import io.kestra.plugin.transform.util.TransformException;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

@KestraTest
class SelectTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void selectsFiltersAndProjectsWithPositionalReferences() throws Exception {
        List<Map<String, Object>> orders = List.of(
            Map.of("order_id", 1, "amount", 120),
            Map.of("order_id", 2, "amount", 80)
        );
        List<Map<String, Object>> customers = List.of(
            Map.of("name", "Anna"),
            Map.of("name", "Ben")
        );
        List<Map<String, Object>> scores = List.of(
            Map.of("score", new BigDecimal("0.91")),
            Map.of("score", new BigDecimal("0.72"))
        );

        Select task = Select.builder()
            .inputs(List.of(
                Property.ofValue(orders),
                Property.ofValue(customers),
                Property.ofValue(scores)
            ))
            .where(Property.ofValue("amount > 100 && $3.score > 0.8"))
            .fields(Map.of(
                "orderId", Select.FieldDefinition.from("order_id"),
                "customer", Select.FieldDefinition.from("$2.name"),
                "amount", Select.FieldDefinition.from("$1.amount"),
                "score", Select.FieldDefinition.from("$3.score")
            ))
            .build();

        RunContext runContext = runContextFactory.of(Map.of());
        Select.Output output = task.run(runContext);

        assertThat(output.getRecords(), hasSize(1));
        Map<String, Object> record = (Map<String, Object>) output.getRecords().getFirst();
        assertThat(record.get("orderId"), is(1L));
        assertThat(record.get("customer"), is("Anna"));
        assertThat(record.get("amount"), is(120L));
        assertThat(record.get("score"), is(new BigDecimal("0.91")));
    }

    @Test
    void defaultsToMergedRowWithoutLeakingPositionalFields() throws Exception {
        List<Map<String, Object>> left = List.of(Map.of("id", "a", "status", "pending"));
        List<Map<String, Object>> right = List.of(Map.of("status", "ok"));

        Select task = Select.builder()
            .inputs(List.of(Property.ofValue(left), Property.ofValue(right)))
            .build();

        RunContext runContext = runContextFactory.of(Map.of());
        Select.Output output = task.run(runContext);

        assertThat(output.getRecords(), hasSize(1));
        Map<String, Object> record = (Map<String, Object>) output.getRecords().getFirst();
        assertThat(record.get("id"), is("a"));
        assertThat(record.get("status"), is("ok"));
        assertThat(record, not(hasKey("$1")));
        assertThat(record, not(hasKey("$2")));
    }

    @Test
    void dropNullsAppliesWhenFieldsOmitted() throws Exception {
        java.util.Map<String, Object> leftRecord = new java.util.HashMap<>();
        leftRecord.put("a", 1);
        leftRecord.put("b", null);
        List<Map<String, Object>> left = List.of(leftRecord);
        List<Map<String, Object>> right = List.of(Map.of("c", 2));

        Select task = Select.builder()
            .inputs(List.of(Property.ofValue(left), Property.ofValue(right)))
            .dropNulls(true)
            .build();

        RunContext runContext = runContextFactory.of(Map.of());
        Select.Output output = task.run(runContext);

        Map<String, Object> record = (Map<String, Object>) output.getRecords().getFirst();
        assertThat(record.get("a"), is(1L));
        assertThat(record.get("c"), is(2L));
        assertThat(record, not(hasKey("b")));
    }

    @Test
    void keepInputFieldsAddsProjectionOnTop() throws Exception {
        List<Map<String, Object>> left = List.of(Map.of("a", 1));
        List<Map<String, Object>> right = List.of(Map.of("b", 2));

        Select task = Select.builder()
            .inputs(List.of(Property.ofValue(left), Property.ofValue(right)))
            .fields(Map.of("sum", Select.FieldDefinition.from("a + b")))
            .keepInputFields(List.of(1, 2))
            .build();

        RunContext runContext = runContextFactory.of(Map.of());
        Select.Output output = task.run(runContext);

        Map<String, Object> record = (Map<String, Object>) output.getRecords().getFirst();
        assertThat(record.get("a"), is(1L));
        assertThat(record.get("b"), is(2L));
        assertThat(record.get("sum"), is(new BigDecimal("3")));
    }

    @Test
    void keepInputFieldsOmittedDropsUnprojectedFields() throws Exception {
        List<Map<String, Object>> left = List.of(Map.of("a", 1, "extra", "x"));
        List<Map<String, Object>> right = List.of(Map.of("b", 2));

        Select task = Select.builder()
            .inputs(List.of(Property.ofValue(left), Property.ofValue(right)))
            .fields(Map.of("a", Select.FieldDefinition.from("a")))
            .build();

        RunContext runContext = runContextFactory.of(Map.of());
        Select.Output output = task.run(runContext);

        Map<String, Object> record = (Map<String, Object>) output.getRecords().getFirst();
        assertThat(record.get("a"), is(1L));
        assertThat(record, not(hasKey("b")));
        assertThat(record, not(hasKey("extra")));
    }

    @Test
    void dropNullsControlsNullProjectionFields() throws Exception {
        List<Map<String, Object>> left = List.of(Map.of("a", 1));
        List<Map<String, Object>> right = List.of(Map.of("b", 2));

        Select drop = Select.builder()
            .inputs(List.of(Property.ofValue(left), Property.ofValue(right)))
            .fields(Map.of("missing", Select.FieldDefinition.builder().expr("c").optional(true).build()))
            .dropNulls(true)
            .build();

        RunContext runContext = runContextFactory.of(Map.of());
        Select.Output dropped = drop.run(runContext);
        Map<String, Object> droppedRecord = (Map<String, Object>) dropped.getRecords().getFirst();
        assertThat(droppedRecord, not(hasKey("missing")));

        Select keep = Select.builder()
            .inputs(List.of(Property.ofValue(left), Property.ofValue(right)))
            .fields(Map.of("missing", Select.FieldDefinition.builder().expr("c").optional(true).build()))
            .dropNulls(false)
            .build();

        assertThat(keep.isDropNulls(), is(false));

        Select.Output kept = keep.run(runContext);
        Map<String, Object> keptRecord = (Map<String, Object>) kept.getRecords().getFirst();
        assertThat(keptRecord, hasKey("missing"));
        assertThat(keptRecord.get("missing"), is((Object) null));
    }

    @Test
    void lengthMismatchSkipStopsAtShortestInput() throws Exception {
        List<Map<String, Object>> left = List.of(Map.of("id", "a"));
        List<Map<String, Object>> right = List.of(Map.of("value", 1), Map.of("value", 2));

        Select task = Select.builder()
            .inputs(List.of(Property.ofValue(left), Property.ofValue(right)))
            .onLengthMismatch(Select.OnLengthMismatchMode.SKIP)
            .build();

        RunContext runContext = runContextFactory.of(Map.of());
        Select.Output output = task.run(runContext);

        assertThat(output.getRecords(), hasSize(1));
    }

    @Test
    void lengthMismatchFailRejectsDifferentLengths() {
        List<Map<String, Object>> left = List.of(Map.of("id", "a"));
        List<Map<String, Object>> right = List.of(Map.of("value", 1), Map.of("value", 2));

        Select task = Select.builder()
            .inputs(List.of(Property.ofValue(left), Property.ofValue(right)))
            .onLengthMismatch(Select.OnLengthMismatchMode.FAIL)
            .build();

        RunContext runContext = runContextFactory.of(Map.of());
        TransformException exception = org.junit.jupiter.api.Assertions.assertThrows(
            TransformException.class,
            () -> task.run(runContext)
        );

        assertThat(exception.getMessage(), is("inputs must have same length"));
    }

    @Test
    void rejectsMissingInputs() {
        Select task = Select.builder().build();

        RunContext runContext = runContextFactory.of(Map.of());
        TransformException exception = org.junit.jupiter.api.Assertions.assertThrows(
            TransformException.class,
            () -> task.run(runContext)
        );

        assertThat(exception.getMessage(), is("inputs is required"));
    }

    @Test
    void rejectsNullFieldDefinition() {
        List<Map<String, Object>> left = List.of(Map.of("a", 1));

        java.util.Map<String, Select.FieldDefinition> fields = new java.util.HashMap<>();
        fields.put("a", null);
        Select task = Select.builder()
            .inputs(List.of(Property.ofValue(left)))
            .fields(fields)
            .build();

        RunContext runContext = runContextFactory.of(Map.of());
        TransformException exception = org.junit.jupiter.api.Assertions.assertThrows(
            TransformException.class,
            () -> task.run(runContext)
        );

        assertThat(exception.getMessage(), is("Field definition is required for 'a'"));
    }

    @Test
    void rejectsBlankExprInFieldDefinition() {
        List<Map<String, Object>> left = List.of(Map.of("a", 1));

        Select task = Select.builder()
            .inputs(List.of(Property.ofValue(left)))
            .fields(Map.of("a", Select.FieldDefinition.builder().expr("  ").build()))
            .build();

        RunContext runContext = runContextFactory.of(Map.of());
        TransformException exception = org.junit.jupiter.api.Assertions.assertThrows(
            TransformException.class,
            () -> task.run(runContext)
        );

        assertThat(exception.getMessage(), is("expr is required for 'a'"));
    }

    @Test
    void onErrorKeepEmitsMergedRowWhenProjectionErrors() throws Exception {
        List<Map<String, Object>> left = List.of(Map.of("id", "a", "value", 10));
        List<Map<String, Object>> right = List.of(Map.of("status", "ok"));

        Select task = Select.builder()
            .inputs(List.of(Property.ofValue(left), Property.ofValue(right)))
            .fields(Map.of("bad", Select.FieldDefinition.from("unknownFunction()")))
            .onError(Select.OnErrorMode.KEEP)
            .build();

        RunContext runContext = runContextFactory.of(Map.of());
        Select.Output output = task.run(runContext);

        assertThat(output.getRecords(), hasSize(1));
        Map<String, Object> record = (Map<String, Object>) output.getRecords().getFirst();
        assertThat(record.get("id"), is("a"));
        assertThat(record.get("status"), is("ok"));
        assertThat(record, not(hasKey("bad")));
    }

    @Test
    void onErrorSkipDropsRowWhenWhereErrors() throws Exception {
        List<Map<String, Object>> left = List.of(Map.of("value", 10));
        List<Map<String, Object>> right = List.of(Map.of("status", "ok"));

        Select task = Select.builder()
            .inputs(List.of(Property.ofValue(left), Property.ofValue(right)))
            .where(Property.ofValue("unknownFunction()"))
            .onError(Select.OnErrorMode.SKIP)
            .build();

        RunContext runContext = runContextFactory.of(Map.of());
        Select.Output output = task.run(runContext);

        assertThat(output.getRecords(), hasSize(0));
    }

    @Test
    void outputModeUriIsAliasForStore() {
        assertThat(Select.OutputMode.from("URI"), is(Select.OutputMode.STORE));
    }

    @Test
    void autoStoresWhenAnyInputIsStorageUri() throws Exception {
        String ordersIon = "[{order_id: 1, amount: 120}, {order_id: 2, amount: 80}]";
        String scoresIon = "[{score: 0.91}, {score: 0.72}]";

        RunContext runContext = runContextFactory.of(Map.of());
        URI ordersUri = runContext.storage().putFile(
            new ByteArrayInputStream(ordersIon.getBytes(StandardCharsets.UTF_8)),
            "select-orders.ion"
        );
        URI scoresUri = runContext.storage().putFile(
            new ByteArrayInputStream(scoresIon.getBytes(StandardCharsets.UTF_8)),
            "select-scores.ion"
        );

        Select task = Select.builder()
            .inputs(List.of(
                Property.ofValue(ordersUri.toString()),
                Property.ofValue(scoresUri.toString())
            ))
            .where(Property.ofValue("$2.score > 0.8"))
            .fields(Map.of(
                "order_id", Select.FieldDefinition.from("$1.order_id"),
                "score", Select.FieldDefinition.from("$2.score")
            ))
            .build();

        Select.Output output = task.run(runContext);
        assertThat(output.getRecords(), is((Object) null));

        try (InputStream inputStream = runContext.storage().getFile(URI.create(output.getUri()))) {
            var datagram = IonValueUtils.system().getLoader().load(inputStream);
            assertThat(datagram.size(), is(1));
            IonStruct struct = (IonStruct) datagram.get(0);
            assertThat(IonValueUtils.toJavaValue(struct), is(Map.of(
                "order_id", 1L,
                "score", new BigDecimal("0.91")
            )));
        }
    }

    @Test
    void storesOutputAsIonFileWithInMemoryInputs() throws Exception {
        List<Map<String, Object>> left = List.of(Map.of("a", 1, "b", 2));
        List<Map<String, Object>> right = List.of(Map.of("c", 3));

        Select task = Select.builder()
            .inputs(List.of(Property.ofValue(left), Property.ofValue(right)))
            .fields(Map.of(
                "a", Select.FieldDefinition.from("a"),
                "c", Select.FieldDefinition.from("c")
            ))
            .output(Select.OutputMode.STORE)
            .outputFormat(OutputFormat.TEXT)
            .build();

        RunContext runContext = runContextFactory.of(Map.of());
        Select.Output output = task.run(runContext);

        assertThat(output.getRecords(), is((Object) null));
        try (InputStream inputStream = runContext.storage().getFile(URI.create(output.getUri()))) {
            var datagram = IonValueUtils.system().getLoader().load(inputStream);
            assertThat(datagram.size(), is(1));
            IonStruct struct = (IonStruct) datagram.get(0);
            assertThat(IonValueUtils.toJavaValue(struct), is(Map.of(
                "a", 1L,
                "c", 3L
            )));
        }
    }

    @Test
    void supportsBinaryOutputFormat() throws Exception {
        List<Map<String, Object>> left = List.of(Map.of("a", 1));
        List<Map<String, Object>> right = List.of(Map.of("b", 2));

        Select task = Select.builder()
            .inputs(List.of(Property.ofValue(left), Property.ofValue(right)))
            .output(Select.OutputMode.STORE)
            .outputFormat(OutputFormat.BINARY)
            .build();

        RunContext runContext = runContextFactory.of(Map.of());
        Select.Output output = task.run(runContext);

        try (InputStream inputStream = runContext.storage().getFile(URI.create(output.getUri()))) {
            var datagram = IonValueUtils.system().getLoader().load(inputStream);
            assertThat(datagram.size(), is(1));
            IonStruct struct = (IonStruct) datagram.get(0);
            assertThat(IonValueUtils.toJavaValue(struct), is(Map.of(
                "a", 1L,
                "b", 2L
            )));
        }
    }

    @Test
    void recordsOutputDoesNotStoreEvenWithStorageInputs() throws Exception {
        String leftIon = "[{id: \"a\"}, {id: \"b\"}]";
        String rightIon = "[{status: \"ok\"}, {status: \"fail\"}]";

        RunContext runContext = runContextFactory.of(Map.of());
        URI leftUri = runContext.storage().putFile(
            new ByteArrayInputStream(leftIon.getBytes(StandardCharsets.UTF_8)),
            "select-left.ion"
        );
        URI rightUri = runContext.storage().putFile(
            new ByteArrayInputStream(rightIon.getBytes(StandardCharsets.UTF_8)),
            "select-right.ion"
        );

        Select task = Select.builder()
            .inputs(List.of(
                Property.ofValue(leftUri.toString()),
                Property.ofValue(rightUri.toString())
            ))
            .output(Select.OutputMode.RECORDS)
            .build();

        Select.Output output = task.run(runContext);
        assertThat(output.getUri(), is((Object) null));
        assertThat(output.getRecords(), hasSize(2));
    }

    @Test
    void castsProjectedFields() throws Exception {
        List<Map<String, Object>> left = List.of(Map.of(
            "customer_id", "c1",
            "total_spent", "12.50"
        ));
        List<Map<String, Object>> right = List.of(Map.of("active", true));

        Select task = Select.builder()
            .inputs(List.of(Property.ofValue(left), Property.ofValue(right)))
            .fields(Map.of(
                "customer_id", Select.FieldDefinition.builder().expr("customer_id").type(io.kestra.plugin.transform.ion.IonTypeName.STRING).build(),
                "total_spent", Select.FieldDefinition.builder().expr("total_spent").type(io.kestra.plugin.transform.ion.IonTypeName.DECIMAL).build()
            ))
            .output(Select.OutputMode.RECORDS)
            .build();

        RunContext runContext = runContextFactory.of(Map.of());
        Select.Output output = task.run(runContext);

        Map<String, Object> record = (Map<String, Object>) output.getRecords().getFirst();
        assertThat(record.get("customer_id"), is("c1"));
        assertThat(record.get("total_spent"), is(new BigDecimal("12.50")));
    }

    @Test
    void onErrorSkipDropsRowWhenCastFails() throws Exception {
        List<Map<String, Object>> left = List.of(Map.of("total_spent", "not-a-number"));

        Select task = Select.builder()
            .inputs(List.of(Property.ofValue(left)))
            .fields(Map.of(
                "total_spent", Select.FieldDefinition.builder().expr("total_spent").type(io.kestra.plugin.transform.ion.IonTypeName.DECIMAL).build()
            ))
            .onError(Select.OnErrorMode.SKIP)
            .output(Select.OutputMode.RECORDS)
            .build();

        RunContext runContext = runContextFactory.of(Map.of());
        Select.Output output = task.run(runContext);
        assertThat(output.getRecords(), hasSize(0));
    }

    @Test
    void onErrorKeepEmitsMergedRowWhenCastFails() throws Exception {
        List<Map<String, Object>> left = List.of(Map.of("total_spent", "not-a-number"));

        Select task = Select.builder()
            .inputs(List.of(Property.ofValue(left)))
            .fields(Map.of(
                "total_spent", Select.FieldDefinition.builder().expr("total_spent").type(io.kestra.plugin.transform.ion.IonTypeName.DECIMAL).build()
            ))
            .onError(Select.OnErrorMode.KEEP)
            .output(Select.OutputMode.RECORDS)
            .build();

        RunContext runContext = runContextFactory.of(Map.of());
        Select.Output output = task.run(runContext);
        assertThat(output.getRecords(), hasSize(1));
        Map<String, Object> record = (Map<String, Object>) output.getRecords().getFirst();
        assertThat(record.get("total_spent"), is("not-a-number"));
    }

    @Test
    void rejectsMissingRequiredFieldWhenOptionalFalse() {
        List<Map<String, Object>> left = List.of(Map.of("customer_id", "c1"));
        List<Map<String, Object>> right = List.of(Map.of("active", true));

        Select task = Select.builder()
            .inputs(List.of(Property.ofValue(left), Property.ofValue(right)))
            .fields(Map.of(
                "total_spent", Select.FieldDefinition.builder().expr("total_spent").optional(false).build()
            ))
            .build();

        RunContext runContext = runContextFactory.of(Map.of());
        TransformException exception = org.junit.jupiter.api.Assertions.assertThrows(
            TransformException.class,
            () -> task.run(runContext)
        );

        assertThat(exception.getMessage(), is("Missing required field: total_spent"));
    }

    @Test
    void lengthMismatchSkipWorksForStorageStreamingPath() throws Exception {
        String leftIon = "[{id: \"a\"}]";
        String rightIon = "[{status: \"ok\"}, {status: \"fail\"}]";

        RunContext runContext = runContextFactory.of(Map.of());
        URI leftUri = runContext.storage().putFile(
            new ByteArrayInputStream(leftIon.getBytes(StandardCharsets.UTF_8)),
            "select-stream-left.ion"
        );
        URI rightUri = runContext.storage().putFile(
            new ByteArrayInputStream(rightIon.getBytes(StandardCharsets.UTF_8)),
            "select-stream-right.ion"
        );

        Select task = Select.builder()
            .inputs(List.of(
                Property.ofValue(leftUri.toString()),
                Property.ofValue(rightUri.toString())
            ))
            .onLengthMismatch(Select.OnLengthMismatchMode.SKIP)
            .output(Select.OutputMode.STORE)
            .build();

        Select.Output output = task.run(runContext);
        try (InputStream inputStream = runContext.storage().getFile(URI.create(output.getUri()))) {
            var datagram = IonValueUtils.system().getLoader().load(inputStream);
            assertThat(datagram.size(), is(1));
        }
    }
}
