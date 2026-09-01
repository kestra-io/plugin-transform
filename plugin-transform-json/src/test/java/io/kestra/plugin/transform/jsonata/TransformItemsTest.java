package io.kestra.plugin.transform.jsonata;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Flux;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@KestraTest
class TransformItemsTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldGetOutputForValidExprReturningStringFromURI() throws Exception {
        // Given
        RunContext runContext = runContextFactory.of();
        final Path ouputFilePath = runContext.workingDir().createTempFile(".ion");
        try (final Writer writer = new OutputStreamWriter(Files.newOutputStream(ouputFilePath))) {
            FileSerde.writeAll(writer, Flux.just(
                new ObjectMapper().readValue(Features.DATASET_ACCOUNT_ORDER_JSON, Map.class),
                new ObjectMapper().readValue(Features.DATASET_ACCOUNT_ORDER_JSON, Map.class))).block();
            writer.flush();
        }
        URI uri = runContext.storage().putFile(ouputFilePath.toFile());

        TransformItems task = TransformItems.builder()
            .from(Property.ofValue(uri.toString()))
            .expression(Property.ofValue(Features.DATASET_ACCOUNT_ORDER_EXPR))
            .build();

        // When
        TransformItems.Output output = task.run(runContext);

        // Then
        Assertions.assertNotNull(output);
        Assertions.assertEquals(2, output.getProcessedItemsTotal());

        InputStream is = runContext.storage().getFile(output.getUri());
        String transformationResult = FileSerde.readAll(new InputStreamReader(is), new TypeReference<String>() {
        }).blockLast();

        Assertions.assertEquals(Features.DATASET_ACCOUNT_ORDER_EXPR_RESULT, transformationResult);
    }

    @Test
    void shouldGetMultipleRecordsForValidExprReturningArrayGivenExplodeTrue() throws Exception {
        // Given
        RunContext runContext = runContextFactory.of();
        final Path ouputFilePath = runContext.workingDir().createTempFile(".ion");
        try (final Writer writer = new OutputStreamWriter(Files.newOutputStream(ouputFilePath))) {
            FileSerde.writeAll(writer, Flux.just(new ObjectMapper().readValue(Features.DATASET_ACCOUNT_ORDER_JSON, Map.class))).block();
            writer.flush();
        }
        URI uri = runContext.storage().putFile(ouputFilePath.toFile());

        TransformItems task = TransformItems.builder()
            .from(Property.ofValue(uri.toString()))
            .expression(Property.ofValue("Account.Order.Product"))
            .explodeArray(Property.ofValue(true))
            .build();

        // When
        TransformItems.Output output = task.run(runContext);

        // Then
        Assertions.assertNotNull(output);
        Assertions.assertEquals(2, output.getProcessedItemsTotal());

        InputStream is = runContext.storage().getFile(output.getUri());
        List<Map> transformationResult = FileSerde.readAll(new InputStreamReader(is), new TypeReference<Map>() {
        }).collectList().block();

        Assertions.assertEquals(2, transformationResult.size());
    }

    @Test
    void shouldGetSingleRecordForValidExprReturningArrayGivenExplodeFalse() throws Exception {
        // Given
        RunContext runContext = runContextFactory.of();
        final Path ouputFilePath = runContext.workingDir().createTempFile(".ion");
        try (final Writer writer = new OutputStreamWriter(Files.newOutputStream(ouputFilePath))) {
            FileSerde.writeAll(writer, Flux.just(new ObjectMapper().readValue(Features.DATASET_ACCOUNT_ORDER_JSON, Map.class))).block();
            writer.flush();
        }
        URI uri = runContext.storage().putFile(ouputFilePath.toFile());

        TransformItems task = TransformItems.builder()
            .from(Property.ofValue(uri.toString()))
            .expression(Property.ofValue("Account.Order.Product"))
            .explodeArray(Property.ofValue(false))
            .build();

        // When
        TransformItems.Output output = task.run(runContext);

        // Then
        Assertions.assertNotNull(output);
        Assertions.assertEquals(1, output.getProcessedItemsTotal());

        InputStream is = runContext.storage().getFile(output.getUri());
        List<List> transformationResult = FileSerde.readAll(new InputStreamReader(is), new TypeReference<List>() {
        }).collectList().block();

        Assertions.assertEquals(1, transformationResult.size());
        Assertions.assertEquals(2, transformationResult.getFirst().size());
    }

    @Test
    void shouldReuseEvalThreadAcrossRecords() throws Exception {
        // Verifies executor reuse: after run() completes, awaitTermination in shutdownEvalExecutor()
        // guarantees the jsonata-eval thread is gone. If the old per-call new Thread() approach were
        // used, 3 threads would be started and could still be alive briefly, making liveAfter > 0
        // probabilistically — so this assertion is a reliable regression guard.
        RunContext runContext = runContextFactory.of();
        final Path outputFilePath = runContext.workingDir().createTempFile(".ion");
        try (final Writer writer = new OutputStreamWriter(Files.newOutputStream(outputFilePath))) {
            FileSerde.writeAll(writer, Flux.just(
                Map.of("v", 1),
                Map.of("v", 2),
                Map.of("v", 3)
            )).block();
            writer.flush();
        }
        URI uri = runContext.storage().putFile(outputFilePath.toFile());

        TransformItems task = TransformItems.builder()
            .from(Property.ofValue(uri.toString()))
            .expression(Property.ofValue("$"))
            .build();

        task.run(runContext);

        long liveAfter = Thread.getAllStackTraces().keySet().stream()
            .filter(t -> "jsonata-eval".equals(t.getName()))
            .count();

        Assertions.assertEquals(0, liveAfter, "jsonata-eval thread should be terminated after run()");
    }

    @Test
    void shouldHandleLargeDatasetWithFlatFieldLookupOnConstrainedStack() throws Exception {
        // Regression: TransformItems crashed the Windows worker with StackOverflowError when processing
        // a large LDAP-like dataset. The crash was in Jsonata$Frame.lookup() scope-chain recursion —
        // unrelated to user-defined function depth, so lowering maxDepth had no effect.
        // The fix is the 4 MB eval thread. This test JVM runs at -Xss512k (build.gradle) to simulate
        // the constrained Windows stack.
        RunContext runContext = runContextFactory.of();
        final Path outputFilePath = runContext.workingDir().createTempFile(".ion");

        int recordCount = 5_000;
        List<Map<String, Object>> records = new ArrayList<>(recordCount);
        for (int i = 0; i < recordCount; i++) {
            Map<String, Object> attributes = new HashMap<>();
            attributes.put("mail", List.of("user" + i + "@example.com"));
            attributes.put("cn", List.of("User " + i));
            attributes.put("displayName", List.of("Display User " + i));
            attributes.put("givenName", List.of("First" + i));
            attributes.put("sn", List.of("Last" + i));
            attributes.put("uid", List.of("uid" + i));
            attributes.put("employeenumber", List.of("EMP" + i));
            attributes.put("tCID", List.of("CID" + i));
            attributes.put("tWrID", List.of("WR" + i));
            attributes.put("tMainWrID", List.of("MWR" + i));
            attributes.put("tisActive", List.of("TRUE"));
            attributes.put("tStatusOfEmployment", List.of("active"));
            attributes.put("preferredLanguage", List.of("en"));
            // Multi-value field — mirrors the isMemberOf array the customer used $join() on
            attributes.put("isMemberOf", List.of("cn=group1,ou=groups", "cn=group2,ou=groups", "cn=group3,ou=groups"));
            records.add(Map.of("dn", "uid=user" + i + ",ou=Account,o=DTAG", "attributes", attributes));
        }

        try (Writer writer = new OutputStreamWriter(Files.newOutputStream(outputFilePath))) {
            FileSerde.writeAll(writer, Flux.fromIterable(records)).block();
            writer.flush();
        }
        URI uri = runContext.storage().putFile(outputFilePath.toFile());

        TransformItems task = TransformItems.builder()
            .from(Property.ofValue(uri.toString()))
            .expression(Property.ofValue("""
                {
                  "DN": dn ? $string(dn) : null,
                  "MAIL": attributes.mail[0] ? $string(attributes.mail[0]) : null,
                  "CN": attributes.cn[0] ? $string(attributes.cn[0]) : null,
                  "DISPLAY_NAME": attributes.displayName[0] ? $string(attributes.displayName[0]) : null,
                  "GIVEN_NAME": attributes.givenName[0] ? $string(attributes.givenName[0]) : null,
                  "SN": attributes.sn[0] ? $string(attributes.sn[0]) : null,
                  "UID": attributes.uid[0] ? $string(attributes.uid[0]) : null,
                  "EMPLOYEENUMBER": attributes.employeenumber[0] ? $string(attributes.employeenumber[0]) : null,
                  "TCID": attributes.tCID[0] ? $string(attributes.tCID[0]) : null,
                  "TWRID": attributes.tWrID[$ != attributes.tMainWrID[0]][0] ? $string(attributes.tWrID[$ != attributes.tMainWrID[0]][0]) : (attributes.tWrID[0] ? $string(attributes.tWrID[0]) : null),
                  "TMAINWRID": attributes.tMainWrID[0] ? $string(attributes.tMainWrID[0]) : null,
                  "TIS_ACTIVE": attributes.tisActive[0] ? $string(attributes.tisActive[0]) : null,
                  "TSTATUS_OF_EMPLOYMENT": attributes.tStatusOfEmployment[0] ? $string(attributes.tStatusOfEmployment[0]) : null,
                  "PREFERREDLANGUAGE": attributes.preferredLanguage[0] ? $string(attributes.preferredLanguage[0]) : null,
                  "ISMEMBEROF": attributes.isMemberOf ? $join(attributes.isMemberOf, "|") : null
                }
                """))
            .build();

        TransformItems.Output output = task.run(runContext);

        Assertions.assertEquals(recordCount, output.getProcessedItemsTotal());
    }

    @ParameterizedTest
    @ValueSource(ints = {100, 5_000})
    void shouldCollapseBatchIntoOneRecordOnDefaultMaxDepth(int itemCount) throws Exception {
        // Regression: kestra-io/plugin-transform#102. One record holding a batch of N items, folded into a
        // single object — the reported shape. Frame.setRuntimeBounds leaked depth per item, so the default
        // maxDepth=1000 failed above roughly 300 items here with "Depth=1001 max=1000" while passing at 100.
        // The nested object constructor is what leaked; the projection form keeps the test linear in time.
        RunContext runContext = runContextFactory.of();
        final Path outputFilePath = runContext.workingDir().createTempFile(".ion");

        List<Map<String, Object>> batch = new ArrayList<>(itemCount);
        for (int i = 0; i < itemCount; i++) {
            batch.add(Map.of("eventId", "e" + i, "value", i, "currency", "USD"));
        }

        try (Writer writer = new OutputStreamWriter(Files.newOutputStream(outputFilePath))) {
            FileSerde.writeAll(writer, Flux.just(batch)).block();
            writer.flush();
        }
        URI uri = runContext.storage().putFile(outputFilePath.toFile());

        TransformItems task = TransformItems.builder()
            .from(Property.ofValue(uri.toString()))
            .explodeArray(Property.ofValue(false))
            .expression(Property.ofValue("""
                {
                  "items": [ $.{
                    "eventId": eventId,
                    "deposit": [ { "value": value, "currency": currency } ]
                  } ]
                }
                """))
            .build();

        TransformItems.Output output = task.run(runContext);

        Assertions.assertEquals(1, output.getProcessedItemsTotal());

        InputStream is = runContext.storage().getFile(output.getUri());
        List<Map> transformationResult = FileSerde.readAll(new InputStreamReader(is), new TypeReference<Map>() {
        }).collectList().block();

        Assertions.assertEquals(1, transformationResult.size());
        Assertions.assertEquals(itemCount, ((List<?>) transformationResult.getFirst().get("items")).size());
    }

    @Test
    void shouldTransformJsonInputWithDefaultIonMapper() throws Exception {
        // Given
        RunContext runContext = runContextFactory.of();
        final Path outputFilePath = runContext.workingDir().createTempFile(".json");

        try (final Writer writer = new OutputStreamWriter(Files.newOutputStream(outputFilePath))) {
            FileSerde.writeAll(
                writer,
                Flux.just(Map.of("title", "ThinkPad", "brand", "Lenovo"))
            ).block();
            writer.flush();
        }

        URI uri = runContext.storage().putFile(outputFilePath.toFile());

        TransformItems task = TransformItems.builder()
            .from(Property.ofValue(uri.toString()))
            .expression(Property.ofValue("title & ' by ' & brand"))
            .build();

        // When
        TransformItems.Output output = task.run(runContext);

        // Then
        Assertions.assertNotNull(output);
        Assertions.assertEquals(1, output.getProcessedItemsTotal());

        InputStream is = runContext.storage().getFile(output.getUri());
        List<String> transformationResult = FileSerde.readAll(new InputStreamReader(is), new TypeReference<String>() {
        }).collectList().block();

        Assertions.assertEquals(1, transformationResult.size());
        Assertions.assertEquals("ThinkPad by Lenovo", transformationResult.get(0));
    }

    @Test
    void shouldNotWriteRecordWhenExpressionMatchesNothing() throws Exception {
        // Regression: kestra-io/plugin-transform#111. An expression matching nothing used to be written
        // out as a phantom `null` record (4-byte output file, processedItemsTotal=1).
        RunContext runContext = runContextFactory.of();
        URI uri = writeItems(runContext, Map.of("products", List.of()));

        TransformItems task = TransformItems.builder()
            .from(Property.ofValue(uri.toString()))
            .expression(Property.ofValue("products.{\"t\": title}"))
            .build();

        TransformItems.Output output = task.run(runContext);

        Assertions.assertEquals(0, output.getProcessedItemsTotal());
        Assertions.assertEquals(0, readRaw(runContext, output.getUri()).length());
    }

    @Test
    void shouldNotWriteRecordWhenExpressionReferencesUnknownRoot() throws Exception {
        // Regression: kestra-io/plugin-transform#111 — same phantom record for a non-existent root.
        RunContext runContext = runContextFactory.of();
        URI uri = writeItems(runContext, Map.of("products", List.of(Map.of("title", "ThinkPad"))));

        TransformItems task = TransformItems.builder()
            .from(Property.ofValue(uri.toString()))
            .expression(Property.ofValue("doesNotExist.title"))
            .build();

        TransformItems.Output output = task.run(runContext);

        Assertions.assertEquals(0, output.getProcessedItemsTotal());
        Assertions.assertEquals(0, readRaw(runContext, output.getUri()).length());
    }

    @Test
    void shouldOnlyWriteMatchingRecordsWhenSomeItemsMatchNothing() throws Exception {
        // Regression: kestra-io/plugin-transform#111 — non-matching items must be dropped, not turned
        // into null records that shift the downstream row count.
        RunContext runContext = runContextFactory.of();
        URI uri = writeItems(
            runContext,
            Map.of("title", "ThinkPad"),
            Map.of("other", "no title here"),
            Map.of("title", "MacBook")
        );

        TransformItems task = TransformItems.builder()
            .from(Property.ofValue(uri.toString()))
            .expression(Property.ofValue("title"))
            .build();

        TransformItems.Output output = task.run(runContext);

        Assertions.assertEquals(2, output.getProcessedItemsTotal());

        InputStream is = runContext.storage().getFile(output.getUri());
        List<String> transformationResult = FileSerde.readAll(new InputStreamReader(is), new TypeReference<String>() {
        }).collectList().block();

        Assertions.assertEquals(List.of("ThinkPad", "MacBook"), transformationResult);
    }

    @Test
    void shouldKeepRecordWhoseResultMerelyContainsNulls() throws Exception {
        // Guard for the kestra-io/plugin-transform#111 fix: only a no-match is dropped. A result that is
        // itself a value containing nulls is still a match and must be written.
        RunContext runContext = runContextFactory.of();
        URI uri = writeItems(runContext, Map.of("title", "ThinkPad"));

        TransformItems task = TransformItems.builder()
            .from(Property.ofValue(uri.toString()))
            .expression(Property.ofValue("[1, null, 2]"))
            .explodeArray(Property.ofValue(false))
            .build();

        TransformItems.Output output = task.run(runContext);

        Assertions.assertEquals(1, output.getProcessedItemsTotal());

        InputStream is = runContext.storage().getFile(output.getUri());
        List<List> transformationResult = FileSerde.readAll(new InputStreamReader(is), new TypeReference<List>() {
        }).collectList().block();

        Assertions.assertEquals(1, transformationResult.size());
        Assertions.assertEquals(Arrays.asList(1, null, 2), transformationResult.getFirst());
    }

    @SafeVarargs
    private URI writeItems(RunContext runContext, Map<String, Object>... items) throws Exception {
        final Path outputFilePath = runContext.workingDir().createTempFile(".ion");
        try (final Writer writer = new OutputStreamWriter(Files.newOutputStream(outputFilePath))) {
            FileSerde.writeAll(writer, Flux.fromArray(items)).block();
            writer.flush();
        }
        return runContext.storage().putFile(outputFilePath.toFile());
    }

    private String readRaw(RunContext runContext, URI uri) throws Exception {
        try (InputStream is = runContext.storage().getFile(uri)) {
            return new String(is.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        }
    }
}
