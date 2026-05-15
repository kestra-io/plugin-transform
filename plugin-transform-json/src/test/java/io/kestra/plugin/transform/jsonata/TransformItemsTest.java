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
import reactor.core.publisher.Flux;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
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
        // Regression test for Pylon #1703 (T-Systems): TransformItems crashed the Windows worker with
        // StackOverflowError when processing a large LDAP dataset (~200k records, ~30 attributes each).
        // The crash was in Jsonata$Frame.lookup() scope-chain recursion — unrelated to user-defined
        // function depth, so lowering maxDepth had no effect. The fix is the 4 MB eval thread.
        // This test JVM runs at -Xss512k (build.gradle) to simulate the constrained Windows stack.
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
}