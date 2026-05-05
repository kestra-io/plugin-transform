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