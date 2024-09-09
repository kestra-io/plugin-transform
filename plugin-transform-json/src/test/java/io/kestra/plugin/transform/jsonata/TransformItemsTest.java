package io.kestra.plugin.transform.jsonata;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
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
            .from(uri.toString())
            .expression(Features.DATASET_ACCOUNT_ORDER_EXPR)
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
            .from(uri.toString())
            .expression("Account.Order.Product")
            .explodeArray(true)
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
            .from(uri.toString())
            .expression("Account.Order.Product")
            .explodeArray(false)
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
        Assertions.assertEquals(2, transformationResult.get(0).size());
    }
}