package io.kestra.plugin.transform.arrow;

import com.fasterxml.jackson.core.type.TypeReference;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTaskException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.Await;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class QueryTest {
    @Inject
    private RunContextFactory runContextFactory;

    @ParameterizedTest
    @ValueSource(strings = {"orders.parquet", "orders.arrow", "orders.csv", "orders.jsonl"})
    void shouldQueryEachInputFormat(String fixture) throws Exception {
        var runContext = runContext();
        var task = Query.builder()
            .from(Property.ofValue(upload(runContext, fixture, fixture.substring(fixture.lastIndexOf('.'))).toString()))
            .expression(Property.ofValue("select(.status == \"paid\") | {name, amount}"))
            .build();

        var output = task.run(runContext);

        assertThat(output.getProcessedItemsTotal(), is(2L));
        assertThat(records(runContext, output.getUri()), contains(
            Map.of("name", "alice", "amount", 120.5),
            Map.of("name", "carol", "amount", 75.25)
        ));
    }

    @Test
    void shouldPassRowsThroughWithoutExpression() throws Exception {
        var runContext = runContext();
        var task = Query.builder()
            .from(Property.ofValue(upload(runContext, "orders.parquet", ".parquet").toString()))
            .build();

        var output = task.run(runContext);

        assertThat(output.getProcessedItemsTotal(), is(3L));
        assertThat(records(runContext, output.getUri()).getFirst(), is(Map.of("id", 1, "name", "alice", "amount", 120.5, "status", "paid")));
    }

    @Test
    void shouldAcceptTheIdentityExpression() throws Exception {
        var runContext = runContext();
        var task = Query.builder()
            .from(Property.ofValue(upload(runContext, "orders.csv", ".csv").toString()))
            .expression(Property.ofValue("."))
            .build();

        assertThat(task.run(runContext).getProcessedItemsTotal(), is(3L));
    }

    @Test
    void shouldAggregateAllRowsWithSlurp() throws Exception {
        var runContext = runContext();
        var task = Query.builder()
            .from(Property.ofValue(upload(runContext, "orders.csv", ".csv").toString()))
            .slurp(Property.ofValue(true))
            .expression(Property.ofValue("{orders: length, revenue: (map(.amount) | add)}"))
            .build();

        var output = task.run(runContext);

        assertThat(output.getProcessedItemsTotal(), is(1L));
        assertThat(records(runContext, output.getUri()), contains(Map.of("orders", 3, "revenue", 225.75)));
    }

    @Test
    void shouldNotInterpretTheExpressionInTheShell() throws Exception {
        var runContext = runContext();
        var literal = "$HOME `id` $(id) ; echo injected # it's";
        var task = Query.builder()
            .from(Property.ofValue(upload(runContext, "orders.csv", ".csv").toString()))
            .expression(Property.ofValue("select(.id == 1) | {text: \"" + literal.replace("\"", "\\\"") + "\"}"))
            .build();

        var output = task.run(runContext);

        assertThat(records(runContext, output.getUri()), contains(Map.of("text", literal)));
    }

    @Test
    void shouldNotRenderTheRenderedExpressionAgain() throws Exception {
        var literal = "{{ 1 + 1 }}";
        var runContext = TestsUtils.mockRunContext(
            runContextFactory,
            Query.builder().id(IdUtils.create()).type(Query.class.getName()).from(Property.ofValue("kestra:///orders.csv")).build(),
            Map.of("payload", "select(.id == 1) | {text: \"" + literal + "\"}")
        );
        var task = Query.builder()
            .from(Property.ofValue(upload(runContext, "orders.csv", ".csv").toString()))
            .expression(Property.ofExpression("{{ inputs.payload }}"))
            .build();

        var output = task.run(runContext);

        assertThat(records(runContext, output.getUri()), contains(Map.of("text", literal)));
    }

    @Test
    void shouldWriteCsv() throws Exception {
        var runContext = runContext();
        var task = Query.builder()
            .from(Property.ofValue(upload(runContext, "orders.parquet", ".parquet").toString()))
            .expression(Property.ofValue("{name, status}"))
            .outputFormat(Property.ofValue(Query.OutputFormat.CSV))
            .build();

        var output = task.run(runContext);

        assertThat(output.getUri().getPath(), endsWith(".csv"));
        assertThat(output.getProcessedItemsTotal(), nullValue());
        assertThat(text(runContext, output.getUri()), is("name,status\nalice,paid\nbob,pending\ncarol,paid\n"));
    }

    @Test
    void shouldWriteJsonAndNdjson() throws Exception {
        var runContext = runContext();
        var from = upload(runContext, "orders.parquet", ".parquet").toString();

        var json = Query.builder()
            .from(Property.ofValue(from))
            .expression(Property.ofValue(".id"))
            .outputFormat(Property.ofValue(Query.OutputFormat.JSON))
            .build()
            .run(runContext);
        var ndjson = Query.builder()
            .from(Property.ofValue(from))
            .expression(Property.ofValue(".id"))
            .outputFormat(Property.ofValue(Query.OutputFormat.NDJSON))
            .build()
            .run(runContext);

        assertThat(text(runContext, json.getUri()), is("[1,2,3]\n"));
        assertThat(text(runContext, ndjson.getUri()), is("1\n2\n3\n"));
    }

    @Test
    void shouldWriteArrow() throws Exception {
        var runContext = runContext();
        var arrow = Query.builder()
            .from(Property.ofValue(upload(runContext, "orders.csv", ".csv").toString()))
            .expression(Property.ofValue("{id, name}"))
            .outputFormat(Property.ofValue(Query.OutputFormat.ARROW))
            .build()
            .run(runContext);

        // the Arrow file can be queried again
        var output = Query.builder()
            .from(Property.ofValue(arrow.getUri().toString()))
            .expression(Property.ofValue(".name"))
            .build()
            .run(runContext);

        assertThat(arrow.getUri().getPath(), endsWith(".arrow"));
        assertThat(records(runContext, output.getUri()), contains("alice", "bob", "carol"));
    }

    @Test
    void shouldUseTheExplicitInputFormatAndDelimiter() throws Exception {
        var runContext = runContext();
        var task = Query.builder()
            .from(Property.ofValue(upload(runContext, "orders-semicolon.csv", ".dat").toString()))
            .inputFormat(Property.ofValue(Query.InputFormat.CSV))
            .delimiter(Property.ofValue(";"))
            .expression(Property.ofValue(".name"))
            .build();

        var output = task.run(runContext);

        assertThat(records(runContext, output.getUri()), contains("alice", "bob", "carol"));
    }

    @Test
    void shouldFailOnAnInvalidExpression() throws Exception {
        var runContext = runContext();
        var task = Query.builder()
            .from(Property.ofValue(upload(runContext, "orders.csv", ".csv").toString()))
            .expression(Property.ofValue("select("))
            .build();

        var exception = assertThrows(RunnableTaskException.class, () -> task.run(runContext));

        assertThat(exception.getMessage(), containsString("exit code 1"));
    }

    @Test
    void shouldFailWhenTheFormatCannotBeDetected() throws Exception {
        var runContext = runContext();
        var task = Query.builder()
            .from(Property.ofValue(upload(runContext, "orders.csv", ".dat").toString()))
            .build();

        var exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));

        assertThat(exception.getMessage(), containsString("inputFormat"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"id,name\n1,alice", "/etc/passwd", "kestra:///orders.csv?x={{ 1 + 1 }}"})
    void shouldFailWhenFromIsNotAnInternalStorageUri(String from) {
        var runContext = runContext();
        var task = Query.builder()
            .from(Property.ofValue(from))
            .inputFormat(Property.ofValue(Query.InputFormat.CSV))
            .build();

        var exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));

        assertThat(exception.getMessage(), containsString("internal storage URI"));
    }

    @Test
    void shouldKillTheRunningContainer() throws Exception {
        var runContext = runContext();
        var task = Query.builder()
            .from(Property.ofValue(upload(runContext, "orders.csv", ".csv").toString()))
            .expression(Property.ofValue("last(range(1e12))"))
            .build();
        var error = new AtomicReference<Throwable>();

        var thread = new Thread(() -> {
            try {
                task.run(runContext);
            } catch (Throwable e) {
                error.set(e);
            }
        });
        thread.start();
        // the container may not be started yet, so retry the kill until the run ends
        Await.until(() -> {
            task.kill();
            return !thread.isAlive();
        }, Duration.ofMillis(500), Duration.ofSeconds(60));

        assertThat(thread.isAlive(), is(false));
        assertThat(error.get(), notNullValue());
    }

    @Test
    void shouldUseTheImageOfTheDockerfile() throws Exception {
        var revision = Files.readAllLines(Path.of("../dockerfiles/aq.Dockerfile")).stream()
            .filter(line -> line.startsWith("ARG AQ_REVISION="))
            .map(line -> line.substring("ARG AQ_REVISION=".length(), "ARG AQ_REVISION=".length() + 7))
            .findFirst()
            .orElseThrow();

        assertThat(Query.DEFAULT_IMAGE, is("ghcr.io/kestra-io/aq:" + revision));
    }

    @Test
    void shouldFailOnAnInvalidDelimiter() throws Exception {
        var runContext = runContext();
        var task = Query.builder()
            .from(Property.ofValue(upload(runContext, "orders.csv", ".csv").toString()))
            .delimiter(Property.ofValue(";;"))
            .build();

        var exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));

        assertThat(exception.getMessage(), containsString("delimiter"));
    }

    private RunContext runContext() {
        return TestsUtils.mockRunContext(runContextFactory, Query.builder().id(IdUtils.create()).type(Query.class.getName()).from(Property.ofValue("kestra:///orders.csv")).build(), Map.of());
    }

    private static URI upload(RunContext runContext, String fixture, String extension) throws Exception {
        var file = runContext.workingDir().createTempFile(extension);
        try (InputStream in = Objects.requireNonNull(QueryTest.class.getClassLoader().getResourceAsStream("data/" + fixture))) {
            Files.copy(in, file, StandardCopyOption.REPLACE_EXISTING);
        }
        return runContext.storage().putFile(file.toFile());
    }

    private static List<Object> records(RunContext runContext, URI uri) throws Exception {
        try (var reader = new InputStreamReader(runContext.storage().getFile(uri), StandardCharsets.UTF_8)) {
            return FileSerde.readAll(reader, new TypeReference<Object>() {}).collectList().block();
        }
    }

    private static String text(RunContext runContext, URI uri) throws Exception {
        try (var in = runContext.storage().getFile(uri)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
