package io.kestra.plugin.transform.grok;

import com.fasterxml.jackson.core.type.TypeReference;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import jakarta.inject.Inject;
import org.hamcrest.Matchers.*;
import org.hamcrest.Matchers.*;
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class TransformItemsTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    public void shouldTransformGivenMultipleItemsAndMultiplePatterns() throws Exception {
        // Given
        RunContext runContext = runContextFactory.of();

        final Path ouputFilePath = runContext.workingDir().createTempFile(".ion");
        try (final Writer os = new OutputStreamWriter(Files.newOutputStream(ouputFilePath))) {
            FileSerde.writeAll(os, Flux.just("1 unittest@kestra.io", "2 admin@kestra.io", "3 no-reply@kestra.io")).block();
            os.flush();
        }
        URI uri = runContext.storage().putFile(ouputFilePath.toFile());

        TransformItems task = TransformItems.builder()
            .patterns(Property.ofValue(List.of("%{INT}", "%{EMAILADDRESS}")))
            .namedCapturesOnly(Property.ofValue(false))
            .from(Property.ofValue(uri.toString()))
            .patternsDir(Property.ofValue(List.of("./custom-patterns")))
            .breakOnFirstMatch(Property.ofValue(false))
            .build();

        // When
        TransformItems.Output output = task.run(runContext);

        // Then
        Assertions.assertNotNull(output);
        Assertions.assertEquals(3, output.getProcessedItemsTotal());

        InputStream is = runContext.storage().getFile(output.getUri());
        List<Map> items = FileSerde.readAll(new InputStreamReader(is), new TypeReference<Map>() {}).collectList().block();
        Assertions.assertEquals(3, items.size());

        Assertions.assertEquals(
            List.of(
                Map.of("INT", "1", "HOSTNAME", "kestra.io", "EMAILLOCALPART", "unittest", "EMAILADDRESS", "unittest@kestra.io"),
                Map.of("INT", "2", "HOSTNAME", "kestra.io", "EMAILLOCALPART", "admin", "EMAILADDRESS", "admin@kestra.io"),
                Map.of("INT", "3", "HOSTNAME", "kestra.io", "EMAILLOCALPART", "no-reply", "EMAILADDRESS", "no-reply@kestra.io")
           ), items);
    }

    @Test
    public void shouldIgnoreUnmatchedLines() throws Exception {
        var runContext = runContextFactory.of();

        var inputFile = runContext.workingDir().createTempFile(".ion");
        try (Writer writer = new OutputStreamWriter(Files.newOutputStream(inputFile))) {
            FileSerde.writeAll(writer, Flux.just(
                "2020-12-14T12:18:51.397Z INFO Hi from Kestra team!",
                "DEBUG:root:This message should go to the log file",
                "2020-12-14T12:18:51.397Z ERROR Something went wrong..",
                "random text should be ignored"
            )).block();
            writer.flush();
        }

        var uri = runContext.storage().putFile(inputFile.toFile());

        TransformItems task = TransformItems.builder()
            .pattern(Property.ofValue("%{TIMESTAMP_ISO8601:logdate} %{LOGLEVEL:loglevel} %{GREEDYDATA:message}"))
            .from(Property.ofValue(uri.toString()))
            .build();

        var output = task.run(runContext);

        assertThat(output, notNullValue());
        assertThat(output.getProcessedItemsTotal().intValue(), is(2));

        InputStream is = runContext.storage().getFile(output.getUri());
        List<Map<String, Object>> items = FileSerde
            .readAll(new InputStreamReader(is), new TypeReference<Map<String, Object>>() {})
            .collectList()
            .block();

        assertThat(items.size(), is(2));

        assertThat(items, contains(
            allOf(hasEntry("loglevel", "INFO"), hasEntry("message", "Hi from Kestra team!")),
            allOf(hasEntry("loglevel", "ERROR"), hasEntry("message", "Something went wrong.."))
        ));
    }
}