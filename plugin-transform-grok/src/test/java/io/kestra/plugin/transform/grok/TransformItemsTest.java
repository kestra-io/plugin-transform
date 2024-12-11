package io.kestra.plugin.transform.grok;

import com.fasterxml.jackson.core.type.TypeReference;
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
            .patterns(Property.of(List.of("%{INT}", "%{EMAILADDRESS}")))
            .namedCapturesOnly(Property.of(false))
            .from(Property.of(uri.toString()))
            .patternsDir(Property.of(List.of("./custom-patterns")))
            .breakOnFirstMatch(Property.of(false))
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
}