package io.kestra.plugin.transform.grok;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

@KestraTest
class TransformValueTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    public void shouldTransformGivenPatternFromDir() throws Exception {
        // Given
        RunContext runContext = runContextFactory.of();

        String customPattern = """
            EMAILLOCALPART [a-zA-Z][a-zA-Z0-9_.+-=:]+
            EMAIL %{EMAILLOCALPART}@%{HOSTNAME}
            """;

        runContext.workingDir()
            .putFile(Path.of("custom-patterns/email"), new ByteArrayInputStream(customPattern.getBytes(StandardCharsets.UTF_8)));

        TransformValue task = TransformValue.builder()
            .pattern(Property.ofValue("%{EMAIL}"))
            .namedCapturesOnly(Property.ofValue(false))
            .from(Property.ofValue("unit-test@kestra.io"))
            .patternsDir(Property.ofValue(List.of("./custom-patterns")))
            .build();

        // When
        TransformValue.Output output = task.run(runContext);

        // Then
        Assertions.assertEquals(
            Map.of("HOSTNAME", "kestra.io", "EMAILLOCALPART", "unit-test", "EMAIL", "unit-test@kestra.io"),
            output.getValue()
        );
    }

    @Test
    public void shouldTransformGivenSinglePattern() throws Exception {
        // Given
        RunContext runContext = runContextFactory.of();
        TransformValue task = TransformValue.builder()
            .patterns(Property.ofValue(List.of("%{EMAILADDRESS}")))
            .namedCapturesOnly(Property.ofValue(false))
            .from(Property.ofValue("unit-test@kestra.io"))
            .build();

        // When
        TransformValue.Output output = task.run(runContext);

        // Then
        Assertions.assertEquals(
            Map.of("HOSTNAME", "kestra.io", "EMAILLOCALPART", "unit-test", "EMAILADDRESS", "unit-test@kestra.io"),
            output.getValue()
        );
    }

    @Test
    public void shouldTransformGivenSinglePatternAndCapturesOnlyTrue() throws Exception {
        // Given
        RunContext runContext = runContextFactory.of();
        TransformValue task = TransformValue.builder()
            .patterns(Property.ofValue(List.of("%{EMAILADDRESS:email}")))
            .namedCapturesOnly(Property.ofValue(true))
            .from(Property.ofValue("unit-test@kestra.io"))
            .build();

        // When
        TransformValue.Output output = task.run(runContext);

        // Then
        Assertions.assertEquals(
            Map.of("email", "unit-test@kestra.io"),
            output.getValue()
        );
    }

    @Test
    public void shouldTransformGivenConfigWithMultiplePatternsAndBreakFalse() throws Exception {
        // Given
        RunContext runContext = runContextFactory.of();
        TransformValue task = TransformValue.builder()
            .patterns(Property.ofValue(List.of("%{NUMBER}", "%{EMAILADDRESS}")))
            .namedCapturesOnly(Property.ofValue(false))
            .breakOnFirstMatch(Property.ofValue(false))
            .from(Property.ofValue("42 unit-test@kestra.io"))
            .build();

        // When
        TransformValue.Output output = task.run(runContext);

        // Then
        Assertions.assertEquals(
            Map.of("NUMBER", "42", "BASE10NUM", "42", "HOSTNAME", "kestra.io", "EMAILLOCALPART", "unit-test", "EMAILADDRESS", "unit-test@kestra.io"),
            output.getValue()
        );
    }

    @Test
    public void shouldTransformGivenConfigWithMultiplePatternsAndBreakTrue() throws Exception {
        // Given
        RunContext runContext = runContextFactory.of();
        TransformValue task = TransformValue.builder()
            .patterns(Property.ofValue(List.of("%{NUMBER}", "%{EMAILADDRESS}")))
            .namedCapturesOnly(Property.ofValue(false))
            .breakOnFirstMatch(Property.ofValue(true))
            .from(Property.ofValue("unit-test@kestra.io"))
            .build();

        // When
        TransformValue.Output output = task.run(runContext);

        // Then
        Assertions.assertEquals(
            Map.of("HOSTNAME", "kestra.io", "EMAILLOCALPART", "unit-test", "EMAILADDRESS", "unit-test@kestra.io"),
            output.getValue()
        );
    }

    @Test
    public void shouldTransformGivenKeepEmptyCapturesTrue() throws Exception {
        // Given
        RunContext runContext = runContextFactory.of();
        TransformValue task = TransformValue.builder()
            .patterns(Property.ofValue(List.of("%{IP:client_ip}(?:\\s+%{WORD:method})? %{NOTSPACE:url}")))
            .namedCapturesOnly(Property.ofValue(true))
            .breakOnFirstMatch(Property.ofValue(true))
            .keepEmptyCaptures(Property.ofValue(true))
            .from(Property.ofValue("192.168.1.1 /index.html"))
            .build();

        // When
        TransformValue.Output output = task.run(runContext);

        // Then
        Assertions.assertEquals(
            Map.of("method", "", "client_ip", "192.168.1.1", "url", "/index.html"),
            output.getValue()
        );
    }

    @Test
    public void shouldTransformGivenKeepEmptyCapturesFalse() throws Exception {
        // Given
        RunContext runContext = runContextFactory.of();
        TransformValue task = TransformValue.builder()
            .patterns(Property.ofValue(List.of("%{IP:client_ip}(?:\\s+%{WORD:method})? %{NOTSPACE:url}")))
            .namedCapturesOnly(Property.ofValue(true))
            .breakOnFirstMatch(Property.ofValue(true))
            .keepEmptyCaptures(Property.ofValue(false))
            .from(Property.ofValue("192.168.1.1 /index.html"))
            .build();

        // When
        TransformValue.Output output = task.run(runContext);

        // Then
        Assertions.assertEquals(
            Map.of("client_ip", "192.168.1.1", "url", "/index.html"),
            output.getValue()
        );
    }
}