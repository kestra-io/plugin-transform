package io.kestra.plugin.transform.grok;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.transform.grok.pattern.GrokMatcher;
import io.kestra.plugin.transform.grok.pattern.GrokPatternCompiler;
import io.kestra.plugin.transform.grok.pattern.GrokPatternResolver;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class Transform extends Task {

    private Property<String> pattern;

    private Property<List<String>> patterns;

    private Property<List<String>> patternsDir;

    private Property<Map<String, String>> patternDefinitions;

    @Builder.Default
    private Property<Boolean> namedCapturesOnly = Property.ofValue(true);

    @Builder.Default
    private Property<Boolean> breakOnFirstMatch = Property.ofValue(true);

    @Builder.Default
    private Property<Boolean> keepEmptyCaptures = Property.ofValue(false);

    @Getter(AccessLevel.PRIVATE)
    private GrokPatternCompiler compiler;

    @Getter(AccessLevel.PRIVATE)
    private List<GrokMatcher> grokMatchers;

    public void init(final RunContext runContext) throws IllegalVariableEvaluationException {

        // create compiler
        this.compiler = new GrokPatternCompiler(
            new GrokPatternResolver(
                runContext.logger(),
                patternDefinitions(runContext),
                patternsDir(runContext)
            ),
            runContext.render(getNamedCapturesOnly()).as(Boolean.class).orElseThrow()
        );

        // compile all patterns
        this.grokMatchers = patterns(runContext).stream().map(compiler::compile).toList();
    }

    public Map<String, Object> matches(final byte[] bytes, RunContext runContext) throws IllegalVariableEvaluationException {
        // match patterns
        final List<Map<String, Object>> allNamedCaptured = new ArrayList<>(grokMatchers.size());
        for (GrokMatcher matcher : grokMatchers) {
            final Map<String, Object> captured = matcher.captures(bytes);
            if (captured != null) {
                allNamedCaptured.add(captured);
                if (runContext.render(getBreakOnFirstMatch()).as(Boolean.class).orElseThrow()) break;
            }
        }
        // merge all named captured
        Map<String, Object> mergedValues = new HashMap<>();
        for (Map<String, Object> namedCaptured : allNamedCaptured) {
            if (runContext.render(getKeepEmptyCaptures()).as(Boolean.class).orElseThrow()) {
                mergedValues.putAll(namedCaptured);
            } else {
                Map<String, Object> filtered = namedCaptured.entrySet()
                    .stream()
                    .filter(entry -> {
                        Object value = entry.getValue();
                        return value != null && (!(value instanceof String str) || !str.isEmpty());
                    })
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                mergedValues.putAll(filtered);
            }
        }
        return mergedValues;
    }

    private Map<String, String> patternDefinitions(RunContext runContext) throws IllegalVariableEvaluationException {
        return runContext.render(patternDefinitions).asMap(String.class, String.class);
    }

    private List<File> patternsDir(RunContext runContext) throws IllegalVariableEvaluationException {
        var renderedPatternsDir = runContext.render(this.patternsDir).asList(String.class);
        if (renderedPatternsDir.isEmpty()) return Collections.emptyList();

        return renderedPatternsDir
            .stream()
            .map(dir -> runContext.workingDir().resolve(Path.of(dir)))
            .map(Path::toFile)
            .collect(Collectors.toList());
    }

    private List<String> patterns(RunContext runContext) throws IllegalVariableEvaluationException {
        if (pattern != null) return List.of(runContext.render(pattern).as(String.class).orElseThrow());

        var patternsList = runContext.render(patterns).asList(String.class);
        if (patternsList.isEmpty()) {
            throw new IllegalArgumentException(
                "Missing required configuration, either `pattern` or `patterns` properties must not be empty.");
        }
        return patternsList;
    }
}
