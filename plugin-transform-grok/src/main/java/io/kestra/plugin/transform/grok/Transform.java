package io.kestra.plugin.transform.grok;

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

    private String pattern;

    private List<String> patterns;

    private List<String> patternsDir;

    private Map<String, String> patternDefinitions;

    @Builder.Default
    private boolean namedCapturesOnly = true;

    @Builder.Default
    private boolean breakOnFirstMatch = true;

    @Builder.Default
    private boolean keepEmptyCaptures = false;

    @Getter(AccessLevel.PRIVATE)
    private GrokPatternCompiler compiler;

    @Getter(AccessLevel.PRIVATE)
    private List<GrokMatcher> grokMatchers;

    public void init(final RunContext runContext) {

        // create compiler
        this.compiler = new GrokPatternCompiler(
            new GrokPatternResolver(
                runContext.logger(),
                patternDefinitions(),
                patternsDir(runContext)
            ),
            isNamedCapturesOnly()
        );

        // compile all patterns
        this.grokMatchers = patterns().stream().map(compiler::compile).toList();
    }

    public Map<String, Object> matches(final byte[] bytes) {
        // match patterns
        final List<Map<String, Object>> allNamedCaptured = new ArrayList<>(grokMatchers.size());
        for (GrokMatcher matcher : grokMatchers) {
            final Map<String, Object> captured = matcher.captures(bytes);
            if (captured != null) {
                allNamedCaptured.add(captured);
                if (isBreakOnFirstMatch()) break;
            }
        }
        // merge all named captured
        Map<String, Object> mergedValues = new HashMap<>();
        for (Map<String, Object> namedCaptured : allNamedCaptured) {
            if (keepEmptyCaptures) {
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

    private Map<String, String> patternDefinitions() {
        return Optional.ofNullable(patternDefinitions).orElse(Collections.emptyMap());
    }

    private List<File> patternsDir(RunContext runContext) {
        if (this.patternsDir == null || this.patternsDir.isEmpty()) return Collections.emptyList();

        return this.patternsDir
            .stream()
            .map(dir -> runContext.workingDir().resolve(Path.of(dir)))
            .map(Path::toFile)
            .collect(Collectors.toList());
    }

    private List<String> patterns() {
        if (pattern != null) return List.of(pattern);

        if (patterns == null || patterns.isEmpty()) {
            throw new IllegalArgumentException(
                "Missing required configuration, either `pattern` or `patterns` properties must not be empty.");
        }
        return patterns;
    }
}
