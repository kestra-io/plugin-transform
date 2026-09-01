package io.kestra.plugin.transform.grok.pattern;

import io.kestra.plugin.transform.grok.data.Type;
import org.jcodings.specific.UTF8Encoding;
import org.joni.Matcher;
import org.joni.NameEntry;
import org.joni.Option;
import org.joni.Regex;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public final class GrokMatcher {

    private final Map<String, GrokPattern> patternsByName;

    private final List<GrokPattern> patterns;

    private final String expression;

    private final Regex regex;

    private final List<GrokCaptureGroup> grokCaptureGroups;

    /**
     * Creates a new {@link GrokMatcher} instance.
     *
     * @param patterns    the list of patterns.
     * @param expression  the original expression.
     */
    GrokMatcher(final List<GrokPattern> patterns,
                final String expression) {
        Objects.requireNonNull(patterns, "pattern can't be null");
        Objects.requireNonNull(expression, "expression can't be null");
        this.patterns = patterns;
        this.expression = expression;
        // Patterns are indexed by the name of the regex capture group they produce, i.e., their
        // semantic when one is defined - see GrokPatternCompiler#compileRegex - otherwise their syntax.
        this.patternsByName = patterns
            .stream()
            .collect(Collectors.toMap(GrokMatcher::captureNameOf, p -> p,  (p1, p2) -> p1.semantic() != null ? p1 : p2));
        byte[] bytes = expression.getBytes(StandardCharsets.UTF_8);
        regex = new Regex(bytes, 0, bytes.length, Option.NONE, UTF8Encoding.INSTANCE);

        grokCaptureGroups = new ArrayList<>();
        for (Iterator<NameEntry> entry = regex.namedBackrefIterator(); entry.hasNext();) {
            NameEntry nameEntry = entry.next();
            final String field = new String(
                nameEntry.name,
                nameEntry.nameP,
                nameEntry.nameEnd - nameEntry.nameP,
                StandardCharsets.UTF_8);
            final GrokPattern pattern = getGrokPattern(field);
            final Type type = pattern != null ? pattern.type() : Type.STRING;
            grokCaptureGroups.add(new GrokCaptureGroup(type, field, nameEntry.getBackRefs()));
        }
    }

    public GrokPattern getGrokPattern(final int i) {
        return patterns.get(i);
    }

    public GrokPattern getGrokPattern(final String name) {
        return patternsByName.get(name);
    }

    private static String captureNameOf(final GrokPattern pattern) {
        return pattern.semantic() != null ? pattern.semantic() : pattern.syntax();
    }

    /**
     * Returns the compiled regex expression.
     * @return the {@link Regex}.
     */
    public Regex regex() {
        return regex;
    }

    /**
     * Returns the raw regex expression.
     * @return the expression.
     */
    public String expression() {
        return expression;
    }

    /**
     * Captures matching group.
     *
     * @param bytes the text bytes to match.
     * @return      a {@code Map} that contains all named captured.
     */
    public Map<String, Object> captures(final byte[] bytes) {

        long now = System.currentTimeMillis();
        final var extractor = new GrokCaptureExtractor.MapGrokCaptureExtractor(grokCaptureGroups);

        final Matcher matcher = regex.matcher(bytes);
        int result = matcher.search(0, bytes.length, Option.DEFAULT);

        if (result == Matcher.FAILED) {
            return null;
        }
        if (result == Matcher.INTERRUPTED) {
            long interruptedAfterMs = System.currentTimeMillis() - now;
            throw new RuntimeException("Grok pattern matching was interrupted before completion (" + interruptedAfterMs + " ms)");
        }
        extractor.extract(bytes, matcher.getEagerRegion());

        return extractor.captured();
    }

    @Override
    public String toString() {
        return "GrokMatcher{" +
                "patterns=" + patterns +
                ", expression='" + expression + '\'' +
                '}';
    }
}
