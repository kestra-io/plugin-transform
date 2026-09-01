package io.kestra.plugin.transform.grok.pattern;

import io.kestra.plugin.transform.grok.pattern.GrokMatcher;
import io.kestra.plugin.transform.grok.pattern.GrokPatternCompiler;
import io.kestra.plugin.transform.grok.pattern.GrokPatternResolver;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

class GrokMatcherTest {
    private GrokPatternCompiler compiler;

    @BeforeEach
    public void setUp() {
        compiler = new GrokPatternCompiler(new GrokPatternResolver(), false);
    }

    @Test
    public void shouldParseGivenSimpleGrokPattern() {
        GrokPatternCompiler compiler = new GrokPatternCompiler(new GrokPatternResolver(), false);
        final GrokMatcher matcher = compiler.compile("%{EMAILADDRESS}");
        final Map<String, Object> captured = matcher.captures("test@kafka.org".getBytes(StandardCharsets.UTF_8));
        Assertions.assertEquals("kafka.org", captured.get("HOSTNAME"));
        Assertions.assertEquals("test@kafka.org", captured.get("EMAILADDRESS"));
        Assertions.assertEquals("test", captured.get("EMAILLOCALPART"));
    }

    @Test
    public void shouldParseGivenCustomGrokPattern() {
        final GrokMatcher matcher = compiler.compile("(?<EMAILADDRESS>(?<EMAILLOCALPART>[a-zA-Z][a-zA-Z0-9_.+-=:]+)@(?<HOSTNAME>\\b(?:[0-9A-Za-z][0-9A-Za-z-]{0,62})(?:\\.(?:[0-9A-Za-z][0-9A-Za-z-]{0,62}))*(\\.?|\\b)))");
        final Map<String, Object> captured = matcher.captures("test@kestra.io".getBytes(StandardCharsets.UTF_8));
        Assertions.assertEquals("kestra.io", captured.get("HOSTNAME"));
        Assertions.assertEquals("test@kestra.io", captured.get("EMAILADDRESS"));
        Assertions.assertEquals("test", captured.get("EMAILLOCALPART"));
    }

    @Test
    public void shouldConvertCapturedValueGivenTypeConverter() {
        final GrokMatcher matcher = compiler.compile("%{NUMBER:n:int}");
        final Map<String, Object> captured = matcher.captures("7".getBytes(StandardCharsets.UTF_8));
        Assertions.assertEquals(7, captured.get("n"));
    }

    @Test
    public void shouldConvertCapturedValuesGivenAllTypeConverters() {
        final GrokPatternCompiler namedOnly = new GrokPatternCompiler(new GrokPatternResolver(), true);
        final GrokMatcher matcher = namedOnly.compile("%{NUMBER:s:short} %{NUMBER:i:int} %{NUMBER:l:long} %{NUMBER:f:float} %{NUMBER:d:double} %{WORD:b:boolean} %{WORD:str}");
        final Map<String, Object> captured = matcher.captures("1 2 3 4.5 6.5 true kestra".getBytes(StandardCharsets.UTF_8));

        Assertions.assertEquals((short) 1, captured.get("s"));
        Assertions.assertEquals(2, captured.get("i"));
        Assertions.assertEquals(3L, captured.get("l"));
        Assertions.assertEquals(4.5F, captured.get("f"));
        Assertions.assertEquals(6.5D, captured.get("d"));
        Assertions.assertEquals(true, captured.get("b"));
        Assertions.assertEquals("kestra", captured.get("str"));
    }

    @Test
    public void shouldNotConvertCapturedValueGivenNoTypeConverter() {
        final GrokMatcher matcher = compiler.compile("%{NUMBER:n}");
        final Map<String, Object> captured = matcher.captures("7".getBytes(StandardCharsets.UTF_8));
        Assertions.assertEquals("7", captured.get("n"));
    }

    @Test
    public void shouldThrowGivenValueNotConvertibleToDeclaredType() {
        final GrokMatcher matcher = compiler.compile("%{WORD:w:int}");
        GrokException exception = Assertions.assertThrows(
            GrokException.class,
            () -> matcher.captures("kestra".getBytes(StandardCharsets.UTF_8))
        );
        Assertions.assertTrue(exception.getMessage().contains("w"));
        Assertions.assertTrue(exception.getMessage().contains("INT"));
    }

    @Test
    public void shouldNotConvertEmptyCaptureGivenTypeConverter() {
        final GrokPatternCompiler namedOnly = new GrokPatternCompiler(new GrokPatternResolver(), true);
        final GrokMatcher matcher = namedOnly.compile("%{IP:client_ip}(?:\\s+%{NUMBER:port:int})?");
        final Map<String, Object> captured = matcher.captures("192.168.1.1".getBytes(StandardCharsets.UTF_8));

        Assertions.assertEquals("192.168.1.1", captured.get("client_ip"));
        Assertions.assertEquals("", captured.get("port"));
    }
}
