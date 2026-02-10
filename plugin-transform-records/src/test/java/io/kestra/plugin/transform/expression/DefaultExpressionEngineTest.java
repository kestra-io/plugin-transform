package io.kestra.plugin.transform.expression;

import com.amazon.ion.IonStruct;
import com.amazon.ion.IonValue;
import io.kestra.plugin.transform.ion.IonValueUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.lang.reflect.Field;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

class DefaultExpressionEngineTest {
    private static BigDecimal evalDecimal(DefaultExpressionEngine engine, String expression, IonStruct record) throws Exception {
        Object value = IonValueUtils.toJavaValue(engine.evaluate(expression, record));
        return new BigDecimal(value.toString());
    }

    private static void assertInvalid(DefaultExpressionEngine engine, IonStruct record, String expression, String causeContains) {
        ExpressionException exception = Assertions.assertThrows(
            ExpressionException.class,
            () -> engine.evaluate(expression, record)
        );

        assertThat(exception.getMessage(), containsString("Invalid expression: " + expression));
        assertThat(exception.getCause() != null, is(true));
        assertThat(exception.getCause().getMessage(), containsString(causeContains));
    }

    @Test
    void returnsNullForBlankExpression() throws Exception {
        DefaultExpressionEngine engine = new DefaultExpressionEngine();
        IonStruct record = IonValueUtils.system().newEmptyStruct();

        IonValue value = engine.evaluate("   ", record);

        assertThat(IonValueUtils.isNull(value), is(true));
    }

    @Test
    void rejectsInvalidExpression() {
        DefaultExpressionEngine engine = new DefaultExpressionEngine();
        IonStruct record = IonValueUtils.system().newEmptyStruct();

        ExpressionException exception = Assertions.assertThrows(
            ExpressionException.class,
            () -> engine.evaluate("user.", record)
        );

        assertThat(exception.getMessage(), containsString("Invalid expression: user."));
    }

    @Test
    void rejectsArrayExpansionOnNonList() {
        DefaultExpressionEngine engine = new DefaultExpressionEngine();
        IonStruct record = IonValueUtils.system().newEmptyStruct();
        record.put("items", IonValueUtils.system().newString("oops"));

        ExpressionException exception = Assertions.assertThrows(
            ExpressionException.class,
            () -> engine.evaluate("items[].price", record)
        );

        assertThat(exception.getMessage(), containsString("Expected list for segment 'items[]'"));
    }

    @Test
    void rejectsUnknownFunction() {
        DefaultExpressionEngine engine = new DefaultExpressionEngine();
        IonStruct record = IonValueUtils.system().newEmptyStruct();

        ExpressionException exception = Assertions.assertThrows(
            ExpressionException.class,
            () -> engine.evaluate("missingFn(1)", record)
        );

        assertThat(exception.getMessage(), containsString("Unknown function: missingFn"));
    }

    @Test
    void resolvesBracketFieldAccess() throws Exception {
        DefaultExpressionEngine engine = new DefaultExpressionEngine();
        IonStruct record = IonValueUtils.system().newEmptyStruct();
        IonStruct user = IonValueUtils.system().newEmptyStruct();
        user.put("first name", IonValueUtils.system().newString("Anna"));
        record.put("user", user);

        IonValue value = engine.evaluate("user[\"first name\"]", record);

        assertThat(IonValueUtils.toJavaValue(value), is("Anna"));
    }

    @Test
    void resolvesBracketFieldAccessOnPositionalScope() throws Exception {
        DefaultExpressionEngine engine = new DefaultExpressionEngine();
        IonStruct record = IonValueUtils.system().newEmptyStruct();
        IonStruct row = IonValueUtils.system().newEmptyStruct();
        row.put("field name", IonValueUtils.system().newInt(1));
        record.put("$1", row);

        IonValue value = engine.evaluate("$1[\"field name\"]", record);

        assertThat(IonValueUtils.toJavaValue(value), is(1L));
    }

    @Test
    void respectsArithmeticPrecedence() throws Exception {
        DefaultExpressionEngine engine = new DefaultExpressionEngine();
        IonStruct record = IonValueUtils.system().newEmptyStruct();

        assertThat(evalDecimal(engine, "1 + 2 * 3", record).compareTo(new BigDecimal("7")), is(0));
        assertThat(evalDecimal(engine, "(1 + 2) * 3", record).compareTo(new BigDecimal("9")), is(0));
        assertThat(evalDecimal(engine, "10 / 2 + 3", record).compareTo(new BigDecimal("8")), is(0));
    }

    @Test
    void respectsBooleanPrecedence() throws Exception {
        DefaultExpressionEngine engine = new DefaultExpressionEngine();
        IonStruct record = IonValueUtils.system().newEmptyStruct();

        assertThat(IonValueUtils.toJavaValue(engine.evaluate("true || false && false", record)), is(true));
        assertThat(IonValueUtils.toJavaValue(engine.evaluate("(true || false) && false", record)), is(false));
    }

    @Test
    void shortCircuitsBooleanOperators() throws Exception {
        DefaultExpressionEngine engine = new DefaultExpressionEngine();
        IonStruct record = IonValueUtils.system().newEmptyStruct();

        assertThat(IonValueUtils.toJavaValue(engine.evaluate("false && missingFn(1)", record)), is(false));
        assertThat(IonValueUtils.toJavaValue(engine.evaluate("true || missingFn(1)", record)), is(true));
    }

    @Test
    void respectsOperatorAssociativity() throws Exception {
        DefaultExpressionEngine engine = new DefaultExpressionEngine();
        IonStruct record = IonValueUtils.system().newEmptyStruct();

        assertThat(evalDecimal(engine, "10 - 5 - 1", record).compareTo(new BigDecimal("4")), is(0));
        assertThat(evalDecimal(engine, "100 / 10 / 2", record).compareTo(new BigDecimal("5")), is(0));
    }

    @Test
    void handlesNestedParentheses() throws Exception {
        DefaultExpressionEngine engine = new DefaultExpressionEngine();
        IonStruct record = IonValueUtils.system().newEmptyStruct();

        assertThat(evalDecimal(engine, "((1 + 2) * (3 + 4))", record).compareTo(new BigDecimal("21")), is(0));
    }

    @Test
    void supportsMixedPrecedenceAcrossOperators() throws Exception {
        DefaultExpressionEngine engine = new DefaultExpressionEngine();
        IonStruct record = IonValueUtils.system().newEmptyStruct();

        assertThat(IonValueUtils.toJavaValue(engine.evaluate("1 + 2 * 3 == 7", record)), is(true));
        assertThat(IonValueUtils.toJavaValue(engine.evaluate("1 + 2 * 3 > 6", record)), is(true));
        assertThat(IonValueUtils.toJavaValue(engine.evaluate("!(1 + 2 * 3 > 6)", record)), is(false));
    }

    @Test
    void nullPropagationAndEquality() throws Exception {
        DefaultExpressionEngine engine = new DefaultExpressionEngine();
        IonStruct record = IonValueUtils.system().newEmptyStruct();

        assertThat(IonValueUtils.toJavaValue(engine.evaluate("null == null", record)), is(true));
        assertThat(IonValueUtils.toJavaValue(engine.evaluate("null != null", record)), is(false));
        assertThat(IonValueUtils.toJavaValue(engine.evaluate("null == 1", record)), is(false));
        assertThat(IonValueUtils.isNull(engine.evaluate("null > 1", record)), is(true));
        assertThat(IonValueUtils.isNull(engine.evaluate("1 + null", record)), is(true));
    }

    @Test
    void nullPropagationForBooleanOperators() throws Exception {
        DefaultExpressionEngine engine = new DefaultExpressionEngine();
        IonStruct record = IonValueUtils.system().newEmptyStruct();

        assertThat(IonValueUtils.isNull(engine.evaluate("null && true", record)), is(true));
        assertThat(IonValueUtils.isNull(engine.evaluate("null || false", record)), is(true));

        assertThat(IonValueUtils.toJavaValue(engine.evaluate("false && null", record)), is(false));
        assertThat(IonValueUtils.toJavaValue(engine.evaluate("true || null", record)), is(true));

        assertThat(IonValueUtils.isNull(engine.evaluate("true && null", record)), is(true));
        assertThat(IonValueUtils.isNull(engine.evaluate("false || null", record)), is(true));

        assertThat(IonValueUtils.isNull(engine.evaluate("null && missingFn(1)", record)), is(true));
        assertThat(IonValueUtils.isNull(engine.evaluate("null || missingFn(1)", record)), is(true));
    }

    @Test
    void divisionByZeroReturnsNull() throws Exception {
        DefaultExpressionEngine engine = new DefaultExpressionEngine();
        IonStruct record = IonValueUtils.system().newEmptyStruct();

        assertThat(IonValueUtils.isNull(engine.evaluate("1 / 0", record)), is(true));
    }

    @Test
    void supportsStringEscapes() throws Exception {
        DefaultExpressionEngine engine = new DefaultExpressionEngine();
        IonStruct record = IonValueUtils.system().newEmptyStruct();

        assertThat(IonValueUtils.toJavaValue(engine.evaluate("\"a\\\\b\"", record)), is("a\\b"));
        assertThat(IonValueUtils.toJavaValue(engine.evaluate("\"a\\\"b\"", record)), is("a\"b"));
        assertThat(IonValueUtils.toJavaValue(engine.evaluate("\"a\\nb\"", record)), is("a\nb"));
        assertThat(IonValueUtils.toJavaValue(engine.evaluate("\"a\\tb\"", record)), is("a\tb"));
    }

    @Test
    void rejectsInvalidEscapeSequence() {
        DefaultExpressionEngine engine = new DefaultExpressionEngine();
        IonStruct record = IonValueUtils.system().newEmptyStruct();

        ExpressionException exception = Assertions.assertThrows(
            ExpressionException.class,
            () -> engine.evaluate("\"\\x\"", record)
        );

        assertThat(exception.getMessage(), containsString("Invalid expression"));
        assertThat(exception.getCause() != null, is(true));
        assertThat(exception.getCause().getMessage(), containsString("Invalid escape sequence"));
    }

    @Test
    void rejectsUnterminatedStringLiteral() {
        DefaultExpressionEngine engine = new DefaultExpressionEngine();
        IonStruct record = IonValueUtils.system().newEmptyStruct();

        ExpressionException exception = Assertions.assertThrows(
            ExpressionException.class,
            () -> engine.evaluate("\"abc", record)
        );

        assertThat(exception.getMessage(), containsString("Invalid expression"));
        assertThat(exception.getCause() != null, is(true));
        assertThat(exception.getCause().getMessage(), containsString("Unterminated string literal"));
    }

    @Test
    void expandsArraysAndPreservesNullsInResult() throws Exception {
        DefaultExpressionEngine engine = new DefaultExpressionEngine();
        IonStruct record = IonValueUtils.system().newEmptyStruct();

        IonStruct item1 = IonValueUtils.system().newEmptyStruct();
        item1.put("price", IonValueUtils.system().newInt(1));
        IonStruct item2 = IonValueUtils.system().newEmptyStruct();
        IonStruct item3 = IonValueUtils.system().newEmptyStruct();
        item3.put("price", IonValueUtils.system().newInt(3));

        var items = IonValueUtils.system().newEmptyList();
        items.add(item1);
        items.add(item2);
        items.add(item3);
        record.put("items", items);

        IonValue value = engine.evaluate("items[].price", record);
        assertThat(IonValueUtils.toJavaValue(value).toString(), is("[1, null, 3]"));
        assertThat(IonValueUtils.toJavaValue(engine.evaluate("sum(items[].price)", record)).toString(), is("4"));
    }

    @Test
    void supportsNestedArrayExpansion() throws Exception {
        DefaultExpressionEngine engine = new DefaultExpressionEngine();
        IonStruct record = IonValueUtils.system().newEmptyStruct();

        IonStruct line1 = IonValueUtils.system().newEmptyStruct();
        line1.put("price", IonValueUtils.system().newInt(1));
        var lines1 = IonValueUtils.system().newEmptyList();
        lines1.add(line1);

        IonStruct order1 = IonValueUtils.system().newEmptyStruct();
        order1.put("lines", lines1);

        IonStruct order2 = IonValueUtils.system().newEmptyStruct();
        order2.put("lines", IonValueUtils.system().newEmptyList());

        var orders = IonValueUtils.system().newEmptyList();
        orders.add(order1);
        orders.add(order2);
        record.put("orders", orders);

        IonValue value = engine.evaluate("orders[].lines[].price", record);
        assertThat(IonValueUtils.toJavaValue(value).toString(), is("[[1], []]"));
    }

    @Test
    void cachesCompiledExpressions() throws Exception {
        DefaultExpressionEngine engine = new DefaultExpressionEngine();
        IonStruct record = IonValueUtils.system().newEmptyStruct();
        record.put("a", IonValueUtils.system().newInt(1));

        engine.evaluate("a + 1", record);
        engine.evaluate("a + 1", record);
        engine.evaluate("a + 2", record);

        Field cacheField = DefaultExpressionEngine.class.getDeclaredField("cache");
        cacheField.setAccessible(true);
        Map<?, ?> cache = (Map<?, ?>) cacheField.get(engine);

        assertThat(cache.size(), is(2));
    }

    @Test
    void rejectsInvalidInputs() {
        DefaultExpressionEngine engine = new DefaultExpressionEngine();
        IonStruct record = IonValueUtils.system().newEmptyStruct();

        assertInvalid(engine, record, "(", "Unexpected token");
        assertInvalid(engine, record, "1 + 2)", "Unexpected token");
        assertInvalid(engine, record, "user..id", "Expected identifier after '.'");
        assertInvalid(engine, record, "user[foo]", "Expected ']' or string key after '['");
        assertInvalid(engine, record, "user[", "Expected ']' or string key after '['");
        assertInvalid(engine, record, "user[\"x\"", "Expected ']'");
        assertInvalid(engine, record, "sum(1 2)", "Expected ')'");
        assertInvalid(engine, record, "sum(,)", "Unexpected token");
        assertInvalid(engine, record, "1..2", "Invalid number literal");
        assertInvalid(engine, record, "@", "Unexpected character");
        assertInvalid(engine, record, "toInt()", "Expected 1 arguments");
        assertInvalid(engine, record, "toInt(1, 2)", "Expected 1 arguments");
    }
}
