package io.kestra.plugin.transform.expression;

import com.amazon.ion.IonList;
import com.amazon.ion.IonString;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonValue;
import com.amazon.ion.Timestamp;
import io.kestra.plugin.transform.ion.CastException;
import io.kestra.plugin.transform.ion.IonValueUtils;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;

public final class DefaultExpressionEngine implements ExpressionEngine {
    private final java.util.Map<String, Expr> cache = new ConcurrentHashMap<>();

    @Override
    public IonValue evaluate(String expression, IonStruct record) throws ExpressionException {
        if (expression == null || expression.isBlank()) {
            return IonValueUtils.nullValue();
        }
        try {
            Expr compiled = cache.computeIfAbsent(expression, this::compile);
            return compiled.eval(new EvalContext(record));
        } catch (IllegalArgumentException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new ExpressionException("Invalid expression: " + expression, cause);
        }
    }

    private Expr compile(String expression) {
        try {
            Tokenizer tokenizer = new Tokenizer(expression);
            Parser parser = new Parser(tokenizer);
            return parser.parseExpression();
        } catch (ExpressionException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static final class EvalContext {
        private final IonStruct record;

        private EvalContext(IonStruct record) {
            this.record = record;
        }
    }

    private interface Expr {
        IonValue eval(EvalContext context) throws ExpressionException;
    }

    private static final class LiteralExpr implements Expr {
        private final IonValue value;

        private LiteralExpr(IonValue value) {
            this.value = value;
        }

        @Override
        public IonValue eval(EvalContext context) {
            return value;
        }
    }

    private static final class PathExpr implements Expr {
        private final List<PathSegment> segments;

        private PathExpr(List<PathSegment> segments) {
            this.segments = segments;
        }

        @Override
        public IonValue eval(EvalContext context) throws ExpressionException {
            return resolvePath(context.record, 0);
        }

        private IonValue resolvePath(IonValue value, int index) throws ExpressionException {
            if (index >= segments.size()) {
                return value;
            }
            if (IonValueUtils.isNull(value)) {
                return IonValueUtils.nullValue();
            }
            PathSegment segment = segments.get(index);
            IonValue nextValue = null;
            if (value instanceof IonStruct struct) {
                nextValue = struct.get(segment.name());
            } else {
                throw new ExpressionException("Expected struct for path segment '" + segment.name() + "'");
            }

            if (segment.arrayExpand()) {
                if (IonValueUtils.isNull(nextValue)) {
                    return IonValueUtils.nullValue();
                }
                if (!(nextValue instanceof IonList list)) {
                    throw new ExpressionException("Expected list for segment '" + segment.name() + "[]'");
                }
                if (index == segments.size() - 1) {
                    return list;
                }
                IonList result = IonValueUtils.system().newEmptyList();
                for (IonValue element : list) {
                    IonValue resolved = resolvePath(element, index + 1);
                    result.add(IonValueUtils.isNull(resolved)
                        ? IonValueUtils.nullValue()
                        : IonValueUtils.cloneValue(resolved));
                }
                return result;
            }
            return resolvePath(nextValue, index + 1);
        }
    }

    private static final class PathSegment {
        private final String name;
        private boolean arrayExpand;

        private PathSegment(String name) {
            this.name = name;
        }

        String name() {
            return name;
        }

        boolean arrayExpand() {
            return arrayExpand;
        }

        void setArrayExpand(boolean arrayExpand) {
            this.arrayExpand = arrayExpand;
        }
    }

    private static final class UnaryExpr implements Expr {
        private final TokenType operator;
        private final Expr expr;

        private UnaryExpr(TokenType operator, Expr expr) {
            this.operator = operator;
            this.expr = expr;
        }

        @Override
        public IonValue eval(EvalContext context) throws ExpressionException {
            IonValue value = expr.eval(context);
            if (IonValueUtils.isNull(value)) {
                return IonValueUtils.nullValue();
            }
            return switch (operator) {
                case MINUS -> negate(value);
                case BANG -> IonValueUtils.system().newBool(!asBoolean(value));
                default -> throw new ExpressionException("Unsupported unary operator: " + operator);
            };
        }

        private IonValue negate(IonValue value) throws ExpressionException {
            BigDecimal decimal = asDecimal(value);
            return IonValueUtils.system().newDecimal(decimal.negate());
        }

        private BigDecimal asDecimal(IonValue value) throws ExpressionException {
            try {
                return IonValueUtils.asDecimal(value);
            } catch (CastException e) {
                throw new ExpressionException(e.getMessage(), e);
            }
        }

        private boolean asBoolean(IonValue value) throws ExpressionException {
            try {
                Boolean bool = IonValueUtils.asBoolean(value);
                return bool != null && bool;
            } catch (CastException e) {
                throw new ExpressionException(e.getMessage(), e);
            }
        }
    }

    private static final class BinaryExpr implements Expr {
        private final Expr left;
        private final Expr right;
        private final TokenType operator;

        private BinaryExpr(Expr left, Expr right, TokenType operator) {
            this.left = left;
            this.right = right;
            this.operator = operator;
        }

        @Override
        public IonValue eval(EvalContext context) throws ExpressionException {
            if (operator == TokenType.AND_AND || operator == TokenType.OR_OR) {
                return evaluateBoolean(context);
            }
            IonValue leftValue = left.eval(context);
            IonValue rightValue = right.eval(context);
            if (operator == TokenType.EQ_EQ || operator == TokenType.NOT_EQ) {
                boolean equals = equalsValue(leftValue, rightValue);
                return IonValueUtils.system().newBool(operator == TokenType.EQ_EQ ? equals : !equals);
            }
            if (operator == TokenType.GT || operator == TokenType.GTE || operator == TokenType.LT || operator == TokenType.LTE) {
                return compare(leftValue, rightValue);
            }
            if (operator == TokenType.PLUS || operator == TokenType.MINUS || operator == TokenType.STAR || operator == TokenType.SLASH) {
                return arithmetic(leftValue, rightValue);
            }
            throw new ExpressionException("Unsupported operator: " + operator);
        }

        private IonValue evaluateBoolean(EvalContext context) throws ExpressionException {
            IonValue leftValue = left.eval(context);
            if (IonValueUtils.isNull(leftValue)) {
                return IonValueUtils.nullValue();
            }
            boolean leftBool = asBoolean(leftValue);
            if (operator == TokenType.AND_AND) {
                if (!leftBool) {
                    return IonValueUtils.system().newBool(false);
                }
            } else {
                if (leftBool) {
                    return IonValueUtils.system().newBool(true);
                }
            }

            IonValue rightValue = right.eval(context);
            if (IonValueUtils.isNull(rightValue)) {
                return IonValueUtils.nullValue();
            }
            boolean rightBool = asBoolean(rightValue);
            boolean result = operator == TokenType.AND_AND ? (leftBool && rightBool) : (leftBool || rightBool);
            return IonValueUtils.system().newBool(result);
        }

        private boolean asBoolean(IonValue value) throws ExpressionException {
            try {
                Boolean bool = IonValueUtils.asBoolean(value);
                return bool != null && bool;
            } catch (CastException e) {
                throw new ExpressionException(e.getMessage(), e);
            }
        }

        private boolean equalsValue(IonValue leftValue, IonValue rightValue) {
            if (IonValueUtils.isNull(leftValue) && IonValueUtils.isNull(rightValue)) {
                return true;
            }
            if (IonValueUtils.isNull(leftValue) || IonValueUtils.isNull(rightValue)) {
                return false;
            }
            return leftValue.equals(rightValue);
        }

        private IonValue compare(IonValue leftValue, IonValue rightValue) throws ExpressionException {
            if (IonValueUtils.isNull(leftValue) || IonValueUtils.isNull(rightValue)) {
                return IonValueUtils.nullValue();
            }
            if (leftValue instanceof IonString leftString && rightValue instanceof IonString rightString) {
                int comparison = leftString.stringValue().compareTo(rightString.stringValue());
                return IonValueUtils.system().newBool(compareResult(comparison));
            }
            BigDecimal leftDecimal = asDecimal(leftValue);
            BigDecimal rightDecimal = asDecimal(rightValue);
            int comparison = leftDecimal.compareTo(rightDecimal);
            return IonValueUtils.system().newBool(compareResult(comparison));
        }

        private boolean compareResult(int comparison) {
            return switch (operator) {
                case GT -> comparison > 0;
                case GTE -> comparison >= 0;
                case LT -> comparison < 0;
                case LTE -> comparison <= 0;
                default -> false;
            };
        }

        private IonValue arithmetic(IonValue leftValue, IonValue rightValue) throws ExpressionException {
            if (IonValueUtils.isNull(leftValue) || IonValueUtils.isNull(rightValue)) {
                return IonValueUtils.nullValue();
            }
            BigDecimal leftDecimal = asDecimal(leftValue);
            BigDecimal rightDecimal = asDecimal(rightValue);
            BigDecimal result = switch (operator) {
                case PLUS -> leftDecimal.add(rightDecimal);
                case MINUS -> leftDecimal.subtract(rightDecimal);
                case STAR -> leftDecimal.multiply(rightDecimal);
                case SLASH -> rightDecimal.compareTo(BigDecimal.ZERO) == 0
                    ? null
                    : leftDecimal.divide(rightDecimal, 10, java.math.RoundingMode.HALF_UP);
                default -> throw new ExpressionException("Unsupported arithmetic operator: " + operator);
            };
            if (result == null) {
                return IonValueUtils.nullValue();
            }
            return IonValueUtils.system().newDecimal(result);
        }

        private BigDecimal asDecimal(IonValue value) throws ExpressionException {
            try {
                return IonValueUtils.asDecimal(value);
            } catch (CastException e) {
                throw new ExpressionException(e.getMessage(), e);
            }
        }
    }

    private static final class FunctionExpr implements Expr {
        private final String name;
        private final List<Expr> args;

        private FunctionExpr(String name, List<Expr> args) {
            this.name = name;
            this.args = args;
        }

        @Override
        public IonValue eval(EvalContext context) throws ExpressionException {
            List<IonValue> values = new ArrayList<>(args.size());
            for (Expr expr : args) {
                values.add(expr.eval(context));
            }
            return applyFunction(name, values);
        }

        private IonValue applyFunction(String name, List<IonValue> values) throws ExpressionException {
            return switch (name.toLowerCase(Locale.ROOT)) {
                case "toint" -> castInt(values);
                case "todecimal" -> castDecimal(values);
                case "tostring" -> castString(values);
                case "toboolean" -> castBoolean(values);
                case "parsetimestamp" -> parseTimestamp(values);
                case "sum" -> sum(values);
                case "count" -> count(values);
                case "min" -> min(values);
                case "max" -> max(values);
                case "coalesce" -> coalesce(values);
                case "concat" -> concat(values);
                default -> throw new ExpressionException("Unknown function: " + name);
            };
        }

        private IonValue castInt(List<IonValue> values) throws ExpressionException {
            requireArgCount(values, 1);
            try {
                BigDecimal decimal = IonValueUtils.asDecimal(values.get(0));
                if (decimal == null) {
                    return IonValueUtils.nullValue();
                }
                return IonValueUtils.system().newInt(decimal.longValueExact());
            } catch (Exception e) {
                throw new ExpressionException("Invalid integer cast", e);
            }
        }

        private IonValue castDecimal(List<IonValue> values) throws ExpressionException {
            requireArgCount(values, 1);
            try {
                BigDecimal decimal = IonValueUtils.asDecimal(values.get(0));
                if (decimal == null) {
                    return IonValueUtils.nullValue();
                }
                return IonValueUtils.system().newDecimal(decimal);
            } catch (Exception e) {
                throw new ExpressionException("Invalid decimal cast", e);
            }
        }

        private IonValue castString(List<IonValue> values) {
            requireArgCount(values, 1);
            String stringValue = IonValueUtils.asString(values.get(0));
            if (stringValue == null) {
                return IonValueUtils.nullValue();
            }
            return IonValueUtils.system().newString(stringValue);
        }

        private IonValue castBoolean(List<IonValue> values) throws ExpressionException {
            requireArgCount(values, 1);
            try {
                Boolean bool = IonValueUtils.asBoolean(values.get(0));
                if (bool == null) {
                    return IonValueUtils.nullValue();
                }
                return IonValueUtils.system().newBool(bool != null && bool);
            } catch (CastException e) {
                throw new ExpressionException("Invalid boolean cast", e);
            }
        }

        private IonValue parseTimestamp(List<IonValue> values) throws ExpressionException {
            requireArgCount(values, 1);
            try {
                Instant instant = IonValueUtils.asInstant(values.get(0));
                if (instant == null) {
                    return IonValueUtils.nullValue();
                }
                return IonValueUtils.system().newTimestamp(Timestamp.forMillis(instant.toEpochMilli(), null));
            } catch (CastException e) {
                throw new ExpressionException("Invalid timestamp value", e);
            }
        }

        private IonValue sum(List<IonValue> values) throws ExpressionException {
            requireArgCount(values, 1);
            IonList list = asList(values.get(0));
            if (list == null) {
                return IonValueUtils.nullValue();
            }
            BigDecimal total = BigDecimal.ZERO;
            for (IonValue value : list) {
                if (IonValueUtils.isNull(value)) {
                    continue;
                }
                total = total.add(asDecimal(value));
            }
            return IonValueUtils.system().newDecimal(total);
        }

        private IonValue count(List<IonValue> values) throws ExpressionException {
            requireArgCount(values, 1);
            IonList list = asList(values.get(0));
            if (list == null) {
                return IonValueUtils.nullValue();
            }
            return IonValueUtils.system().newInt(list.size());
        }

        private IonValue min(List<IonValue> values) throws ExpressionException {
            requireArgCount(values, 1);
            IonList list = asList(values.get(0));
            if (list == null || list.isEmpty()) {
                return IonValueUtils.nullValue();
            }
            BigDecimal min = null;
            for (IonValue value : list) {
                if (IonValueUtils.isNull(value)) {
                    continue;
                }
                BigDecimal decimal = asDecimal(value);
                if (min == null || decimal.compareTo(min) < 0) {
                    min = decimal;
                }
            }
            return min == null ? IonValueUtils.nullValue() : IonValueUtils.system().newDecimal(min);
        }

        private IonValue max(List<IonValue> values) throws ExpressionException {
            requireArgCount(values, 1);
            IonList list = asList(values.get(0));
            if (list == null || list.isEmpty()) {
                return IonValueUtils.nullValue();
            }
            BigDecimal max = null;
            for (IonValue value : list) {
                if (IonValueUtils.isNull(value)) {
                    continue;
                }
                BigDecimal decimal = asDecimal(value);
                if (max == null || decimal.compareTo(max) > 0) {
                    max = decimal;
                }
            }
            return max == null ? IonValueUtils.nullValue() : IonValueUtils.system().newDecimal(max);
        }

        private IonValue coalesce(List<IonValue> values) {
            for (IonValue value : values) {
                if (!IonValueUtils.isNull(value)) {
                    return value;
                }
            }
            return IonValueUtils.nullValue();
        }

        private IonValue concat(List<IonValue> values) {
            StringBuilder builder = new StringBuilder();
            for (IonValue value : values) {
                if (IonValueUtils.isNull(value)) {
                    continue;
                }
                builder.append(IonValueUtils.asString(value));
            }
            return IonValueUtils.system().newString(builder.toString());
        }

        private IonList asList(IonValue value) throws ExpressionException {
            if (IonValueUtils.isNull(value)) {
                return null;
            }
            if (!(value instanceof IonList list)) {
                throw new ExpressionException("Expected list argument");
            }
            return list;
        }

        private BigDecimal asDecimal(IonValue value) throws ExpressionException {
            try {
                return IonValueUtils.asDecimal(value);
            } catch (CastException e) {
                throw new ExpressionException(e.getMessage(), e);
            }
        }

        private void requireArgCount(List<IonValue> values, int expected) {
            if (values.size() != expected) {
                throw new IllegalArgumentException("Expected " + expected + " arguments, got " + values.size());
            }
        }
    }

    private enum TokenType {
        IDENT,
        NUMBER,
        STRING,
        LPAREN,
        RPAREN,
        COMMA,
        DOT,
        LBRACKET,
        RBRACKET,
        PLUS,
        MINUS,
        STAR,
        SLASH,
        AND_AND,
        OR_OR,
        EQ_EQ,
        NOT_EQ,
        GT,
        GTE,
        LT,
        LTE,
        BANG,
        EOF
    }

    private record Token(TokenType type, String text) {
    }

    private static final class Tokenizer {
        private final String input;
        private int index;

        private Tokenizer(String input) {
            this.input = input;
        }

        Token next() throws ExpressionException {
            skipWhitespace();
            if (index >= input.length()) {
                return new Token(TokenType.EOF, "");
            }
            char current = input.charAt(index);
            if (current == '$' && index + 1 < input.length() && Character.isDigit(input.charAt(index + 1))) {
                return readPositionalIdentifier();
            }
            if (Character.isLetter(current) || current == '_') {
                return readIdentifier();
            }
            if (Character.isDigit(current)) {
                return readNumber();
            }
            if (current == '"') {
                return readString();
            }
            if (current == '&' && peek('&')) {
                index += 2;
                return new Token(TokenType.AND_AND, "&&");
            }
            if (current == '|' && peek('|')) {
                index += 2;
                return new Token(TokenType.OR_OR, "||");
            }
            if (current == '=' && peek('=')) {
                index += 2;
                return new Token(TokenType.EQ_EQ, "==");
            }
            if (current == '!' && peek('=')) {
                index += 2;
                return new Token(TokenType.NOT_EQ, "!=");
            }
            if (current == '>' && peek('=')) {
                index += 2;
                return new Token(TokenType.GTE, ">=");
            }
            if (current == '<' && peek('=')) {
                index += 2;
                return new Token(TokenType.LTE, "<=");
            }
            index++;
            return switch (current) {
                case '(' -> new Token(TokenType.LPAREN, "(");
                case ')' -> new Token(TokenType.RPAREN, ")");
                case ',' -> new Token(TokenType.COMMA, ",");
                case '.' -> new Token(TokenType.DOT, ".");
                case '[' -> new Token(TokenType.LBRACKET, "[");
                case ']' -> new Token(TokenType.RBRACKET, "]");
                case '+' -> new Token(TokenType.PLUS, "+");
                case '-' -> new Token(TokenType.MINUS, "-");
                case '*' -> new Token(TokenType.STAR, "*");
                case '/' -> new Token(TokenType.SLASH, "/");
                case '>' -> new Token(TokenType.GT, ">");
                case '<' -> new Token(TokenType.LT, "<");
                case '!' -> new Token(TokenType.BANG, "!");
                default -> throw new ExpressionException("Unexpected character: " + current);
            };
        }

        private Token readIdentifier() {
            int start = index;
            index++;
            while (index < input.length()) {
                char current = input.charAt(index);
                if (!Character.isLetterOrDigit(current) && current != '_') {
                    break;
                }
                index++;
            }
            return new Token(TokenType.IDENT, input.substring(start, index));
        }

        private Token readPositionalIdentifier() {
            int start = index;
            index++; // '$'
            while (index < input.length() && Character.isDigit(input.charAt(index))) {
                index++;
            }
            return new Token(TokenType.IDENT, input.substring(start, index));
        }

        private Token readNumber() {
            int start = index;
            index++;
            while (index < input.length()) {
                char current = input.charAt(index);
                if (Character.isDigit(current) || current == '.') {
                    index++;
                    continue;
                }
                if (current == 'e' || current == 'E') {
                    index++;
                    if (index < input.length()) {
                        char sign = input.charAt(index);
                        if (sign == '+' || sign == '-') {
                            index++;
                        }
                    }
                    continue;
                }
                break;
            }
            return new Token(TokenType.NUMBER, input.substring(start, index));
        }

        private Token readString() throws ExpressionException {
            index++; // opening quote
            StringBuilder builder = new StringBuilder();
            while (index < input.length()) {
                char current = input.charAt(index);
                if (current == '"') {
                    index++;
                    return new Token(TokenType.STRING, builder.toString());
                }
                if (current == '\\') {
                    index++;
                    if (index >= input.length()) {
                        throw new ExpressionException("Unterminated string literal");
                    }
                    char escaped = input.charAt(index);
                    builder.append(switch (escaped) {
                        case '"', '\\', '/' -> escaped;
                        case 'n' -> '\n';
                        case 'r' -> '\r';
                        case 't' -> '\t';
                        default -> throw new ExpressionException("Invalid escape sequence: \\" + escaped);
                    });
                    index++;
                    continue;
                }
                builder.append(current);
                index++;
            }
            if (index >= input.length()) {
                throw new ExpressionException("Unterminated string literal");
            }
            throw new ExpressionException("Unterminated string literal");
        }

        private boolean peek(char expected) {
            return index + 1 < input.length() && input.charAt(index + 1) == expected;
        }

        private void skipWhitespace() {
            while (index < input.length() && Character.isWhitespace(input.charAt(index))) {
                index++;
            }
        }
    }

    private static final class Parser {
        private final Tokenizer tokenizer;
        private Token current;

        private Parser(Tokenizer tokenizer) throws ExpressionException {
            this.tokenizer = tokenizer;
            this.current = tokenizer.next();
        }

        Expr parseExpression() throws ExpressionException {
            Expr expr = parseOr();
            if (current.type() != TokenType.EOF) {
                throw new ExpressionException("Unexpected token: " + current.text());
            }
            return expr;
        }

        private Expr parseOr() throws ExpressionException {
            Expr expr = parseAnd();
            while (match(TokenType.OR_OR)) {
                expr = new BinaryExpr(expr, parseAnd(), TokenType.OR_OR);
            }
            return expr;
        }

        private Expr parseAnd() throws ExpressionException {
            Expr expr = parseEquality();
            while (match(TokenType.AND_AND)) {
                expr = new BinaryExpr(expr, parseEquality(), TokenType.AND_AND);
            }
            return expr;
        }

        private Expr parseEquality() throws ExpressionException {
            Expr expr = parseComparison();
            while (true) {
                if (match(TokenType.EQ_EQ)) {
                    expr = new BinaryExpr(expr, parseComparison(), TokenType.EQ_EQ);
                } else if (match(TokenType.NOT_EQ)) {
                    expr = new BinaryExpr(expr, parseComparison(), TokenType.NOT_EQ);
                } else {
                    break;
                }
            }
            return expr;
        }

        private Expr parseComparison() throws ExpressionException {
            Expr expr = parseTerm();
            while (true) {
                if (match(TokenType.GT)) {
                    expr = new BinaryExpr(expr, parseTerm(), TokenType.GT);
                } else if (match(TokenType.GTE)) {
                    expr = new BinaryExpr(expr, parseTerm(), TokenType.GTE);
                } else if (match(TokenType.LT)) {
                    expr = new BinaryExpr(expr, parseTerm(), TokenType.LT);
                } else if (match(TokenType.LTE)) {
                    expr = new BinaryExpr(expr, parseTerm(), TokenType.LTE);
                } else {
                    break;
                }
            }
            return expr;
        }

        private Expr parseTerm() throws ExpressionException {
            Expr expr = parseFactor();
            while (true) {
                if (match(TokenType.PLUS)) {
                    expr = new BinaryExpr(expr, parseFactor(), TokenType.PLUS);
                } else if (match(TokenType.MINUS)) {
                    expr = new BinaryExpr(expr, parseFactor(), TokenType.MINUS);
                } else {
                    break;
                }
            }
            return expr;
        }

        private Expr parseFactor() throws ExpressionException {
            Expr expr = parseUnary();
            while (true) {
                if (match(TokenType.STAR)) {
                    expr = new BinaryExpr(expr, parseUnary(), TokenType.STAR);
                } else if (match(TokenType.SLASH)) {
                    expr = new BinaryExpr(expr, parseUnary(), TokenType.SLASH);
                } else {
                    break;
                }
            }
            return expr;
        }

        private Expr parseUnary() throws ExpressionException {
            if (match(TokenType.BANG)) {
                return new UnaryExpr(TokenType.BANG, parseUnary());
            }
            if (match(TokenType.MINUS)) {
                return new UnaryExpr(TokenType.MINUS, parseUnary());
            }
            return parsePrimary();
        }

        private Expr parsePrimary() throws ExpressionException {
            if (match(TokenType.NUMBER)) {
                return parseNumber(previous());
            }
            if (match(TokenType.STRING)) {
                return new LiteralExpr(IonValueUtils.system().newString(previous().text()));
            }
            if (match(TokenType.IDENT)) {
                String ident = previous().text();
                if ("true".equalsIgnoreCase(ident)) {
                    return new LiteralExpr(IonValueUtils.system().newBool(true));
                }
                if ("false".equalsIgnoreCase(ident)) {
                    return new LiteralExpr(IonValueUtils.system().newBool(false));
                }
                if ("null".equalsIgnoreCase(ident)) {
                    return new LiteralExpr(IonValueUtils.nullValue());
                }
                if (match(TokenType.LPAREN)) {
                    List<Expr> args = new ArrayList<>();
                    if (!check(TokenType.RPAREN)) {
                        do {
                            args.add(parseOr());
                        } while (match(TokenType.COMMA));
                    }
                    consume(TokenType.RPAREN, "Expected ')'");
                    return new FunctionExpr(ident, args);
                }
                return parsePath(ident);
            }
            if (match(TokenType.LPAREN)) {
                Expr expr = parseOr();
                consume(TokenType.RPAREN, "Expected ')'");
                return expr;
            }
            throw new ExpressionException("Unexpected token: " + current.text());
        }

        private Expr parseNumber(Token token) {
            String raw = token.text();
            try {
                BigDecimal decimal = new BigDecimal(raw);
                return new LiteralExpr(IonValueUtils.system().newDecimal(decimal));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(new ExpressionException("Invalid number literal: " + raw, e));
            }
        }

        private Expr parsePath(String first) throws ExpressionException {
            List<PathSegment> segments = new ArrayList<>();
            PathSegment segment = new PathSegment(first);
            segments.add(segment);
            while (true) {
                if (match(TokenType.LBRACKET)) {
                    if (match(TokenType.RBRACKET)) {
                        segment.setArrayExpand(true);
                        continue;
                    }
                    if (match(TokenType.STRING)) {
                        String fieldName = previous().text();
                        consume(TokenType.RBRACKET, "Expected ']'");
                        segment = new PathSegment(fieldName);
                        segments.add(segment);
                        continue;
                    }
                    throw new ExpressionException("Expected ']' or string key after '['");
                }
                if (match(TokenType.DOT)) {
                    consume(TokenType.IDENT, "Expected identifier after '.'");
                    segment = new PathSegment(previous().text());
                    segments.add(segment);
                    continue;
                }
                break;
            }
            return new PathExpr(segments);
        }

        private boolean match(TokenType type) throws ExpressionException {
            if (check(type)) {
                advance();
                return true;
            }
            return false;
        }

        private boolean check(TokenType type) {
            return current.type() == type;
        }

        private void advance() throws ExpressionException {
            previous = current;
            current = tokenizer.next();
        }

        private Token previous() {
            return previous;
        }

        private Token previous;

        private void consume(TokenType type, String message) throws ExpressionException {
            if (check(type)) {
                advance();
                return;
            }
            throw new ExpressionException(message);
        }
    }
}
