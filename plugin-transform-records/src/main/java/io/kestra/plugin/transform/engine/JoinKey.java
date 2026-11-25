package io.kestra.plugin.transform.engine;

import com.amazon.ion.IonStruct;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import io.kestra.plugin.transform.ion.IonValueUtils;
import io.kestra.plugin.transform.util.JoinIOHandler;
import io.kestra.plugin.transform.util.JoinMessages;
import io.kestra.plugin.transform.util.JoinSide;
import io.kestra.plugin.transform.util.MissingKeyAction;
import io.kestra.plugin.transform.util.TransformException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * Abstraction of join key providing a data structure for nested fields, side and method for extraction.
 */
public final class JoinKey {

    /** ION types of key values that can be processed. */
    private static final Set<IonType> PROCESSABLE_TYPES = EnumSet.of(
        IonType.BOOL,
        IonType.DECIMAL,
        IonType.FLOAT,
        IonType.INT,
        IonType.STRING,
        IonType.TIMESTAMP
    );

    /** String of the key used as input, which can be the field name or a nested key path. */
    private final String keyString;

    /** Side of the key. */
    private final JoinSide keySide;

    /**
     * Path of the key.
     * Each item is a field name forming the path of nested key.
     */
    private final List<String> keyPath;

    /** Action when key is missing. */
    private final MissingKeyAction onMissingKey;

    /** JoinIOHandler instance. */
    private final JoinIOHandler ioHandler;

    /** If key is missing in at least one row. */
    private boolean isMissing = false;

    /**
     * Creates a new JoinKey.
     * @param keyString String of the key used as input, which can be the field name or a nested key path.
     * @param keySide Side of the key.
     * @param onMissingKey Action when key is missing.
     * @param ioHandler JoinIOHandler instance.
     */
    public JoinKey(String keyString,
        JoinSide keySide,
        MissingKeyAction onMissingKey,
        JoinIOHandler ioHandler) {

        this.keyString = keyString;
        this.keySide = keySide;
        this.onMissingKey = onMissingKey;
        this.ioHandler = ioHandler;

        // Split of key string with . character used as separator for nested key path.
        this.keyPath = Arrays.stream(keyString.split("\\.")).map(String::trim).toList();
    }

    /**
     * Get the key string.
     * @return String of the key used as input, which can be the field name or a nested key path.
     */
    public String getKeyString() {
        return keyString;
    }

    /**
     * Extracts value from a row by unique or nested key.
     * @param row The row where to extracts the value.
     * @return The corresponding Ion value, null or Ion null when the key is missing and not failing.
     * @throws TransformException If the key is missing (FAIL action), unreachable, or has an unprocessable value.
     */
    public IonValue extractKeyValue(IonStruct row) throws TransformException {
        return traverseRow(row, 0);
    }

    /**
     * Extracts the key value of each row, once.
     * @param rows The rows of an input.
     * @return A mutable list of keyed rows, in input order.
     * @throws TransformException If a key value cannot be extracted.
     */
    public List<KeyedRow> index(Iterable<IonStruct> rows) throws TransformException {
        List<KeyedRow> keyedRows = new ArrayList<>();
        for (IonStruct row : rows) {
            keyedRows.add(new KeyedRow(row, extractKeyValue(row)));
        }
        return keyedRows;
    }

    /**
     * Recursive function to get the key value in a simple or nested row.
     * @param row The row where to extracts the value.
     * @param depth Current depth of the traversing.
     * @return The final Ion value.
     * @throws TransformException If the value cannot be extracted.
     */
    private IonValue traverseRow(IonStruct row, int depth) throws TransformException {
        IonValue val = row.get(keyPath.get(depth));
        if (depth < keyPath.size() - 1) {
            if (val != null && val.getType() == IonType.STRUCT) {
                return traverseRow((IonStruct) val, depth + 1);
            }
            throw new TransformException(String.format(JoinMessages.UNREACHABLE_KEY_DEPTH, keyString, keySide));
        }
        if (IonValueUtils.isNull(val)) {
            handleMissingKey(row);
            return val;
        }
        if (val.getType() == IonType.STRUCT) {
            throw new TransformException(String.format(JoinMessages.KEY_VALUE_IS_OBJECT, keyString, keySide));
        }
        if (!PROCESSABLE_TYPES.contains(val.getType())) {
            throw new TransformException(String.format(JoinMessages.UNPROCESSABLE_KEY_TYPE, keyString, keySide));
        }
        return val;
    }

    /**
     * Applies the missing key action. A warning is logged only for the first missing key.
     * @param row The row where the key is missing.
     * @throws TransformException If the action is FAIL.
     */
    private void handleMissingKey(IonStruct row) throws TransformException {
        String message = String.format(JoinMessages.MISSING_KEY, keyString, keySide, row);
        switch (onMissingKey) {
            case FAIL -> throw new TransformException(message);
            case WARNING -> {
                ioHandler.markWarning();
                warnOnce(message);
            }
            case IGNORE -> warnOnce(message);
        }
    }

    /**
     * Logs a warning if no key was missing before.
     * @param message The warning message.
     */
    private void warnOnce(String message) {
        if (!isMissing) {
            ioHandler.warn(message);
            isMissing = true;
        }
    }
}
