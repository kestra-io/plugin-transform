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

public final class JoinKey {

    private static final Set<IonType> PROCESSABLE_TYPES = EnumSet.of(
        IonType.BOOL,
        IonType.DECIMAL,
        IonType.FLOAT,
        IonType.INT,
        IonType.STRING,
        IonType.TIMESTAMP
    );

    private final String keyString;
    private final JoinSide keySide;
    private final List<String> keyPath;
    private final MissingKeyAction onMissingKey;
    private final JoinIOHandler ioHandler;

    // True once a missing key has been reported, so that the warning is logged only once.
    private boolean isMissing = false;

    public JoinKey(String keyString,
        JoinSide keySide,
        MissingKeyAction onMissingKey,
        JoinIOHandler ioHandler) {

        this.keyString = keyString;
        this.keySide = keySide;
        this.onMissingKey = onMissingKey;
        this.ioHandler = ioHandler;

        this.keyPath = Arrays.stream(keyString.split("\\.")).map(String::trim).toList();
    }

    public String getKeyString() {
        return keyString;
    }

    /** Returns null when the key is missing and the missing key action does not fail. */
    public JoinKeyValue extractKeyValue(IonStruct row) throws TransformException {
        var value = traverseRow(row, 0);
        return IonValueUtils.isNull(value) ? null : JoinKeyValue.of(value);
    }

    /** Returns a mutable list, in input order, so that it can be sorted in place. */
    public List<KeyedRow> index(Iterable<IonStruct> rows) throws TransformException {
        var keyedRows = new ArrayList<KeyedRow>();
        for (var row : rows) {
            keyedRows.add(new KeyedRow(row, extractKeyValue(row)));
        }
        return keyedRows;
    }

    private IonValue traverseRow(IonStruct row, int depth) throws TransformException {
        var val = row.get(keyPath.get(depth));
        if (depth < keyPath.size() - 1) {
            // An absent or null parent object makes the key missing; only a non-object parent is an error.
            if (IonValueUtils.isNull(val)) {
                handleMissingKey(row);
                return null;
            }
            if (val.getType() == IonType.STRUCT) {
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

    private void handleMissingKey(IonStruct row) throws TransformException {
        var message = String.format(JoinMessages.MISSING_KEY, keyString, keySide, row);
        switch (onMissingKey) {
            case FAIL -> throw new TransformException(message);
            case WARNING -> {
                ioHandler.markWarning();
                warnOnce(message);
            }
            case IGNORE -> warnOnce(message);
        }
    }

    private void warnOnce(String message) {
        if (!isMissing) {
            ioHandler.warn(message);
            isMissing = true;
        }
    }
}
