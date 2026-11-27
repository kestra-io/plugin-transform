package io.kestra.plugin.transform.engine;

import com.amazon.ion.IonBool;
import com.amazon.ion.IonDecimal;
import com.amazon.ion.IonFloat;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonString;
import com.amazon.ion.IonTimestamp;
import com.amazon.ion.IonValue;
import io.kestra.plugin.transform.util.JoinIOHandler;
import io.kestra.plugin.transform.util.JoinMessages;
import io.kestra.plugin.transform.util.JoinSide;
import io.kestra.plugin.transform.util.JoinType;
import io.kestra.plugin.transform.util.TransformException;

import java.util.Comparator;
import java.util.List;

/**
 * Sorted Merge Join implementation.
 * Both inputs are held in memory and sorted on the key.
 */
public class SortedMergeJoin extends AbstractJoinImplementation {

    /**
     * Creates a new SortedMergeJoin.
     * @param ioHandler JoinIOHandler instance.
     * @param leftKey Join key of left input.
     * @param rightKey Join key of right input.
     * @param joinType Join type.
     * @param onlyFirstMatchOnSemi Option to keep only the first match in the output on semi joins.
     */
    public SortedMergeJoin(
        JoinIOHandler ioHandler,
        JoinKey leftKey,
        JoinKey rightKey,
        JoinType joinType,
        boolean onlyFirstMatchOnSemi
    ) {
        super(ioHandler, leftKey, rightKey, joinType, onlyFirstMatchOnSemi);
    }

    /** {@inheritDoc} */
    @Override
    public void join() throws TransformException {
        var sortedLeft = leftKey.index(ioHandler.getInputReader(JoinSide.LEFT));
        var sortedRight = rightKey.index(ioHandler.getInputReader(JoinSide.RIGHT));

        // Checked before sorting, so that the comparator cannot fail.
        checkSameKeyType(sortedLeft, sortedRight);

        Comparator<KeyedRow> byKey = (a, b) -> compareIonValues(a.getKey(), b.getKey());
        sortedLeft.sort(byKey);
        sortedRight.sort(byKey);

        mergeLoop(sortedLeft, sortedRight);
    }

    /**
     * Merge stage for sorted merge join: loop over sorted inputs to search matches (and writing results).
     * @param sortedLeft Sorted rows from left input.
     * @param sortedRight Sorted rows from right input.
     * @throws TransformException If a row cannot be written.
     */
    private void mergeLoop(List<KeyedRow> sortedLeft, List<KeyedRow> sortedRight) throws TransformException {
        var l = 0;
        var r = 0;
        var leftSize = sortedLeft.size();
        var rightSize = sortedRight.size();

        while (l < leftSize && r < rightSize) {
            var leftValue = sortedLeft.get(l).getKey();
            var rightValue = sortedRight.get(r).getKey();

            var leftToRightCompare = compareIonValues(leftValue, rightValue);

            // Mismatch from Left
            if (leftToRightCompare < 0) {
                if (keepsUnmatched(JoinSide.LEFT)) {
                    ioHandler.writeSideRow(sortedLeft.get(l).getRow(), JoinSide.LEFT);
                }
                l++;
                continue;
            }

            // Mismatch from Right
            if (leftToRightCompare > 0) {
                if (keepsUnmatched(JoinSide.RIGHT)) {
                    ioHandler.writeSideRow(sortedRight.get(r).getRow(), JoinSide.RIGHT);
                }
                r++;
                continue;
            }

            // Equality means a match (except for null values): find the groups of equal keys on both sides
            var l0 = l;
            var r0 = r;
            while (l < leftSize && compareIonValues(leftValue, sortedLeft.get(l).getKey()) == 0) l++;
            while (r < rightSize && compareIonValues(rightValue, sortedRight.get(r).getKey()) == 0) r++;

            // Forced mismatch for null values on both sides
            if (!isJoinable(leftValue)) {
                emitUnmatched(sortedLeft.subList(l0, l), JoinSide.LEFT);
                emitUnmatched(sortedRight.subList(r0, r), JoinSide.RIGHT);
                continue;
            }

            // True match
            for (var i = l0; i < l; i++) {
                var leftRow = sortedLeft.get(i);
                for (var j = r0; j < r; j++) {
                    var rightRow = sortedRight.get(j);
                    emitMatch(leftRow.getRow(), rightRow.getRow(), leftRow.isMatched(), rightRow.isMatched());
                    leftRow.markMatched();
                    rightRow.markMatched();
                }
            }
        }

        // Remaining non-matching rows from both sides
        emitUnmatched(sortedLeft.subList(l, leftSize), JoinSide.LEFT);
        emitUnmatched(sortedRight.subList(r, rightSize), JoinSide.RIGHT);
    }

    /**
     * Checks that all non-null key values of both inputs have the same type.
     * @param left Rows from left input.
     * @param right Rows from right input.
     * @throws TransformException If two key values have different types.
     */
    private void checkSameKeyType(List<KeyedRow> left, List<KeyedRow> right) throws TransformException {
        IonValue reference = null;
        for (var rows : List.of(left, right)) {
            for (var row : rows) {
                var key = row.getKey();
                if (!isJoinable(key)) {
                    continue;
                }
                if (reference == null) {
                    reference = key;
                } else if (reference.getType() != key.getType()) {
                    throw new TransformException(String.format(JoinMessages.DIFFERENT_TYPES, reference, key));
                }
            }
        }
    }

    /**
     * Compares two Ion values based on their type and return an integer representing their order relationship (Comparator pattern).
     * Null values are sorted last. Values must have the same processable type (see {@link #checkSameKeyType}).
     * @param va The Ion value to compares to.
     * @param vb The Ion value to be compared.
     * @return A negative value if the first value is lesser than the second, positive if greater, and 0 if both are equals.
     */
    private static int compareIonValues(IonValue va, IonValue vb) {
        var vaNull = !isJoinable(va);
        var vbNull = !isJoinable(vb);
        if (vaNull && vbNull) return 0;
        if (vaNull) return 1;
        if (vbNull) return -1;

        if (va.getType() != vb.getType()) {
            throw new IllegalStateException(String.format(JoinMessages.DIFFERENT_TYPES, va, vb));
        }

        return switch (va.getType()) {
            case BOOL -> Boolean.compare(((IonBool) va).booleanValue(), ((IonBool) vb).booleanValue());
            case INT -> ((IonInt) va).bigIntegerValue().compareTo(((IonInt) vb).bigIntegerValue());
            case FLOAT -> Double.compare(((IonFloat) va).doubleValue(), ((IonFloat) vb).doubleValue());
            case DECIMAL -> ((IonDecimal) va).decimalValue().compareTo(((IonDecimal) vb).decimalValue());
            case STRING -> ((IonString) va).stringValue().compareTo(((IonString) vb).stringValue());
            case TIMESTAMP -> ((IonTimestamp) va).timestampValue().compareTo(((IonTimestamp) vb).timestampValue());
            default -> throw new IllegalStateException(JoinMessages.UNPROCESSABLE_COMPARED_TYPES);
        };
    }
}
