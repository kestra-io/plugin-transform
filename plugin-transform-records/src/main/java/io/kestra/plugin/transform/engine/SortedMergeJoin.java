package io.kestra.plugin.transform.engine;

import io.kestra.plugin.transform.util.JoinIOHandler;
import io.kestra.plugin.transform.util.JoinMessages;
import io.kestra.plugin.transform.util.JoinSide;
import io.kestra.plugin.transform.util.JoinType;
import io.kestra.plugin.transform.util.TransformException;

import java.util.Comparator;
import java.util.List;

/** Both inputs are held in memory and sorted on the key. */
public class SortedMergeJoin extends AbstractJoinImplementation {

    // Null (missing) keys are sorted last. Keys must have the same type, see checkSameKeyType.
    private static final Comparator<JoinKeyValue> KEY_ORDER = Comparator.nullsLast(Comparator.naturalOrder());

    public SortedMergeJoin(
        JoinIOHandler ioHandler,
        JoinKey leftKey,
        JoinKey rightKey,
        JoinType joinType,
        boolean onlyFirstMatchOnSemi
    ) {
        super(ioHandler, leftKey, rightKey, joinType, onlyFirstMatchOnSemi);
    }

    @Override
    public void join() throws TransformException {
        var sortedLeft = leftKey.index(ioHandler.getInputReader(JoinSide.LEFT));
        var sortedRight = rightKey.index(ioHandler.getInputReader(JoinSide.RIGHT));

        // Checked before sorting, so that the comparator cannot fail.
        checkSameKeyType(sortedLeft, sortedRight);

        Comparator<KeyedRow> byKey = Comparator.comparing(KeyedRow::getKey, KEY_ORDER);
        sortedLeft.sort(byKey);
        sortedRight.sort(byKey);

        mergeLoop(sortedLeft, sortedRight);
    }

    private void mergeLoop(List<KeyedRow> sortedLeft, List<KeyedRow> sortedRight) throws TransformException {
        var l = 0;
        var r = 0;
        var leftSize = sortedLeft.size();
        var rightSize = sortedRight.size();

        while (l < leftSize && r < rightSize) {
            var leftValue = sortedLeft.get(l).getKey();
            var rightValue = sortedRight.get(r).getKey();

            var leftToRightCompare = KEY_ORDER.compare(leftValue, rightValue);

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
            while (l < leftSize && KEY_ORDER.compare(leftValue, sortedLeft.get(l).getKey()) == 0) l++;
            while (r < rightSize && KEY_ORDER.compare(rightValue, sortedRight.get(r).getKey()) == 0) r++;

            // Forced mismatch for null values on both sides
            if (!isJoinable(leftValue)) {
                emitUnmatched(sortedLeft.subList(l0, l), JoinSide.LEFT);
                emitUnmatched(sortedRight.subList(r0, r), JoinSide.RIGHT);
                continue;
            }

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

        emitUnmatched(sortedLeft.subList(l, leftSize), JoinSide.LEFT);
        emitUnmatched(sortedRight.subList(r, rightSize), JoinSide.RIGHT);
    }

    private void checkSameKeyType(List<KeyedRow> left, List<KeyedRow> right) throws TransformException {
        JoinKeyValue reference = null;
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
}
