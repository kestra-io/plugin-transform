package io.kestra.plugin.transform.engine;

import com.amazon.ion.IonStruct;
import io.kestra.plugin.transform.util.JoinIOHandler;
import io.kestra.plugin.transform.util.JoinImplementation;
import io.kestra.plugin.transform.util.JoinSide;
import io.kestra.plugin.transform.util.JoinType;
import io.kestra.plugin.transform.util.TransformException;

import java.util.List;

/** All implementations return the same rows, possibly in a different order. */
public abstract class AbstractJoinImplementation {

    protected final JoinIOHandler ioHandler;
    protected final JoinKey leftKey;
    protected final JoinKey rightKey;
    protected final JoinType joinType;
    protected final boolean onlyFirstMatchOnSemi;

    protected AbstractJoinImplementation(
        JoinIOHandler ioHandler,
        JoinKey leftKey,
        JoinKey rightKey,
        JoinType joinType,
        boolean onlyFirstMatchOnSemi
    ) {
        this.ioHandler = ioHandler;
        this.leftKey = leftKey;
        this.rightKey = rightKey;
        this.joinType = joinType;
        this.onlyFirstMatchOnSemi = onlyFirstMatchOnSemi;
    }

    public static AbstractJoinImplementation of(
        JoinImplementation implementation,
        JoinIOHandler ioHandler,
        JoinKey leftKey,
        JoinKey rightKey,
        JoinType joinType,
        boolean onlyFirstMatchOnSemi
    ) {
        return switch (implementation) {
            case HASH -> new HashJoin(ioHandler, leftKey, rightKey, joinType, onlyFirstMatchOnSemi);
            case NESTED_LOOP -> new NestedLoopJoin(ioHandler, leftKey, rightKey, joinType, onlyFirstMatchOnSemi);
            case SORTED_MERGE -> new SortedMergeJoin(ioHandler, leftKey, rightKey, joinType, onlyFirstMatchOnSemi);
        };
    }

    public abstract void join() throws TransformException;

    protected JoinKey keyOf(JoinSide side) {
        return side == JoinSide.LEFT ? leftKey : rightKey;
    }

    /** A missing or null key never matches. */
    protected static boolean isJoinable(JoinKeyValue key) {
        return key != null;
    }

    protected boolean keepsUnmatched(JoinSide side) {
        return switch (joinType) {
            case FULL_OUTER -> true;
            case LEFT_OUTER, LEFT_ANTI -> side == JoinSide.LEFT;
            case RIGHT_OUTER, RIGHT_ANTI -> side == JoinSide.RIGHT;
            default -> false;
        };
    }

    /** The caller marks both rows as matched afterwards. */
    protected void emitMatch(IonStruct leftRow, IonStruct rightRow, boolean leftAlreadyMatched, boolean rightAlreadyMatched) throws TransformException {
        switch (joinType) {
            case INNER, FULL_OUTER, LEFT_OUTER, RIGHT_OUTER -> ioHandler.writeMergedRow(leftRow, rightRow);
            case LEFT_SEMI -> {
                if (!onlyFirstMatchOnSemi || !leftAlreadyMatched) {
                    ioHandler.writeSideRow(leftRow, JoinSide.LEFT);
                }
            }
            case RIGHT_SEMI -> {
                if (!onlyFirstMatchOnSemi || !rightAlreadyMatched) {
                    ioHandler.writeSideRow(rightRow, JoinSide.RIGHT);
                }
            }
            default -> {
                // Anti joins write nothing on a match.
            }
        }
    }

    protected void emitUnmatched(List<KeyedRow> rows, JoinSide side) throws TransformException {
        if (!keepsUnmatched(side)) {
            return;
        }
        for (var row : rows) {
            if (!row.isMatched()) {
                ioHandler.writeSideRow(row.getRow(), side);
            }
        }
    }
}
