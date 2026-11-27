package io.kestra.plugin.transform.engine;

import com.amazon.ion.IonStruct;
import com.amazon.ion.IonValue;
import io.kestra.plugin.transform.ion.IonValueUtils;
import io.kestra.plugin.transform.util.JoinIOHandler;
import io.kestra.plugin.transform.util.JoinImplementation;
import io.kestra.plugin.transform.util.JoinSide;
import io.kestra.plugin.transform.util.JoinType;
import io.kestra.plugin.transform.util.TransformException;

import java.util.List;

/**
 * Base of join implementations (algorithms): common state and rules of the join types.
 * All implementations return the same rows, possibly in a different order.
 */
public abstract class AbstractJoinImplementation {

    /** JoinIOHandler instance. */
    protected final JoinIOHandler ioHandler;

    /** Join key of left input. */
    protected final JoinKey leftKey;

    /** Join key of right input. */
    protected final JoinKey rightKey;

    /** Join type. */
    protected final JoinType joinType;

    /** Option to keep only the first match in the output on semi joins. */
    protected final boolean onlyFirstMatchOnSemi;

    /**
     * Creates a new join implementation.
     * @param ioHandler JoinIOHandler instance.
     * @param leftKey Join key of left input.
     * @param rightKey Join key of right input.
     * @param joinType Join type.
     * @param onlyFirstMatchOnSemi Option to keep only the first match in the output on semi joins.
     */
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

    /**
     * Creates the join implementation of the given kind.
     * @param implementation Kind of join implementation.
     * @param ioHandler JoinIOHandler instance.
     * @param leftKey Join key of left input.
     * @param rightKey Join key of right input.
     * @param joinType Join type.
     * @param onlyFirstMatchOnSemi Option to keep only the first match in the output on semi joins.
     * @return The join implementation.
     */
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

    /**
     * Performs the join.
     * @throws TransformException If a key cannot be extracted or a row cannot be read or written.
     */
    public abstract void join() throws TransformException;

    /**
     * Get the join key of a side.
     * @param side The side (left or right).
     * @return The join key.
     */
    protected JoinKey keyOf(JoinSide side) {
        return side == JoinSide.LEFT ? leftKey : rightKey;
    }

    /**
     * Whether a key value can match: a missing or null key never matches.
     * @param key The key value.
     * @return True if the key can match.
     */
    protected static boolean isJoinable(IonValue key) {
        return !IonValueUtils.isNull(key);
    }

    /**
     * Whether unmatched rows of a side are part of the output (outer and anti joins).
     * @param side The side (left or right).
     * @return True if unmatched rows of the side are written.
     */
    protected boolean keepsUnmatched(JoinSide side) {
        return switch (joinType) {
            case FULL_OUTER -> true;
            case LEFT_OUTER, LEFT_ANTI -> side == JoinSide.LEFT;
            case RIGHT_OUTER, RIGHT_ANTI -> side == JoinSide.RIGHT;
            default -> false;
        };
    }

    /**
     * Writes the output of a matched pair of rows, according to the join type.
     * The caller marks both rows as matched afterwards.
     * @param leftRow Row from left input.
     * @param rightRow Row from right input.
     * @param leftAlreadyMatched True if the left row was matched before this pair.
     * @param rightAlreadyMatched True if the right row was matched before this pair.
     * @throws TransformException If the row cannot be written.
     */
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

    /**
     * Writes the unmatched rows of a side, if the join type keeps them.
     * @param rows Rows of the side.
     * @param side The side (left or right).
     * @throws TransformException If a row cannot be written.
     */
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
