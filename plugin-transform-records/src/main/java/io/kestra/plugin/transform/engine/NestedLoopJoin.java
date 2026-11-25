package io.kestra.plugin.transform.engine;

import com.amazon.ion.IonStruct;
import com.amazon.ion.IonValue;
import io.kestra.plugin.transform.util.JoinIOHandler;
import io.kestra.plugin.transform.util.JoinSide;
import io.kestra.plugin.transform.util.JoinType;
import io.kestra.plugin.transform.util.TransformException;

import java.util.List;

/**
 * Nested Loop Join implementation.
 * The right input is held in memory, the left input is streamed.
 */
public class NestedLoopJoin extends AbstractJoinImplementation {

    /**
     * Creates a new NestedLoopJoin.
     * @param ioHandler JoinIOHandler instance.
     * @param leftKey Join key of left input.
     * @param rightKey Join key of right input.
     * @param joinType Join type.
     * @param onlyFirstMatchOnSemi Option to keep only the first match in the output on semi joins.
     */
    public NestedLoopJoin(
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
        List<KeyedRow> rightRows = rightKey.index(ioHandler.getInputReader(JoinSide.RIGHT));

        for (IonStruct leftRow : ioHandler.getInputReader(JoinSide.LEFT)) {
            IonValue leftKeyValue = leftKey.extractKeyValue(leftRow);
            boolean leftMatched = false;

            if (isJoinable(leftKeyValue)) {
                for (KeyedRow rightRow : rightRows) {
                    if (isJoinable(rightRow.getKey()) && leftKeyValue.equals(rightRow.getKey())) {
                        emitMatch(leftRow, rightRow.getRow(), leftMatched, rightRow.isMatched());
                        rightRow.markMatched();
                        leftMatched = true;
                    }
                }
            }

            if (!leftMatched && keepsUnmatched(JoinSide.LEFT)) {
                ioHandler.writeSideRow(leftRow, JoinSide.LEFT);
            }
        }

        emitUnmatched(rightRows, JoinSide.RIGHT);
    }
}
