package io.kestra.plugin.transform.engine;

import com.amazon.ion.IonValue;
import io.kestra.plugin.transform.util.JoinIOHandler;
import io.kestra.plugin.transform.util.JoinSide;
import io.kestra.plugin.transform.util.JoinType;
import io.kestra.plugin.transform.util.TransformException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Hash Join implementation.
 * The smaller input is held in memory in a hash table, the other input is streamed.
 */
public class HashJoin extends AbstractJoinImplementation {

    /**
     * Creates a new HashJoin.
     * @param ioHandler JoinIOHandler instance.
     * @param leftKey Join key of left input.
     * @param rightKey Join key of right input.
     * @param joinType Join type.
     * @param onlyFirstMatchOnSemi Option to keep only the first match in the output on semi joins.
     */
    public HashJoin(
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
        var tableSide = getTableSide();
        var probeSide = tableSide.opposite();

        // Build stage: hash table from the smaller input.
        var tableRows = keyOf(tableSide).index(ioHandler.getInputReader(tableSide));
        var hashTable = new HashMap<IonValue, List<KeyedRow>>();
        for (var tableRow : tableRows) {
            if (isJoinable(tableRow.getKey())) {
                hashTable.computeIfAbsent(tableRow.getKey(), k -> new ArrayList<>()).add(tableRow);
            }
        }

        // Probe stage: streaming the opposite input to search matches (and writing results).
        var probeKey = keyOf(probeSide);
        for (var probeRow : ioHandler.getInputReader(probeSide)) {
            var keyValue = probeKey.extractKeyValue(probeRow);
            var currentMatches = isJoinable(keyValue) ? hashTable.get(keyValue) : null;

            // Writes mismatched row of probe side for outer and anti joins
            if (currentMatches == null) {
                if (keepsUnmatched(probeSide)) {
                    ioHandler.writeSideRow(probeRow, probeSide);
                }
                continue;
            }

            // Writes all matched rows in hash table for current row of probe side
            var probeMatched = false;
            for (var tableRow : currentMatches) {
                if (tableSide == JoinSide.LEFT) {
                    emitMatch(tableRow.getRow(), probeRow, tableRow.isMatched(), probeMatched);
                } else {
                    emitMatch(probeRow, tableRow.getRow(), probeMatched, tableRow.isMatched());
                }
                tableRow.markMatched();
                probeMatched = true;
            }
        }

        // Final stage for outer and anti joins: remaining mismatched rows of the hash table.
        emitUnmatched(tableRows, tableSide);
    }

    /**
     * Get the side of the best input to build the hash table for hash join (i.e. the smaller).
     * @return The side (left or right).
     */
    private JoinSide getTableSide() {
        var leftSize = ioHandler.getInputCount(JoinSide.LEFT);
        var rightSize = ioHandler.getInputCount(JoinSide.RIGHT);
        return leftSize <= rightSize ? JoinSide.LEFT : JoinSide.RIGHT;
    }
}
