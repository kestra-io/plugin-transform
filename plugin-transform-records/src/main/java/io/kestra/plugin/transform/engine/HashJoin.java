package io.kestra.plugin.transform.engine;

import io.kestra.plugin.transform.util.JoinIOHandler;
import io.kestra.plugin.transform.util.JoinSide;
import io.kestra.plugin.transform.util.JoinType;
import io.kestra.plugin.transform.util.TransformException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/** The smaller input is held in memory in a hash table, the other input is streamed. */
public class HashJoin extends AbstractJoinImplementation {

    public HashJoin(
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
        var tableSide = getTableSide();
        var probeSide = tableSide.opposite();

        // Build stage: hash table from the smaller input.
        var tableRows = keyOf(tableSide).index(ioHandler.getInputReader(tableSide));
        var hashTable = new HashMap<JoinKeyValue, List<KeyedRow>>();
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

            if (currentMatches == null) {
                if (keepsUnmatched(probeSide)) {
                    ioHandler.writeSideRow(probeRow, probeSide);
                }
                continue;
            }

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

    private JoinSide getTableSide() {
        var leftSize = ioHandler.getInputCount(JoinSide.LEFT);
        var rightSize = ioHandler.getInputCount(JoinSide.RIGHT);
        return leftSize <= rightSize ? JoinSide.LEFT : JoinSide.RIGHT;
    }
}
