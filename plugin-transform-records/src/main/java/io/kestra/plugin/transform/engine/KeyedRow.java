package io.kestra.plugin.transform.engine;

import com.amazon.ion.IonStruct;
import com.amazon.ion.IonValue;

/**
 * Row of an input held in memory by a join implementation, with its key value extracted once
 * and a match metadata to keep trace of join matches.
 */
public final class KeyedRow {

    /** ION struct of row data. */
    private final IonStruct row;

    /** Key value of the row, null or Ion null when the key is missing. */
    private final IonValue key;

    /** True if this row is matched with at least one row from other side. */
    private boolean matched = false;

    /**
     * Creates a new KeyedRow.
     * @param row ION struct of row data.
     * @param key Key value of the row.
     */
    public KeyedRow(IonStruct row, IonValue key) {
        this.row = row;
        this.key = key;
    }

    /**
     * Get row data.
     * @return ION struct of row data.
     */
    public IonStruct getRow() {
        return row;
    }

    /**
     * Get key value.
     * @return Key value of the row, null or Ion null when the key is missing.
     */
    public IonValue getKey() {
        return key;
    }

    /**
     * Get match metadata.
     * @return True if this row is matched with at least one row from other side.
     */
    public boolean isMatched() {
        return matched;
    }

    /**
     * Sets match metadata as true.
     */
    public void markMatched() {
        this.matched = true;
    }
}
