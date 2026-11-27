package io.kestra.plugin.transform.engine;

import com.amazon.ion.IonStruct;

/** Input row held in memory, with its key extracted once and whether it matched a row of the other side. */
public final class KeyedRow {

    private final IonStruct row;

    // Null when the key is missing.
    private final JoinKeyValue key;

    private boolean matched = false;

    public KeyedRow(IonStruct row, JoinKeyValue key) {
        this.row = row;
        this.key = key;
    }

    public IonStruct getRow() {
        return row;
    }

    public JoinKeyValue getKey() {
        return key;
    }

    public boolean isMatched() {
        return matched;
    }

    public void markMatched() {
        this.matched = true;
    }
}
