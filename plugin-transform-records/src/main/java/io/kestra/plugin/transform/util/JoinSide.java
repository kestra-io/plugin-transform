package io.kestra.plugin.transform.util;

/**
 * Side of an input of the Join task.
 */
public enum JoinSide {
    LEFT,
    RIGHT;

    /** The other side. */
    public JoinSide opposite() {
        return this == LEFT ? RIGHT : LEFT;
    }
}
