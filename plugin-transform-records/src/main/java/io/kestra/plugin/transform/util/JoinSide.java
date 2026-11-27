package io.kestra.plugin.transform.util;

public enum JoinSide {
    LEFT,
    RIGHT;

    public JoinSide opposite() {
        return this == LEFT ? RIGHT : LEFT;
    }
}
