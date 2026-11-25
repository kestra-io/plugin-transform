package io.kestra.plugin.transform.util;

/**
 * Join implementation (algorithm) of the Join task.
 */
public enum JoinImplementation {
    HASH,
    NESTED_LOOP,
    SORTED_MERGE
}
