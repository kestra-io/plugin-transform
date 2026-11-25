package io.kestra.plugin.transform.util;

/**
 * Action of the Join task when a key field is missing from an input row.
 */
public enum MissingKeyAction {
    FAIL,
    WARNING,
    IGNORE
}
