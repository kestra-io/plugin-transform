package io.kestra.plugin.transform.util;

/**
 * How the Join task lays out the fields of both sides in an output row.
 */
public enum MergeMode {
    /** Fields are merged at the top level; on a name conflict, the right value is kept. */
    OVERWRITE,
    /** Field names are prefixed with the side label and the prefix separator. */
    PREFIX,
    /** Fields of each side are nested in an object named after the side label. */
    NEST
}
