package io.kestra.plugin.transform.util;

public record TransformOptions(
    boolean keepOriginalFields,
    boolean dropNulls,
    OnErrorMode onError
) {
    public enum OnErrorMode {
        FAIL,
        SKIP,
        NULL
    }
}
