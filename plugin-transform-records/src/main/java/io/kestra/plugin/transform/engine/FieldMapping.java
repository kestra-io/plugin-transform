package io.kestra.plugin.transform.engine;

import io.kestra.plugin.transform.ion.IonTypeName;

public record FieldMapping(
    String targetField,
    String expression,
    IonTypeName type,
    boolean optional
) {
}
