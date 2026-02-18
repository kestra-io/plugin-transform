package io.kestra.plugin.transform.engine;

import com.amazon.ion.IonStruct;

import java.util.List;

public record TransformResult(
    List<IonStruct> records,
    TransformStats stats
) {
}
