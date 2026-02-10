package io.kestra.plugin.transform.engine;

import io.swagger.v3.oas.annotations.media.Schema;

public record TransformStats(
    @Schema(title = "Processed records")
    int processed,
    @Schema(title = "Failed records")
    int failed,
    @Schema(title = "Dropped records")
    int dropped,
    @Schema(title = "Field-level errors")
    java.util.Map<String, String> fieldErrors
) {
}
