package io.kestra.plugin.transform.jsonata;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;

public interface JSONataInterface {

    @Schema(title = "The JSONata expression to apply on the JSON object")
    @NotNull
    @PluginProperty(group = "main")
    Property<String> getExpression();

    @Schema(
        title = "The maximum number of nested evaluation levels allowed for the JSONata expression",
        description = """
            Bounds how deeply the expression may nest while it is evaluated, which is what caps runaway \
            recursive functions. The limit depends only on the expression, never on how many records are \
            processed: the default of 1000 is far above what ordinary expressions reach, and a batch of \
            1,000,000 records needs no higher a value than a batch of 10 does. Raise it only for \
            expressions with proven deep recursion needs — each level adds frames to the chain traversed \
            by variable lookup, so very high values trade the clean error for a JVM StackOverflowError."""
    )
    @NotNull
    @PluginProperty(group = "main")
    Property<Integer> getMaxDepth();
}
