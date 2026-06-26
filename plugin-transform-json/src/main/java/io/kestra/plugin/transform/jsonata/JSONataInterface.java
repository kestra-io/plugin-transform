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
        title = "The maximum number of recursive calls allowed for the JSONata transformation",
        description = "Limits recursive JSONata function call depth. Each recursive call adds a frame to the chain traversed by variable lookup, so high values can cause a JVM StackOverflowError on platforms with small default thread stacks (e.g. Windows ~256 KB vs Linux ~512 KB). Raise only for expressions with proven deep recursion needs."
    )
    @NotNull
    @PluginProperty(group = "main")
    Property<Integer> getMaxDepth();
}
