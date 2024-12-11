package io.kestra.plugin.transform.jsonata;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;

public interface JSONataInterface {

    @Schema(title = "The JSONata expression to apply on the JSON object.")
    @NotNull
    Property<String> getExpression();

    @Schema(title = "The maximum number of recursive calls allowed for the JSONata transformation.")
    @NotNull
    Property<Integer> getMaxDepth();
}
