package io.kestra.plugin.transform.grok;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;

import java.util.List;
import java.util.Map;

public interface GrokInterface {

    @PluginProperty
    @Schema(title = "The Grok pattern to match.")
    String getPattern();

    @PluginProperty
    @Schema(title = "The list of Grok patterns to match.")
    List<String> getPatterns();

    @PluginProperty
    @Schema(
        title = "List of user-defined pattern directories.",
        description = "Directories must be paths relative to the working directory."
    )
    List<String> getPatternsDir();

    @PluginProperty
    @Schema(
        title = "Custom pattern definitions.",
        description = "A map of pattern-name and pattern pairs defining custom patterns to be used by the current tasks. Patterns matching existing names will override the pre-existing definition. "
    )
    Map<String, String> getPatternDefinitions();

    @PluginProperty
    @Schema(title = "If `true`, only store named captures from grok.")
    boolean isNamedCapturesOnly();

    @PluginProperty
    @Schema(
        title = "If `true`, break on first match.",
        description = "The first successful match by grok will result in the task being finished. Set to `false` if you want the task to try all configured patterns."
    )
    boolean isBreakOnFirstMatch();

    @PluginProperty
    @Schema(
        title = "If `true`, keep empty captures.",
        description = "When an optional field cannot be captured, the empty field is retained in the output. Set `false` if you want empty optional fields to be filtered out."
    )
    boolean isKeepEmptyCaptures();
}
