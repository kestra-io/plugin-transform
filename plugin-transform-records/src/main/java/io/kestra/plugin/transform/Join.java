package io.kestra.plugin.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.transform.engine.AbstractJoinImplementation;
import io.kestra.plugin.transform.engine.JoinKey;
import io.kestra.plugin.transform.util.JoinIOHandler;
import io.kestra.plugin.transform.util.JoinImplementation;
import io.kestra.plugin.transform.util.JoinMessages;
import io.kestra.plugin.transform.util.JoinSide;
import io.kestra.plugin.transform.util.JoinType;
import io.kestra.plugin.transform.util.MergeMode;
import io.kestra.plugin.transform.util.MissingKeyAction;
import io.kestra.plugin.transform.util.OutputFormat;
import io.kestra.plugin.transform.util.TransformException;
import io.kestra.plugin.transform.util.TransformTaskSupport;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.net.URI;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Join two record sets on a key",
    description = """
        Apply inner, outer, anti or semi joins on two inputs (records or Ion files), matching rows whose key values are equal.
        Keys of different types never match; decimals are compared by value (`1.0` equals `1.00`) and timestamps by instant, whatever their offset or precision."""
)
@Plugin(
    examples = {
        @Example(
            title = "Left outer join of in-memory records",
            full = true,
            code = """
                id: join_records
                namespace: company.team

                tasks:
                  - id: customers
                    type: io.kestra.plugin.core.output.OutputValues
                    values:
                      records:
                        - customer_id: 1
                          name: Alice
                        - customer_id: 2
                          name: Bob

                  - id: orders
                    type: io.kestra.plugin.core.output.OutputValues
                    values:
                      records:
                        - order_id: 101
                          customer_id: 1
                          total: 25.5

                  - id: join
                    type: io.kestra.plugin.transform.Join
                    left: "{{ outputs.customers.values.records }}"
                    right: "{{ outputs.orders.values.records }}"
                    leftKey: customer_id
                    rightKey: customer_id
                    joinType: LEFT_OUTER
                """
        ),
        @Example(
            title = "Inner join of customers and their orders",
            full = true,
            code = """
                id: join_customers_orders
                namespace: company.team

                tasks:
                  - id: customers
                    type: io.kestra.plugin.core.storage.Write
                    extension: .ion
                    content: |
                      {customer_id:1,name:"Alice"}
                      {customer_id:2,name:"Bob"}
                      {customer_id:3,name:"Carol"}

                  - id: orders
                    type: io.kestra.plugin.core.storage.Write
                    extension: .ion
                    content: |
                      {order_id:101,customer_id:1,total:25.50}
                      {order_id:102,customer_id:1,total:12.00}
                      {order_id:103,customer_id:3,total:40.00}
                      {order_id:104,customer_id:4,total:8.75}

                  - id: join
                    type: io.kestra.plugin.transform.Join
                    left: "{{ outputs.customers.uri }}"
                    right: "{{ outputs.orders.uri }}"
                    leftKey: customer_id
                    rightKey: customer_id
                    joinType: INNER
                """
        ),
        @Example(
            title = "Left outer join on a nested key, keeping each side in its own object",
            full = true,
            code = """
                id: join_customers_orders_nested
                namespace: company.team

                tasks:
                  - id: customers
                    type: io.kestra.plugin.core.storage.Write
                    extension: .ion
                    content: |
                      {customer:{id:1},name:"Alice"}
                      {customer:{id:2},name:"Bob"}

                  - id: orders
                    type: io.kestra.plugin.core.storage.Write
                    extension: .ion
                    content: |
                      {order_id:101,customer_id:1,total:25.50}

                  - id: join
                    type: io.kestra.plugin.transform.Join
                    left: "{{ outputs.customers.uri }}"
                    right: "{{ outputs.orders.uri }}"
                    leftKey: customer.id
                    rightKey: customer_id
                    joinType: LEFT_OUTER
                    mergeMode: NEST
                    leftLabel: customer
                    rightLabel: order
                """
        )
    }
)
public class Join extends Task implements RunnableTask<Join.Output> {

    @Schema(
        title = "Left data",
        description = "Records (list or single record), Ion values, or a `kestra://` internal storage URI of an Ion file."
    )
    @PluginProperty(group = "source", internalStorageURI = true)
    @NotNull
    private Property<Object> left;

    @Schema(
        title = "Right data",
        description = "Records (list or single record), Ion values, or a `kestra://` internal storage URI of an Ion file."
    )
    @PluginProperty(group = "source", internalStorageURI = true)
    @NotNull
    private Property<Object> right;

    @Schema(
        title = "Key field name or nested key of left input, that will be compared to the right input (equality)",
        description = "Use the `.` sign to make a nested key (e.g. `object.id`)."
    )
    @PluginProperty(group = "main")
    @NotNull
    private Property<String> leftKey;

    @Schema(
        title = "Key field name or nested key of right input, that will be compared to the left input (equality)",
        description = "Use the `.` sign to make a nested key (e.g. `object.id`)."
    )
    @PluginProperty(group = "main")
    @NotNull
    private Property<String> rightKey;

    @Schema(
        title = "Join implementation (algorithm)",
        description = """
            All implementations return the same rows, possibly in a different order.
            - `HASH` (default): builds a hash table from the smaller input, then streams the other input to find matches. Only the smaller input is held in memory.
            - `NESTED_LOOP`: compares every row of one input with every row of the other; only suited to small inputs. The right input is held in memory.
            - `SORTED_MERGE`: sorts both inputs on the key, then merges them; the key must have the same type in every row of both inputs. Both inputs are held in memory."""
    )
    @PluginProperty(group = "processing")
    @Builder.Default
    private Property<JoinImplementation> joinImplementation = Property.ofValue(JoinImplementation.HASH);

    @Schema(
        title = "Join type",
        description = """
            - `INNER` (default): only rows whose key matches on both sides.
            - `FULL_OUTER`, `LEFT_OUTER`, `RIGHT_OUTER`: matched rows, plus unmatched rows from both sides, the left side or the right side.
            - `LEFT_ANTI`, `RIGHT_ANTI`: only the rows of one side that have no match on the other side.
            - `LEFT_SEMI`, `RIGHT_SEMI`: only the rows of one side that have a match, without fields from the other side (see `onlyFirstMatchOnSemi`)."""
    )
    @PluginProperty(group = "main")
    @Builder.Default
    private Property<JoinType> joinType = Property.ofValue(JoinType.INNER);

    @Schema(
        title = "Merge mode",
        description = """
            Configure output structure to differentiate fields by side.
            - `OVERWRITE` (default): fields of both sides at the top level; on a name conflict, the right value is kept.
            - `PREFIX`: field names are prefixed with `leftLabel` or `rightLabel` and `prefixSeparator`.
            - `NEST`: fields of each side are nested in an object named after `leftLabel` or `rightLabel` values."""
    )
    @PluginProperty(group = "destination")
    @Builder.Default
    private Property<MergeMode> mergeMode = Property.ofValue(MergeMode.OVERWRITE);

    @Schema(
        title = "Label for left fields in output",
        description = "Depending on merge mode, used as prefix in combination with prefixSeparator, or object key for left fields. Must differ from rightLabel."
    )
    @PluginProperty(group = "destination")
    @NotBlank
    @Builder.Default
    private String leftLabel = "left";

    @Schema(
        title = "Label for right fields in output",
        description = "Depending on merge mode, used as prefix in combination with prefixSeparator, or object key for right fields. Must differ from leftLabel."
    )
    @PluginProperty(group = "destination")
    @NotBlank
    @Builder.Default
    private String rightLabel = "right";

    @Schema(
        title = "Separator character for prefixes",
        description = "When merge mode is prefix, will be concatenated between label and field names. Can be blank."
    )
    @PluginProperty(group = "destination")
    @Builder.Default
    private String prefixSeparator = "_";

    @Schema(
        title = "Keep only the first match in the output on semi joins",
        description = "If false, multiple identical rows can appear for each matching occurrence."
    )
    @PluginProperty(group = "processing")
    @Builder.Default
    private Property<Boolean> onlyFirstMatchOnSemi = Property.ofValue(false);

    @Schema(
        title = "What to do when key field is missing from an input (for at least one row)",
        description = """
            - `FAIL` (default): the task fails.
            - `WARNING`: the task continues and ends in `WARNING` state.
            - `IGNORE`: the task continues and its state is not changed."""
    )
    @PluginProperty(group = "reliability")
    @Builder.Default
    private Property<MissingKeyAction> onMissingKey = Property.ofValue(MissingKeyAction.FAIL);

    @Schema(
        title = "Output format",
        description = """
            Experimental: TEXT or BINARY. Only transform tasks can read binary Ion. Use TEXT as the final step.
            """
    )
    @PluginProperty(group = "destination")
    @Builder.Default
    private Property<OutputFormat> outputFormat = Property.ofValue(OutputFormat.TEXT);

    @Schema(
        title = "Output type",
        description = """
            AUTO stores to internal storage when any input is a storage URI; otherwise it returns records.
            """
    )
    @PluginProperty(group = "destination")
    @Builder.Default
    private Property<OutputMode> outputType = Property.ofValue(OutputMode.AUTO);

    @Schema(hidden = true)
    @JsonIgnore
    @AssertTrue(message = "leftLabel and rightLabel must be different")
    public boolean isLabelsDistinct() {
        // A missing label is reported by @NotBlank, not here.
        return leftLabel == null || rightLabel == null || !leftLabel.equals(rightLabel);
    }

    @Override
    public Join.Output run(RunContext runContext) throws Exception {
        var rLeftKey = requireNonBlank(render(leftKey, String.class, "leftKey", runContext), "leftKey");
        var rRightKey = requireNonBlank(render(rightKey, String.class, "rightKey", runContext), "rightKey");
        var rJoinImplementation = render(joinImplementation, JoinImplementation.class, "joinImplementation", runContext);
        var rJoinType = render(joinType, JoinType.class, "joinType", runContext);
        var rMergeMode = render(mergeMode, MergeMode.class, "mergeMode", runContext);
        var rOnlyFirstMatchOnSemi = render(onlyFirstMatchOnSemi, Boolean.class, "onlyFirstMatchOnSemi", runContext);
        var rOnMissingKey = render(onMissingKey, MissingKeyAction.class, "onMissingKey", runContext);
        var rOutputFormat = render(outputFormat, OutputFormat.class, "outputFormat", runContext);
        var rOutputType = render(outputType, OutputMode.class, "outputType", runContext);

        var rLeft = resolveInput(left, "left", runContext);
        var rRight = resolveInput(right, "right", runContext);

        var anyFromStorage = rLeft.fromStorage() || rRight.fromStorage();
        var storeOutput = rOutputType == OutputMode.AUTO ? anyFromStorage : rOutputType == OutputMode.STORE;

        try (var ioHandler = new JoinIOHandler(
            runContext,
            rLeft,
            rRight,
            storeOutput,
            rOutputFormat,
            rMergeMode,
            leftLabel,
            rightLabel,
            prefixSeparator
        )) {

            var leftJoinKey = new JoinKey(rLeftKey, JoinSide.LEFT, rOnMissingKey, ioHandler);
            var rightJoinKey = new JoinKey(rRightKey, JoinSide.RIGHT, rOnMissingKey, ioHandler);

            AbstractJoinImplementation.of(
                rJoinImplementation,
                ioHandler,
                leftJoinKey,
                rightJoinKey,
                rJoinType,
                rOnlyFirstMatchOnSemi
            ).join();

            var result = ioHandler.storeOutputFile();

            return Output.builder()
                .uri(result)
                .records(ioHandler.getRecords())
                .outputSize(ioHandler.getOutputSize())
                .leftSize(ioHandler.getInputCount(JoinSide.LEFT))
                .rightSize(ioHandler.getInputCount(JoinSide.RIGHT))
                .state(ioHandler.getState())
                .build();
        }
    }

    /** A property that renders to nothing is an error, never silently replaced by a default value. */
    private <T> T render(
        Property<T> property,
        Class<T> type,
        String propertyName,
        RunContext runContext) throws TransformException {
        Optional<T> rendered;
        try {
            rendered = runContext.render(property).as(type);
        } catch (Exception e) {
            throw new TransformException(String.format(JoinMessages.RENDER_ERROR, propertyName), e);
        }
        return rendered.orElseThrow(() -> new TransformException(String.format(JoinMessages.BLANK_PROPERTY, propertyName)));
    }

    private TransformTaskSupport.ResolvedInput resolveInput(
        Property<Object> property,
        String propertyName,
        RunContext runContext) throws TransformException {
        TransformTaskSupport.ResolvedInput resolved;
        try {
            resolved = TransformTaskSupport.resolveInput(runContext, property);
        } catch (Exception e) {
            throw new TransformException(String.format(JoinMessages.RENDER_ERROR, propertyName), e);
        }
        if (resolved.value() == null || (resolved.value() instanceof String value && value.isBlank())) {
            throw new TransformException(String.format(JoinMessages.BLANK_PROPERTY, propertyName));
        }
        return resolved;
    }

    private String requireNonBlank(String value, String propertyName) throws TransformException {
        if (value == null || value.isBlank()) {
            throw new TransformException(String.format(JoinMessages.BLANK_PROPERTY, propertyName));
        }
        return value;
    }

    public enum OutputMode {
        AUTO,
        RECORDS,
        STORE;

        @JsonCreator
        public static OutputMode from(Object value) {
            if (value == null) {
                return null;
            }
            var raw = String.valueOf(value).trim();
            return OutputMode.valueOf(raw.toUpperCase(Locale.ROOT));
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(hidden = true)
        @JsonIgnore
        private final State.Type state;

        @Schema(
            title = "Joined tables",
            description = "A file that contains matched entries, may be empty. Set when output type is STORE or AUTO resolves to STORE."
        )
        private final URI uri;

        @Schema(
            title = "Joined records",
            description = "JSON-safe records when output type is RECORDS or AUTO resolves to RECORDS."
        )
        private final List<Object> records;

        @Schema(
            title = "The number of rows of output",
            description = "Number of rows written to the output."
        )
        private final long outputSize;

        @Schema(
            title = "The number of rows of left input",
            description = "Number of rows read from the left input."
        )
        private final long leftSize;

        @Schema(
            title = "The number of rows of right input",
            description = "Number of rows read from the right input."
        )
        private final long rightSize;

        @Override
        public Optional<State.Type> finalState() {
            return Optional.ofNullable(state);
        }
    }
}
