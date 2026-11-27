# Kestra Transform Plugin

## What

- Provides plugin components under `io.kestra.plugin`.
- Keeps the implementation focused on the integration scope exposed by this repository.

## Why

- What user problem does this solve? Teams need a reliable way to operate Transform from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps Transform steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on Transform.

## How

### Architecture

This is a **multi-module** plugin with 4 submodules:

- `plugin-transform-grok`
- `plugin-transform-json`
- `plugin-transform-records`
- `plugin-transform-arrow`

### Key Plugin Classes

**plugin-transform-grok:**

- `io.kestra.plugin.transform.grok.TransformItems`
- `io.kestra.plugin.transform.grok.TransformValue`
**plugin-transform-json:**

- `io.kestra.plugin.transform.jsonata.TransformItems`
- `io.kestra.plugin.transform.jsonata.TransformValue`
**plugin-transform-records:**

- `io.kestra.plugin.transform.Aggregate`
- `io.kestra.plugin.transform.Filter`
- `io.kestra.plugin.transform.Join`
- `io.kestra.plugin.transform.Map`
- `io.kestra.plugin.transform.Select`
- `io.kestra.plugin.transform.Unnest`
- `io.kestra.plugin.transform.Zip`

**plugin-transform-arrow:**

- `io.kestra.plugin.transform.arrow.Query`

### Join task layout (plugin-transform-records)

`Join` joins two inputs on a key (inner, outer, anti and semi joins). Its classes are spread over the shared packages of `io.kestra.plugin.transform`:

- `engine`: join implementations (`AbstractJoinImplementation`, `HashJoin`, `NestedLoopJoin`, `SortedMergeJoin`), `JoinKey`, `JoinKeyValue` (key value compared the same way by every implementation), `KeyedRow`
- `util`: property enums (`JoinType`, `JoinImplementation`, `MergeMode`, `MissingKeyAction`), `JoinSide`, `JoinMessages`, and `JoinIOHandler` (input reading and output writing through the run context)
- `ion`: `IterableIonReader` (streams a stored Ion file)

Join-specific rules:

- Keep the `leftSize`, `rightSize` and `outputSize` outputs; metrics may only be added on top of them.
- `leftLabel`, `rightLabel` and `prefixSeparator` are dynamic properties; labels are checked (not blank, distinct) at run time, and the separator may be empty.
- Join keys are split on `.`, not evaluated with the shared expression language.
- `HASH` must keep streaming the larger input; only the smaller one is held in memory.

### Project Structure

```
plugin-transform/
├── plugin-transform-grok/
│   └── src/main/java/...
├── ...                                    # Other submodules
├── build.gradle
├── settings.gradle
└── README.md
```

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
