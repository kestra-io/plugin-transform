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

This is a **multi-module** plugin with 3 submodules:

- `plugin-transform-grok`
- `plugin-transform-json`
- `plugin-transform-records`

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
- `io.kestra.plugin.transform.Map`
- `io.kestra.plugin.transform.Select`
- `io.kestra.plugin.transform.Unnest`
- `io.kestra.plugin.transform.Zip`

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
