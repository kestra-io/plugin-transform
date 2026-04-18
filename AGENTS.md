# Kestra Transform Plugin

## What

- Provides plugin components under `io.kestra.plugin`.
- Keeps the implementation focused on the integration scope exposed by this repository.

## Why

- This plugin integrates Kestra with Transform.
- It adds workflow components that reflect the code in this repository.

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
