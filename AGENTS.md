# Kestra Transform Plugin

## What

Kestra plugin providing integration with Transform. Exposes 10 plugin components (tasks, triggers, and/or conditions).

## Why

Enables Kestra workflows to interact with Transform, allowing orchestration of Transform-based operations as part of data pipelines and automation workflows.

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

### Important Commands

```bash
# Build the plugin
./gradlew shadowJar

# Run tests
./gradlew test

# Run a specific submodule's tests
./gradlew :plugin-transform-grok:test

# Build without tests
./gradlew shadowJar -x test
```

### Configuration

All tasks and triggers accept standard Kestra plugin properties. Credentials should use
`{{ secret('SECRET_NAME') }}` — never hardcode real values.

## Agents

**IMPORTANT:** This is a Kestra plugin repository (prefixed by `plugin-`, `storage-`, or `secret-`). You **MUST** delegate all coding tasks to the `kestra-plugin-developer` agent. Do NOT implement code changes directly — always use this agent.
