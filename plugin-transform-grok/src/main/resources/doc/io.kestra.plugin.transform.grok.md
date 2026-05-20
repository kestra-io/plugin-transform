# How to use the Transform Grok plugin

Parse unstructured text — log lines, error messages, raw event data — into structured fields using Grok patterns.

## Tasks

`TransformValue` applies one or more Grok patterns to a single string value passed via `from` and returns the captured named fields as a map. Use it to parse a single string already in your flow's variables.

`TransformItems` reads a file from Kestra internal storage (passed as a `kestra://` URI via `from`), applies the pattern to each line, and writes the structured results back to storage. Use it for batch log parsing or event normalization.

Both tasks accept `pattern` for a single Grok expression or `patterns` for a list of expressions tried in order — matching stops at the first successful pattern. Add custom pattern definitions with `patternDefinitions` (a map of name to regex) or point `patternsDir` at directories containing pattern files. By default only named capture groups are returned (`namedCapturesOnly: true`) and matching stops at the first successful pattern (`breakOnFirstMatch: true`).
