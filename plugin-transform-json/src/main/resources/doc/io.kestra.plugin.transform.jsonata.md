# How to use the Transform JSONata plugin

Query and transform JSON data using [JSONata](https://jsonata.org/) expressions: a functional language for selecting, filtering, reshaping, and aggregating JSON structures.

## Tasks

`TransformValue` evaluates a JSONata `expression` against a single JSON string passed via `from` and returns the transformed result. Use it to reshape or extract values from a task output already in your flow.

`TransformItems` reads a JSON or Ion file from Kestra internal storage (passed as a `kestra://` URI via `from`), evaluates the `expression` against each record, and writes the results back to storage. If the expression returns an array and `explodeArray` is `true` (the default), each array element is written as a separate record. Set `explodeArray: false` to write the whole array as a single record instead. Use `TransformItems` for batch transformations over large datasets.

## Notes

Expressions follow the standard JSONata syntax. See the [JSONata documentation](https://docs.jsonata.org/) for operators, built-in functions, and examples.

`TransformValue` works on a value already in the flow, while `TransformItems` streams over a file in internal storage record by record.
