# How to use the Transform JSONata plugin

Query and transform JSON data using [JSONata](https://jsonata.org/) expressions: a functional language for selecting, filtering, reshaping, and aggregating JSON structures.

## Tasks

`TransformValue` evaluates a JSONata `expression` against a single JSON string passed via `from` and returns the transformed result. Use it to reshape or extract values from a task output already in your flow.

`TransformItems` reads a JSON or Ion file from Kestra internal storage (passed as a `kestra://` URI via `from`), evaluates the `expression` against each record, and writes the results back to storage. If the expression returns an array and `explodeArray` is `true` (the default), each array element is written as a separate record. Set `explodeArray: false` to write the whole array as a single record instead. Use `TransformItems` for batch transformations over large datasets.

## Notes

Expressions follow the standard JSONata syntax. See the [JSONata documentation](https://docs.jsonata.org/) for operators, built-in functions, and examples.

`TransformValue` works on a value already in the flow, while `TransformItems` streams over a file in internal storage record by record.

`maxDepth` bounds how deeply an expression nests during evaluation, which is what stops a runaway recursive function. It does not depend on how much data you process, so the default of 1000 needs no adjustment as batches grow.

### Prefer path projection over `$map` with a lambda

Building an object around a batch mapped through a user-defined function is quadratic in the underlying engine, so it degrades sharply with batch size — around 18 s for 1000 items where the equivalent path projection takes 18 ms. Both forms produce the same output, so prefer the projection:

```
# slow — avoid
{ "items": $map($, function($r) { { "id": $r.eventId, "amount": $r.value } }) }

# fast — same result
{ "items": [ $.{ "id": eventId, "amount": value } ] }
```

It takes both ingredients to trigger: the same object wrapper around a builtin function (`$map($, $string)`) stays fast, and so does the lambda on its own without the wrapper. The cost is in [`dashjoin/jsonata-java`](https://github.com/dashjoin/jsonata-java), not in this plugin. Reach for `$map` with a lambda only when the transformation genuinely needs a function value, such as passing it to `$reduce` or `$sort`.
