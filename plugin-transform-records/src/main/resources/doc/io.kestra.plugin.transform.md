# How to use the Transform plugin

These tasks let you reshape, filter, combine, join, flatten, and summarize structured data without writing custom code. They work both with in-memory records and with stored Ion files, so the same tasks can be used for small workflow variables and larger streamed datasets.

## Common Inputs And Outputs

Most tasks accept records directly or as a stored Ion file.

- `from`: used by tasks that read a single input, such as `Map`, `Filter`, `Unnest`, and `Aggregate`
- `inputs`: used by tasks that read multiple inputs, such as `Select` and `Zip`
- `left` and `right`: used by `Join`, which matches rows of two inputs on a key

These inputs can typically be:

- a list of records from a previous task output or variable
- a single structured value that can be treated as a record
- a stored Ion file URI

Most tasks also share the same output options:

- `outputType: AUTO`: return records for in-memory input, or store the result when the input is already a stored Ion file
- `outputType: RECORDS`: return records directly in the task output
- `outputType: STORE`: write the result to internal storage and return a URI
- `outputFormat`: choose `TEXT` or `BINARY` when writing Ion output

Common error handling options are also shared across several tasks:

- `FAIL`: stop the task when an expression or transform step fails
- `SKIP`: drop the current record or row and continue
- some tasks also support task-specific modes such as `KEEP` for `Filter` or row-length and field-conflict handling for `Select` and `Zip`

## Common Expression Language

`Map`, `Filter`, `Select`, and `Unnest` share the same per-record expression language.

Use it to read fields, navigate nested data, compute values, and write filter conditions.

### Field Access

Use dot notation for regular fields:

- `user.id`
- `customer.address.city`

Use bracket notation when a field name contains spaces or special characters:

- `user["first name"]`
- `$1["event type"]`

### Arrays

Use array indexes to read a specific element:

- `items[0]`
- `orders[1].total`

Use `[]` to expand an array and work with all of its elements:

- `items[].price`
- `orders[].lines[].sku`

This is especially useful with functions such as `sum`, `count`, `min`, and `max`.

### Literals And Operators

Supported literals:

- numbers: `1`, `3.14`
- strings: `"paid"`
- booleans: `true`, `false`
- null: `null`

Supported operators:

- boolean: `&&`, `||`, `!`
- comparison: `==`, `=`, `!=`, `>`, `>=`, `<`, `<=`, `in (...)`
- arithmetic: `+`, `-`, `*`, `/`

Use `==` as the primary equality operator in examples and docs. A single `=` is also accepted as an alias for equality.
Use `in (...)` for membership checks against an explicit list of candidate values.

Parentheses can be used to control precedence:

- `(is_active || is_trial) && total_spent > 100`
- `sum(items[].price) / count(items[].price)`
- `country in ("FR", "DE", "AT")`

### Shared Functions

The shared expression language supports these functions:

- `toInt(value)`: convert a single scalar value to an integer
- `toDecimal(value)`: convert a single scalar value to a decimal number
- `toString(value)`: convert a single scalar value to a string
- `toBoolean(value)`: convert a single scalar value to a boolean
- `parseTimestamp(value)`: parse an Ion timestamp or an ISO-8601 timestamp string accepted by Java `Instant.parse`, such as `2024-02-01T12:00:00Z`, `2024-02-01T12:00:00.123Z`, or `2024-02-01T13:00:00+01:00`
- `sum(values)`: aggregate numeric values from an array or expanded field path such as `items[].price`
- `avg(values)`: return the average of numeric values from an array or expanded field path
- `count(values)`: count values from an array or expanded field path
- `min(values)`: return the minimum value from a comparable array or expanded field path
- `max(values)`: return the maximum value from a comparable array or expanded field path
- `coalesce(v1, v2, ...)`: return the first non-null value from one or more scalar expressions
- `concat(v1, v2, ...)`: concatenate one or more scalar expressions as strings

Examples:

- `toDecimal(amount)`
- `coalesce(country, "unknown")`
- `concat(first_name, " ", last_name)`
- `sum(items[].price)`

### Positional Inputs In `Select`

`Select` works on multiple row-aligned inputs and also supports positional references:

- `$1`, `$2`, `$3`, ...

Example:

- `$1.amount > 100 && $2.status == "active"`

Use positional references when you want to distinguish fields coming from different aligned inputs.

## Aggregate Expressions

`Aggregate` does not use the same per-record expression language for its aggregate definitions. Its `aggregates` entries use aggregate expressions instead:

- `count()`
- `sum(total)`
- `min(score)`
- `max(score)`
- `avg(duration)`
- `first(status)`
- `last(status)`

Use the shared expression language for per-record transforms and conditions. Use aggregate expressions when summarizing grouped records.

## Join

`Join` matches the rows of two inputs, `left` and `right`, whose key values are equal, the way you would join two tables in SQL.\
Keys of different types never match; decimals are compared by value (`1.0` equals `1.00`) and timestamps by instant, whatever their offset or precision.\
Each input can be a list of records or a stored Ion file URI, and the common `outputType` and `outputFormat` options apply.\
Besides `uri` or `records`, the task outputs the row counts `outputSize`, `leftSize` and `rightSize`.

### Join types

Several join types are supported, allowing different operations to be applied on the input data:

- **inner join (default)**
- outer joins: full outer, left outer, right outer
- anti joins: left anti, right anti
- semi joins: left semi, right semi

Note that **anti** and **semi** joins are not *merge-type* joins, which means that they only filter rows from one side (left or right) without combining data from both inputs.

### Join implementations

These are algorithmic strategies for performing the join.\
The choice does not affect the output content, but it can influence performance and memory use, depending on dataset size and whether inputs are sorted.\
Output row order may also vary depending on this.

Available implementations:

- **hash (default)**: only the smaller input is held in memory, in a hash table; the other input is streamed.
- nested loop: the right input is held in memory and compared with every row of the streamed left input; only suited to small inputs.
- sorted merge: both inputs are held in memory to be sorted on the key.

When an input is given as records rather than a stored file, it is already in memory, whatever the implementation.

### Merge mode

The `mergeMode` property allows you to configure the output structure to differentiate fields by side.\
It can have the following values:
- **overwrite (default)**: with [merge-type joins](#join-types), fields that have identical names between inputs will be overwritten in matched rows.\
As fields from **left** are written before **right** ones, the remaining values will be those from **right** in the case of overwriting.
- **prefix**: adds a prefix to fields names according to side with `leftLabel` and `rightLabel` values, and a separator character specified by the `prefixSeparator` property.\
For example, with common `name` field, `leftLabel = "L"`, `rightLabel = "R"` and `prefixSeparator = "_"`, output could be:
```json
{"L_name": "x", "R_name": "y", ...}
...
```
- **nest**: nest fields in two objects for each side named by `leftLabel` and `rightLabel`.\
For example, with common `name` field and `leftLabel = "left"` and `rightLabel = "right"`, output could be:
```json
{"left": {"name":"x", ...}, "right": {"name":"y", ...}
...
```

`leftLabel` and `rightLabel` are static values, checked when the flow is saved: they must not be blank and must differ.

### Accepted key types

Following **Ion** types are accepted for key fields:
- `BOOL`,
- `DECIMAL`,
- `FLOAT`,
- `INT`,
- `STRING`,
- `TIMESTAMP`.

Additionally, `NULL` type is accepted only if `onMissingKey` value is `IGNORE` or `WARNING`, which is not the case by default (see [Missing key](#missing-key)).

Values only match when they have the same type: for example, the `INT` value `1` does not match the `DECIMAL` value `1.0`.

### Special cases

#### Nested key

You can use a nested field as join key with an object-like syntax.

For example, for this input:

```json
{"id": {"a": 1, "b": "x"}}
...
```

The `a` field can be accessed with the following key: `id.a`.\
This works with more levels of nesting too.\
Note however that the key does not use the [common expression language](#common-expression-language): it is just a raw string that is split on the dot sign, so field names may contain any other character (e.g. `order-id`).

The task could fail in the following contexts of nested fields:
- using a nested key that is too deep (not reachable) for the actual row of input, for example: `id.a.z` for the previous input.
- using a key that is too shallow for the actual row of input, meaning the value is still a nested row (which is not a valid type), for example: `id` for the previous input.

#### Empty input

The task will **not fail** if one or both input(s) is empty, as this case is not strictly invalid for join algorithms.\
However you will get a log message for each empty input.

#### Missing key

By default, the task will **fail** if the key field does not exist in at least one row of an input or has a `NULL` value.

This behaviour is defined by the `onMissingKey` optional property, which has the `FAIL` default value.\
Other possible values are:
- `WARNING`: sets the task state to `WARNING`, and a log will be emitted for the first corresponding row of each input.
- `IGNORE`: allows the task to continue with no state change, and a log will be emitted for the first corresponding row of each input.

If there are missing keys in both inputs, they will not match.

#### Semi joins identical rows

By default, semi joins can output multiple identical rows, as one row can match with several rows from other side, but without retrieving the fields from the other side.

You can change this behaviour to get only the first matched rows, by putting the value of
`onlyFirstMatchOnSemi` property to `true`.\
This property is optional, and its default is `false`.

#### Sorted merge specific errors

Some errors will occur only with sorted merge implementation, as it needs a stricter data consistency by design:
- different key types between rows from a single input,
- different key types between inputs.
