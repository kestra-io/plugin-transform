These tasks let you reshape, filter, combine, flatten, and summarize structured data without writing custom code. They work both with in-memory records and with stored Ion files, so the same tasks can be used for small workflow variables and larger streamed datasets.

## Common Inputs And Outputs

Most tasks accept records directly or as a stored Ion file.

- `from`: used by tasks that read a single input, such as `Map`, `Filter`, `Unnest`, and `Aggregate`
- `inputs`: used by tasks that read multiple inputs, such as `Select` and `Zip`

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
