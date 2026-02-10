# Kestra Transform Plugin

Typed, streaming-friendly transform tasks for Kestra using Amazon Ion.

## Tasks
- `io.kestra.plugin.transform.Select`: align N inputs by position + optional filter + projection
- `io.kestra.plugin.transform.Map`: project/rename/cast fields
- `io.kestra.plugin.transform.Unnest`: explode array fields into rows
- `io.kestra.plugin.transform.Filter`: keep/drop records by boolean expression
- `io.kestra.plugin.transform.Aggregate`: group-by and typed aggregates
- `io.kestra.plugin.transform.Zip`: merge multiple record streams by position

## Input and output
Most tasks accept:
- `from`: in-memory records (map/list/Ion) or a storage URI (`kestra://...`)

Multi-input tasks:
- `Zip`: `inputs` (list)
- `Select`: `inputs` (list)

Output mode (`output`) supports:
- `AUTO` (default): STORE if `from` is a storage URI, else RECORDS
- `RECORDS`: emit `outputs.records`
- `STORE`: write newline-delimited Ion to internal storage and emit `outputs.uri`

Experimental output format (`outputFormat`):
- `TEXT` (default): newline-delimited Ion text
- `BINARY`: Ion binary output (only readable by transform tasks; use TEXT as final step)

## Expression language
- Field access: `user.address.city` or `user["first name"]`
- Arrays: `items[].price`
- Comparisons: `> < == != >= <=`
- Boolean: `&& || !`
- Functions: `sum`, `count`, `min`, `max`, `avg`, `first`, `last`, `coalesce`, `concat`, `toInt`, `toDecimal`, `toString`, `toBoolean`, `parseTimestamp`

Cheat sheet: `docs/EXPRESSION_CHEATSHEET.md`

## Type casting
- `type` is optional in Map/Aggregate definitions.
- If `type` is omitted, the evaluated Ion value is passed through.
- `TIMESTAMP` supports Ion timestamps and ISO-8601 strings (`parseTimestamp` can convert strings).

## Tasks (reference + examples)

### Select
```yaml
type: io.kestra.plugin.transform.Select
```
Use this when you want "Zip + optional Filter + Map" in one streaming operator: align inputs, filter rows, and project/cast output fields.

Common config:
- `inputs`: list of inputs (1+)
- `where`: optional boolean expression (supports `$1`, `$2`, ...)
- `fields`: Map-style definitions (shorthand or `{ expr, type, optional }`)
- `keepInputFields`: optional list of input indices to copy into output when `fields` is set (e.g. `[1]` keeps only `$1` fields)
- `onLengthMismatch`: `FAIL | SKIP` (when inputs are different lengths)
- `onError`: `FAIL | SKIP | KEEP` (KEEP emits the original merged row)

Example: enrich orders with scores and output typed columns
```yaml
- id: select
  type: io.kestra.plugin.transform.Select
  inputs:
    - "{{ outputs.orders.values.records }}"
    - "{{ outputs.scores.values.records }}"
  where: $1.amount > 100 && $2.score > 0.8
  fields:
    order_id:
      expr: $1.order_id
      type: INT
    amount:
      expr: $1.amount
      type: DECIMAL
    score:
      expr: $2.score
      type: DECIMAL
  output: RECORDS
```

### Map
```yaml
type: io.kestra.plugin.transform.Map
```
Use this when you want to normalize records into a typed schema: rename fields, compute derived fields, and cast values (without scripts).

Common config:
- `fields`: shorthand `field: expr` or full `{ expr, type, optional }`
- `keepOriginalFields`: keep input fields not mapped by target name
- `dropNulls`: drop null fields from output
- `onError`: `FAIL | SKIP | NULL` (NULL sets failing fields to null)

Example: normalize API records into typed columns
```yaml
- id: normalize
  type: io.kestra.plugin.transform.Map
  from: "{{ outputs.fetch.records }}"

  fields:
    customer_id:
      expr: user.id
      type: STRING
    created_at:
      expr: createdAt
      type: TIMESTAMP
    total:
      expr: sum(items[].price)
      type: DECIMAL

  keepOriginalFields: false
  dropNulls: true
  onError: SKIP
```
Note: `keepOriginalFields` keeps input fields not mapped by name; mapping `a_new: a` still keeps the original `a`.

### Unnest
```yaml
type: io.kestra.plugin.transform.Unnest
```
Use this when you want to explode an array field into multiple rows (one per element), similar to "UNNEST" in SQL.

Common config:
- `path`: array path to explode (e.g. `items[]`)
- `as`: field name that receives the element value
- `keepOriginalFields`: keep original fields except the exploded array field

Example: explode items into one row per item
```yaml
- id: explode_items
  type: io.kestra.plugin.transform.Unnest
  from: "{{ outputs.fetch.records }}"
  path: items[]
  as: item
```

### Filter
```yaml
type: io.kestra.plugin.transform.Filter
```
Use this when you want to keep/drop records based on a boolean expression, like a SQL `WHERE`.

Common config:
- `where`: boolean expression evaluated per record
- `onError`: `FAIL | SKIP | KEEP` (KEEP keeps the record if `where` fails)

Example: keep only expensive items
```yaml
- id: expensive_items
  type: io.kestra.plugin.transform.Filter
  from: "{{ outputs.explode_items.records }}"
  where: item.price > 10
```

### Aggregate
```yaml
type: io.kestra.plugin.transform.Aggregate
```
Use this when you want typed group-by aggregates (count/sum/min/max) without exporting to a database.

Common config:
- `groupBy`: list of fields that form the group key
- `aggregates`: Map-style definitions `{ expr, type, optional }` (type optional)

Example: compute per-customer totals
```yaml
- id: totals
  type: io.kestra.plugin.transform.Aggregate
  from: "{{ outputs.normalize.records }}"
  groupBy:
    - customer_id
    - country
  aggregates:
    order_count:
      expr: count()
      type: INT
    total_spent:
      expr: sum(total_spent)
      type: DECIMAL
    last_order_at:
      expr: max(created_at)
      type: TIMESTAMP
  onError: FAIL
```

### Zip
```yaml
type: io.kestra.plugin.transform.Zip
```
Use this when you have multiple sources already aligned by row order and want to merge them positionally (record i with record i).

Common config:
- `inputs`: list of inputs (2+)
- `onConflict`: `FAIL | LEFT | RIGHT` when two inputs have the same field name

Example: merge two record streams by row position
```yaml
- id: zip
  type: io.kestra.plugin.transform.Zip
  inputs:
    - "{{ outputs.left.values.records }}"
    - "{{ outputs.right.values.records }}"
  onConflict: RIGHT
```

## Examples index
- `examples/api_to_typed_records.yml`: normalize API output into typed fields
- `examples/http_download_transform.yml`: download products, unnest, map, and store
- `examples/dummyjson_products_flow.yml`: unnest products, filter, map
- `examples/dummyjson_carts_flow.yml`: compute max product total per cart and filter
- `examples/dummyjson_users_flow.yml`: unnest users, map, filter
- `examples/aggregate_totals.yml`: group and aggregate totals
- `examples/zip_basic.yml`: zip record streams by position

More flows live in `examples/`.

## Migration notes
See `docs/UPGRADE.md` for breaking changes (options flattening, renamed fields).

## Development
Prerequisites:
- Java 21
- Docker

Run tests:
```sh
./gradlew test
```

Run Kestra locally with the plugin:
```sh
./gradlew shadowJar && docker build -t kestra-custom . && docker run --rm -p 8080:8080 kestra-custom server local
```

## Benchmarks
Opt-in benchmarks live in `src/test/java/io/kestra/plugin/transform/BenchTest.java`.

Examples:
```sh
./gradlew test --tests io.kestra.plugin.transform.BenchTest -Dbench=true -Dbench.records=10000,100000 -Dbench.format=text
./gradlew test --tests io.kestra.plugin.transform.BenchTest -Dbench=true -Dbench.records=10000,100000 -Dbench.format=binary
```

Reports are written to `build/bench/report.md`.

## Documentation
Kestra docs: https://kestra.io/docs  
Plugin developer guide: https://kestra.io/docs/plugin-developer-guide
