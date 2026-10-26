# How to use the Transform Arrow plugin

Query columnar data files with jq expressions using [aq](https://github.com/Anaethelion/aq), a jq for Apache Arrow.

## Tasks

`Query` reads a Parquet, Arrow IPC, CSV or NDJSON file from Kestra internal storage (passed as a `kestra://` URI via `from`), converts each row to a JSON object and applies the jq `expression` to it. Set `slurp: true` to apply the expression once to the array of all the rows, for aggregations such as `length` or `map(.amount) | add`.

The result is written as Ion records by default, like the other transform tasks, so it can be used by any task reading Ion files. Set `outputFormat` to `NDJSON`, `JSON`, `CSV` or `ARROW` to get the file produced by aq instead.

## Notes

The input format is detected from the file extension. Set `inputFormat` when the file has no extension or an unusual one. Ion files are not supported as input: convert them first, for example with the `IonToJson` task of the Serdes plugin.

The task runs aq with a task runner, the Docker task runner by default, using the `ghcr.io/kestra-io/aq` image. Set `containerImage` to use another image providing the `aq` CLI.

aq relies on [jaq](https://github.com/01mf02/jaq), which supports most of the jq language. See the aq documentation for the jq features that are not supported and for how each Arrow type is converted to JSON.
