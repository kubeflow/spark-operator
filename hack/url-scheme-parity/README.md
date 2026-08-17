# URL-scheme parity harness

Independent, self-contained check for the concern raised in review of the
webhook URL-scheme validation (PR #3019): the webhook extracts a value's scheme
with Go's `net/url`, but Spark decides whether to *fetch* a dependency using
`java.net.URI`. If the two disagreed such that the webhook **allowed** a value
Spark would then **fetch remotely** (with the operator's credentials), the check
would have a bypass.

This directory reproduces both decisions from the same input corpus so they can
be diffed directly.

- `parity.go` reproduces `internal/webhook` `checkURLScheme` with an empty
  allow-list (the default posture): `net/url.Parse`, lower-cased scheme, the
   always-allowed local schemes (`""`, `file`, `local`), the authority guard for
   `//host`, `file://host`, and `local://host`, and fail-closed parse errors.
   Verdict: `ALLOW` / `REJECT`.
- `Parity.java` reproduces how Spark classifies the value:
  `SparkFileUtils.resolveURI` + `Utils.downloadFile` in the Spark source
  (`java.net.URI.getScheme`, case-sensitive `http|https|ftp` match, Hadoop
  FileSystem for other schemes, local for `file`/`local`/schemeless/unparseable).
  Verdict: `LOCAL` / `FETCH-HTTP` / `FETCH-HADOOP` / `RPC`.

The `INPUTS`/`inputs` arrays are kept byte-identical and in the same order so the
two outputs line up row for row.

## Run

Requires a JDK 11+ (single-file source launch, no `javac` step) and Go 1.25+.

```sh
go run parity.go   > /tmp/go.txt
java Parity.java   > /tmp/java.txt
paste -d'|' /tmp/go.txt /tmp/java.txt
```

## Invariant to check

The only dangerous divergence is **Go `ALLOW` while Java `FETCH-HTTP` or
`FETCH-HADOOP`**. There should be no such row. Observed results:

- Wherever the two differ, Go is the **stricter** side. Malformed inputs
  (embedded tab/newline, illegal scheme chars, leading space, backslash) make
  Go's `url.Parse` error, so `checkURLScheme` rejects them; Java's `URI` throws
  `URISyntaxException`, which `resolveURI` swallows and treats as a **local**
  file. Both ends are safe.
- `local://` is Go `ALLOW` / Java `LOCAL` (in-container scheme, never fetched) -
  the safe direction.

This confirms the check is defense-in-depth that does not under-detect relative
to Spark's own parsing; it is not a claim that Go and Java parse URIs
identically. Re-run this harness against a new Spark version if the dependency
resolution path (`resolveURI` / `downloadFile`) changes.
