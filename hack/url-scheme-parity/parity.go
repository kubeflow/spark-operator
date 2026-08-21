// Command parity reproduces the webhook's URL-scheme decision (internal/webhook
// checkURLScheme, with an empty allow-list) using Go's net/url, so it can be
// diffed against Parity.java which reproduces how Spark decides whether to fetch
// a value (java.net.URI.getScheme, as used by SparkFileUtils.resolveURI and
// Utils.downloadFile).
//
// The two programs share the same INPUTS list. The security-relevant question
// is one-directional: is there any input the webhook ALLOWs but Spark would
// FETCH remotely? Run both and diff:
//
//	go run parity.go            > /tmp/go.txt
//	java Parity.java            > /tmp/java.txt
//	paste -d' ' /tmp/go.txt /tmp/java.txt   # eyeball, or diff the VERDICT columns
//
// A safe result is: every row where Java prints FETCH-* has Go printing REJECT.
package main

import (
	"fmt"
	"net/url"
	"strings"
)

// inputs is the shared adversarial corpus. Keep byte-identical to INPUTS in
// Parity.java (same order) so the two outputs line up row for row.
var inputs = []string{
	"http://evil/x",
	"HTTP://evil/x",
	"https://evil/x",
	"ftp://evil/x",
	"s3a://bucket/x",
	"hdfs://nn/x",
	"gs://bucket/x",
	"gopher+ssh://x",
	"http:evil/x",     // opaque, no //
	"s3a:/onlyone",    // single slash
	"file:///opt/a.jar",
	"local:///opt/a.jar",
	"file://host/opt/a.jar",
	"local://host/opt/a.jar",
	"/abs/path",
	"./rel/path",
	"c:/windows/path", // Windows drive letter reads as scheme "c"
	"mailto:a@b",
	"//host/path",       // scheme-relative
	" http://evil/x",    // leading space
	"http\t://evil/x",   // embedded tab (control char)
	"java\nscript:x",    // embedded newline
	"ht!tp://x",         // illegal scheme char
	"1http://x",         // scheme cannot start with a digit
	"a\\b://x",          // backslash
	"http ://x",         // space before colon
}

// alwaysAllowed mirrors internal/webhook.alwaysAllowedURLSchemes. All local forms
// must also have an empty authority; see decide.
var alwaysAllowed = map[string]struct{}{"": {}, "file": {}, "local": {}}

// decide reproduces checkURLScheme with an empty operator allow-list: only the
// always-allowed local schemes with no authority pass; anything else, an authority-bearing
// local form, or a value net/url cannot parse (fail closed) is rejected.
func decide(value string) (verdict, scheme, detail string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "ALLOW", "", "empty"
	}
	u, err := url.Parse(value)
	if err != nil {
		return "REJECT", "", "parse error (fail closed): " + err.Error()
	}
	scheme = strings.ToLower(u.Scheme)
	if _, ok := alwaysAllowed[scheme]; ok {
		if u.Host != "" {
			return "REJECT", scheme, "authority " + u.Host + " not local"
		}
		return "ALLOW", scheme, "always-allowed local scheme"
	}
	return "REJECT", scheme, "scheme not in allow-list"
}

func main() {
	fmt.Printf("%-8s %-12s %s\n", "VERDICT", "SCHEME", "INPUT")
	for _, in := range inputs {
		verdict, scheme, _ := decide(in)
		fmt.Printf("%-8s %-12s %s\n", verdict, scheme, quote(in))
	}
}

// quote renders control chars visibly so rows stay single-line and line up with
// the Java output.
func quote(s string) string {
	r := strings.NewReplacer("\t", "\\t", "\n", "\\n", "\\", "\\\\")
	return r.Replace(s)
}
