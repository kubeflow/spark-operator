// Parity reproduces how Spark decides whether to fetch a dependency value, using
// the same java.net.URI parsing Spark uses, so it can be diffed against parity.go
// (which reproduces the operator webhook's Go net/url decision).
//
// Spark path modelled (Spark 3.x/4.x, unchanged in sfdc-spark):
//   - org.apache.spark.util.SparkFileUtils.resolveURI: new URI(path); if getScheme()
//     is null it becomes a local file:// path; a URISyntaxException also falls back
//     to a local File.
//   - org.apache.spark.util.Utils.downloadFile: switches on
//     Option(uri.getScheme).getOrElse("file") -> "spark" (RPC),
//     "http"|"https"|"ftp" (URL.openConnection), "file" (local copy),
//     otherwise Hadoop FileSystem (fetch). The http/https/ftp match is
//     case-sensitive against the original-case scheme.
//
// Run directly (JDK 11+, no separate compile step):
//   java Parity.java > /tmp/java.txt
//
// The security-relevant question: is there any input classified FETCH-* here that
// parity.go classifies ALLOW? Diff the two outputs; there should be none.
import java.net.URI;
import java.util.Locale;

public class Parity {

    // INPUTS must stay byte-identical (and same order) to inputs in parity.go.
    static final String[] INPUTS = new String[] {
        "http://evil/x",
        "HTTP://evil/x",
        "https://evil/x",
        "ftp://evil/x",
        "s3a://bucket/x",
        "hdfs://nn/x",
        "gs://bucket/x",
        "gopher+ssh://x",
        "http:evil/x",       // opaque, no //
        "s3a:/onlyone",      // single slash
        "file:///opt/a.jar",
        "local:///opt/a.jar",
        "file://host/opt/a.jar",
        "local://host/opt/a.jar",
        "/abs/path",
        "./rel/path",
        "c:/windows/path",   // Windows drive letter reads as scheme "c"
        "mailto:a@b",
        "//host/path",       // scheme-relative
        " http://evil/x",    // leading space
        "http\t://evil/x",   // embedded tab (control char)
        "java\nscript:x",    // embedded newline
        "ht!tp://x",         // illegal scheme char
        "1http://x",         // scheme cannot start with a digit
        "a\\b://x",          // backslash
        "http ://x",         // space before colon
    };

    // decide mirrors resolveURI + downloadFile: what Spark would do with the value.
    // Returns "VERDICT\tSCHEME".
    static String decide(String value) {
        String scheme;
        try {
            scheme = new URI(value).getScheme();
        } catch (Exception e) {
            // resolveURI catches URISyntaxException and treats the value as a local File.
            return "LOCAL\t<unparseable>";
        }
        if (scheme == null) {
            return "LOCAL\t<none>";       // resolveURI -> file:// local path
        }
        // downloadFile matches http/https/ftp case-sensitively on the raw scheme.
        switch (scheme) {
            case "spark":
                return "RPC\t" + scheme;
            case "http":
            case "https":
            case "ftp":
                return "FETCH-HTTP\t" + scheme;
            case "file":
                return "LOCAL\t" + scheme;
            case "local":
                // "local://" is Spark's in-container scheme: the resource is expected
                // to already exist in the driver/executor image and is never fetched by
                // the submitter. KubernetesUtils treats it as a non-local-dependency that
                // is left as-is, so the operator does not dereference it. Not a vector.
                return "LOCAL\t" + scheme;
            default:
                // Everything else (s3a, hdfs, gs, HTTP, c, mailto, ...) hits the
                // Hadoop FileSystem branch: getHadoopFileSystem(uri) + fetch. Some
                // of these fail later for lack of a filesystem impl, but the fetch
                // is attempted with the submitter's (operator's) credentials.
                return "FETCH-HADOOP\t" + scheme;
        }
    }

    static String show(String s) {
        return s.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n");
    }

    public static void main(String[] args) {
        System.out.printf("%-13s %-12s %s%n", "VERDICT", "SCHEME", "INPUT");
        for (String in : INPUTS) {
            String[] parts = decide(in).split("\t", 2);
            String verdict = parts[0];
            String scheme = parts.length > 1 ? parts[1] : "";
            // Lower-case display of scheme to line up with Go's lowercased output;
            // the switch above already used the raw case for the fetch decision.
            System.out.printf("%-13s %-12s %s%n", verdict, scheme.toLowerCase(Locale.ROOT), show(in));
        }
    }
}
