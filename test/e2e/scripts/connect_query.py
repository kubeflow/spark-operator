# Copyright The Kubeflow Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Query a Spark Connect server and print marker lines for the e2e test.

The Spark Connect REPL only starts a real remote session when it is launched
with ``--remote`` and driven through a terminal. Without a terminal it either
refuses to start or, with a classic ``--master``, silently falls back to the
local Classic session. In the Classic fallback the query runs on the client's
own JVM, which never sees the dependencies declared in ``spec.deps``, so the
result is a false negative. ``pty.fork`` gives the REPL the terminal it needs,
which keeps every query on the server session.

Usage::

    python3 connect_query.py sc://<host>:15002

On success it prints ``AVRO=<hex>`` and ``RE2J=<bool>`` and exits 0. The Avro
value comes from the declared ``spark-avro`` package and the re2j value from the
declared jar, so both halves of ``spec.deps`` are proven to resolve on the server
session. Any other outcome exits non-zero with a diagnostic line, so the caller
can surface the server's own error message.
"""

import os
import pty
import re
import select
import sys
import time

URL = sys.argv[1]
ANSI = re.compile(r"\x1b\[[0-9;]*[a-zA-Z]")

pid, fd = pty.fork()
if pid == 0:
    os.environ["HOME"] = "/tmp"
    os.environ["TERM"] = "xterm"
    os.execvp(
        "/opt/spark/bin/spark-connect-shell",
        ["/opt/spark/bin/spark-connect-shell", "--remote", URL],
    )
    os._exit(1)


def pump(seconds):
    """Drain the terminal for a fixed window and return the cleaned output."""
    buf = ""
    deadline = time.time() + seconds
    while time.time() < deadline:
        ready, _, _ = select.select([fd], [], [], 0.4)
        if ready:
            try:
                chunk = os.read(fd, 65536)
            except OSError:
                break
            if not chunk:
                break
            buf += chunk.decode(errors="replace")
    return ANSI.sub("", buf).replace("\r", "\n")


def value(text, prefix):
    """Return the first printed value with this prefix, skipping the echoed command."""
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith(prefix):
            return stripped[len(prefix):]
    return None


pump(90)  # server session bootstrap and Ammonite compilation
os.write(fd, b'println("SESSION_PROBE=" + spark.getClass.getName)\n')
session_out = pump(25)
# AVRO comes from the declared spark-avro package, RE2J from the declared jar.
# Asserting on the values (not a row count) proves each one resolved on the
# server session; either being absent makes the SQL itself fail.
os.write(
    fd,
    b'try { println("AVRO=" + spark.sql("SELECT hex(to_avro(42))")'
    b'.collect()(0).getString(0)) } '
    b'catch { case e: Throwable => println("AVRO_ERROR=" + e.getMessage.take(160)) }\n',
)
os.write(
    fd,
    b'try { println("RE2J=" + spark.sql("SELECT reflect(\'com.google.re2j.Pattern\', '
    b'\'matches\', \'a.c\', \'abc\')").collect()(0).getString(0)) } '
    b'catch { case e: Throwable => println("RE2J_ERROR=" + e.getMessage.take(160)) }\n',
)
query_out = pump(90)
os.write(fd, b"System.exit(0)\n")
time.sleep(1)
try:
    os.close(fd)
except OSError:
    pass

session = value(session_out, "SESSION_PROBE=")
avro = value(query_out, "AVRO=")
re2j = value(query_out, "RE2J=")
error = value(query_out, "AVRO_ERROR=") or value(query_out, "RE2J_ERROR=")

if session != "org.apache.spark.sql.connect.SparkSession":
    print("CONNECT_SESSION_UNEXPECTED=" + str(session))
    sys.exit(1)
if error is not None:
    print("CONNECT_QUERY_FAILED=" + error)
    sys.exit(1)
if avro is None or re2j is None:
    print("CONNECT_VALUE_MISSING avro=%r re2j=%r" % (avro, re2j))
    sys.exit(1)
print("AVRO=" + avro)
print("RE2J=" + re2j)
