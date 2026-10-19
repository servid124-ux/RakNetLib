#!/bin/sh
# Compiles the library and the tests, then runs the stress test (lossy simulated network) and the localhost end-to-end test.
set -e
cd "$(dirname "$0")"
sh build.sh >/dev/null
mkdir -p target/test
javac -Xlint:-options -encoding UTF-8 -cp target/classes -d target/test $(find src/test/java -name '*.java')
java -cp target/classes:target/test raknetlib.EndToEndTest
java -cp target/classes:target/test raknetlib.LinkStressTest
