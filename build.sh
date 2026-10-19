#!/bin/sh
# Compila RakNetLib sin Maven y sin dependencias: deja target/RakNetLib.jar
# Necesita JDK 11 o mas nuevo.
set -e
cd "$(dirname "$0")"
JAVAC="javac"; command -v javac >/dev/null 2>&1 || JAVAC="java -m jdk.compiler/com.sun.tools.javac.Main"
JAR="jar";     command -v jar   >/dev/null 2>&1 || JAR="java -m jdk.jartool/sun.tools.jar.Main"

rm -rf target && mkdir -p target/classes
$JAVAC -source 11 -target 11 -Xlint:-options -encoding UTF-8 -d target/classes $(find src/main/java -name '*.java')
$JAR --create --file target/RakNetLib.jar -C target/classes .
echo "OK -> target/RakNetLib.jar"
