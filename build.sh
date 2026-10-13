#!/bin/sh
# Compila RakNetLib sin Maven: deja target/RakNetLib.jar y target/netty.cp (los jars de Netty que usa).
#
# Transporte UDP: Netty 4.2 si se encuentran sus jars; si no, sockets Java (la libreria sigue sin dependencias).
#   NETTY_JARS=/carpeta  usa los jars que ya tengas ahi (netty-common, netty-buffer, netty-resolver, netty-transport)
#   (sin definir)        los baja de Maven Central a ~/.cache/raknetlib (necesita curl o wget)
#   NO_NETTY=1           compila sin Netty: solo el transporte de sockets
# Necesita JDK 11 o mas nuevo.
set -e
cd "$(dirname "$0")"
JAVAC="javac"; command -v javac >/dev/null 2>&1 || JAVAC="java -m jdk.compiler/com.sun.tools.javac.Main"
JAR="jar";     command -v jar   >/dev/null 2>&1 || JAR="java -m jdk.jartool/sun.tools.jar.Main"

NETTY_VERSION=4.2.18.Final
NETTY_MODULES="netty-common netty-buffer netty-resolver netty-transport"

NETTY_CP=""
if [ -n "${NO_NETTY:-}" ]; then
    echo "NO_NETTY=1: sin Netty, solo transporte de sockets"
else
    if [ -n "${NETTY_JARS:-}" ]; then
        NETTY_DIR="$(cd "$NETTY_JARS" && pwd)"
    else
        NETTY_DIR="${RAKNETLIB_CACHE:-$HOME/.cache/raknetlib}/netty-$NETTY_VERSION"
        mkdir -p "$NETTY_DIR"
        for m in $NETTY_MODULES; do
            f="$NETTY_DIR/$m-$NETTY_VERSION.jar"
            if [ ! -f "$f" ]; then
                url="https://repo1.maven.org/maven2/io/netty/$m/$NETTY_VERSION/$m-$NETTY_VERSION.jar"
                echo "Descargando $m $NETTY_VERSION..."
                if command -v curl >/dev/null 2>&1; then curl -fsSL -o "$f.part" "$url"
                elif command -v wget >/dev/null 2>&1; then wget -q -O "$f.part" "$url"
                else echo "Necesito curl o wget para bajar Netty (o define NETTY_JARS=/carpeta con los jars)"; exit 1; fi
                mv "$f.part" "$f"
            fi
        done
    fi
    for m in $NETTY_MODULES; do
        f="$NETTY_DIR/$m-$NETTY_VERSION.jar"
        if [ ! -f "$f" ]; then echo "Falta $f (revisa NETTY_JARS o borra la carpeta de cache para bajarlo de nuevo)"; exit 1; fi
        NETTY_CP="${NETTY_CP:+$NETTY_CP:}$f"
    done
    echo "Netty $NETTY_VERSION en $NETTY_DIR"
fi

rm -rf target && mkdir -p target/classes
SRC=$(find src/main/java -name '*.java')
set -- -source 11 -target 11 -Xlint:-options -encoding UTF-8 -d target/classes
if [ -n "$NETTY_CP" ]; then
    set -- "$@" -cp "$NETTY_CP"
else
    SRC=$(printf '%s\n' "$SRC" | grep -v '/NettyTransport\.java$')
fi
$JAVAC "$@" $SRC
printf '%s' "$NETTY_CP" > target/netty.cp
$JAR --create --file target/RakNetLib.jar -C target/classes .
if [ -n "$NETTY_CP" ]; then
    echo "OK -> target/RakNetLib.jar (transporte Netty $NETTY_VERSION, con sockets de respaldo)"
else
    echo "OK -> target/RakNetLib.jar (transporte de sockets)"
fi
