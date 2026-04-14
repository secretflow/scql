#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR/java-flight-sql-server"
MAVEN_VERSION="3.9.9"
MAVEN_DIR="$PROJECT_DIR/.tools/apache-maven-$MAVEN_VERSION"
JAR_PATH="$PROJECT_DIR/target/scql-hive-flight-sql-server.jar"

ensure_maven() {
  if command -v mvn >/dev/null 2>&1; then
    command -v mvn
    return
  fi

  if [ ! -x "$MAVEN_DIR/bin/mvn" ]; then
    mkdir -p "$PROJECT_DIR/.tools"
    ARCHIVE="$PROJECT_DIR/.tools/apache-maven-$MAVEN_VERSION-bin.tar.gz"
    URL="https://archive.apache.org/dist/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz"
    echo "Downloading Maven $MAVEN_VERSION..." >&2
    curl -fsSL "$URL" -o "$ARCHIVE"
    tar -xzf "$ARCHIVE" -C "$PROJECT_DIR/.tools"
  fi

  echo "$MAVEN_DIR/bin/mvn"
}

MVN_BIN="$(ensure_maven)"

if [ ! -f "$JAR_PATH" ] || [ "${SCQL_FORCE_REBUILD:-0}" = "1" ]; then
  echo "Building Java Flight SQL server..."
  "$MVN_BIN" -q -f "$PROJECT_DIR/pom.xml" -DskipTests package
fi

exec java -jar "$JAR_PATH" "$@"
