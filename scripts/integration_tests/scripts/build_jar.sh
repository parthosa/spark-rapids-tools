#!/bin/bash

if [ -z "$TOOLS_DIR" ]; then
  echo "ERROR: The TOOLS_DIR environment variable is not set." >&2
  echo "Please set TOOLS_DIR to the root directory of the spark-rapids-tools repository. Exiting script." >&2
  exit 1
fi

JAR_TOOLS_DIR="$TOOLS_DIR/core"

# Build JAR file using Maven
echo "Building Spark RAPIDS Tools JAR file"
pushd "$JAR_TOOLS_DIR" || exit
mvn install -DskipTests
popd || exit

