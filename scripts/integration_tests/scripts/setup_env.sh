#!/bin/bash

if [ -z "$TOOLS_DIR" ]; then
  echo "ERROR: The TOOLS_DIR environment variable is not set." >&2
  echo "Please set TOOLS_DIR to the root directory of the spark-rapids-tools repository. Exiting script." >&2
  exit 1
fi

echo "Building JAR file and installing Python package"
#./scripts/build_jar.sh "$TOOLS_DIR"
./scripts/install_python_package.sh "$TOOLS_DIR"