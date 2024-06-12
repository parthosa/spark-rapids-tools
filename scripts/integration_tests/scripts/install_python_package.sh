#!/bin/bash
set -e

if [ -z "$TOOLS_DIR" ]; then
  echo "ERROR: The TOOLS_DIR environment variable is not set." >&2
  echo "Please set TOOLS_DIR to the root directory of the spark-rapids-tools repository. Exiting script." >&2
  exit 1
fi

if [ -z "$VENV_DIR" ]; then
  echo "ERROR: The VENV_DIR environment variable is not set." >&2
  echo "Please set VENV_DIR to the name of the virtual environment. Exiting script." >&2
  exit 1
fi

PYTHON_TOOLS_DIR="$TOOLS_DIR/user_tools"

echo "Setting up Python environment in $VENV_DIR"
python -m venv "$VENV_DIR"
source "$VENV_DIR"/bin/activate

echo "Installing Spark RAPIDS Tools Python package"
pushd "$PYTHON_TOOLS_DIR" || exit
pip install --upgrade pip setuptools wheel > /dev/null
pip install .
popd || exit

echo "$VENV_DIR"
