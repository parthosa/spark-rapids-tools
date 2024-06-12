import subprocess
import os
import shutil
from typing import List

import pytest
from pathlib import Path
import sys
import tempfile


@pytest.fixture
def temp_output_dir():
    output_dir = tempfile.mkdtemp()
    yield output_dir
    shutil.rmtree(output_dir)


def set_environment_variables():
    """
    Set environment variables needed for the virtual environment setup.
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    tools_ver = '24.06.1'
    venv_name = 'spark_rapids_pytools_env'

    os.environ['TOOLS_DIR'] = Path(current_dir).parents[2].as_posix()
    os.environ['VENV_DIR'] = os.path.join(os.environ['TOOLS_DIR'], 'scripts', 'integration_tests', venv_name)
    os.environ['TOOLS_JAR_PATH'] = os.path.join(os.environ['TOOLS_DIR'],
                                                f'core/target/rapids-4-spark-tools_2.12-{tools_ver}-SNAPSHOT.jar')


def clear_environment_variables():
    """
    Clear environment variables set for the virtual environment setup.
    """
    keys = ['TOOLS_DIR', 'VENV_DIR', 'TOOLS_JAR_PATH']
    for key in keys:
        if key in os.environ:
            del os.environ[key]


@pytest.fixture(scope='function')
def virtualenv():
    set_environment_variables()
    result = subprocess.run(['./scripts/setup_env.sh'], text=True, env=os.environ, stdout=sys.stdout, stderr=sys.stderr)
    if result.returncode != 0:
        pytest.fail(f"Failed to create virtual environment. Reason: {result.stderr}")
    try:
        yield
    finally:
        # Clean up the temporary directory
        # shutil.rmtree(env['env_dir'])
        clear_environment_variables()
        pass


def get_spark_rapids_cli():
    return os.path.join(os.environ['VENV_DIR'], 'bin', 'spark_rapids')


def get_tools_jar_path():
    return os.environ['TOOLS_JAR_PATH']


def run_test(cmd: List[str], expected_output: List[str] = None, expected_error: List[str] = None,
             expected_return_code: int = 0):
    """
    Run a subprocess command and validate the output, error, and return code.

    Args:
        cmd (List[str]): The command to run.
        expected_output (List[str], optional): List of expected strings in stdout.
        expected_error (List[str], optional): List of expected strings in stderr.
        expected_return_code (int, optional): The expected return code.

    Raises:
        AssertionError: If the actual output, error, or return code does not match the expected values.
        pytest.fail: If the command fails to run with the expected return code.
    """
    expected_output = expected_output or []
    expected_error = expected_error or []
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as e:
        if e.returncode != expected_return_code:
            pytest.fail(f"Command '{e.cmd}' returned non-zero exit status {e.returncode}. Reason: {e.stderr}")
        result = e

    assert result.returncode == expected_return_code, (
        f"Expected return code {expected_return_code}, but got {result.returncode}."
    )

    # Check stdout
    for line in expected_output:
        assert line in result.stdout, f"Expected '{line}' in stdout, but it was not found."

    # Check stderr
    for line in expected_error:
        assert line in result.stderr, f"Expected '{line}' in stderr, but it was not found."

