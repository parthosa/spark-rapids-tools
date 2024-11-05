# Copyright (c) 2024, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" Utility functions for distributed tools """

import subprocess

from pyarrow import fs


class Utilities:
    """ Utility functions for distributed tools """

    # Utility function to run shell commands with error handling
    @classmethod
    def run_cmd(cls, command, description=None) -> subprocess.CompletedProcess:
        try:
            res = subprocess.run(command, check=True, capture_output=True, text=True)
            if description:
                print(f'SUCCESS: {description or " ".join(command)}')
            return res
        except subprocess.CalledProcessError as e:
            if description:
                raise Exception(f'ERROR: {description or " ".join(command)}\n{e.stderr}') from e
            raise Exception(f'ERROR: {" ".join(command)}\n{e.stderr}') from e

    @classmethod    # Utility function to check if a command is available
    def check_cmd_availability(cls, cmd, check_cmd):
        error_msg = f'{cmd} is not available'

        def run_and_check(command):
            """Helper function to run a command and check its return code."""
            result = Utilities.run_cmd(command)
            if result.returncode != 0:
                raise FileNotFoundError(error_msg)

        try:
            run_and_check(['which', cmd])
            run_and_check(check_cmd)
        except FileNotFoundError as e:
            raise e

    @classmethod
    def parse_memory_size(cls, memory_str: str) -> float:
        """
        Helper function to convert JVM memory string to float in gigabytes.
        """
        if not memory_str or len(memory_str) < 2:
            raise ValueError("Memory size string must include a value and a unit (e.g., '512m', '2g').")

        unit = memory_str[-1].lower()
        size_value = float(memory_str[:-1])

        if unit == 'g':
            return size_value
        if unit == 'm':
            return size_value / 1024  # Convert MB to GB
        if unit == 'k':
            return size_value / (1024 ** 2)  # Convert KB to GB

        raise ValueError(f"Invalid memory unit '{unit}' in memory size: '{memory_str}'")

    @classmethod
    def resource_exists(cls, resource_path: str, input_fs: fs.FileSystem) -> bool:
        try:
            file_info = input_fs.get_file_info(resource_path)
            return file_info.type in {fs.FileType.File, fs.FileType.Directory}
        except FileNotFoundError:
            return False
