# Copyright (c) 2024, NVIDIA CORPORATION.
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

import subprocess


class Utilities:
    # Utility function to run shell commands with error handling
    def run_cmd(command, description=None) -> subprocess.CompletedProcess:
        try:
            res = subprocess.run(command, check=True, capture_output=True, text=True)
            if description:
                print(f"SUCCESS: {description or ' '.join(command)}")
            return res
        except subprocess.CalledProcessError as e:
            if description:
                raise Exception(f"ERROR: {description or ' '.join(command)}\n{e.stderr}")
            else:
                raise Exception(f"ERROR: {' '.join(command)}\n{e.stderr}")

    def check_cmd_availability(cmd, check_cmd):
        error_msg = f'{cmd} is not available'

        def run_and_check(command):
            """Helper function to run a command and check its return code."""
            result = Utilities.run_cmd(command)
            if result.returncode != 0:
                raise FileNotFoundError(error_msg)

        try:
            run_and_check(["which", cmd])
            run_and_check(check_cmd)
        except FileNotFoundError as e:
            raise e
