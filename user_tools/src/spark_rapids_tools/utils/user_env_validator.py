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

""" Validate the user environment """

import sys

from packaging.specifiers import SpecifierSet

import spark_rapids_pytools
from spark_rapids_tools.exceptions import UnsupportedPythonVersionException, UnsupportedOSException


class UserEnvValidator:
    """ Validate the user environment"""

    @classmethod
    def _validate_python_version(cls) -> None:
        requires_python = spark_rapids_pytools.requires_python
        specifier = SpecifierSet(requires_python)
        current_version = sys.version.split(" ")[0]
        if current_version not in specifier:
            raise UnsupportedPythonVersionException(current_version, requires_python)

    @classmethod
    def _validate_os(cls) -> None:
        requires_os = spark_rapids_pytools.requires_os
        current_os = sys.platform
        if current_os not in requires_os:
            raise UnsupportedOSException(current_os, requires_os)

    @classmethod
    def validate(cls) -> None:
        cls._validate_os()
        cls._validate_python_version()
