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

import os
import subprocess
from dataclasses import dataclass, field
from urllib.parse import urlparse

import pyarrow.fs as fs

from spark_rapids_tools.tools.distributed.utils import Utilities


@dataclass
class HdfsManager:
    output_folder_name: str
    executor_output_path: str = field(init=False)
    _HDFS_SCHEME: str = "hdfs"

    def __post_init__(self):
        assert os.getenv("HADOOP_HOME") is not None, "HADOOP_HOME environment variable is not set"
        # Set the CLASSPATH environment variable. This is required by pyarrow to access HDFS.
        try:
            result = self._run_hdfs_command(["classpath", "--glob"], "Setting CLASSPATH")
            os.environ['CLASSPATH'] = result.stdout.strip()
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Error retrieving Hadoop classpath") from e

        executor_output_path_raw = Utilities.get_executor_output_path(self.output_folder_name)
        self.executor_output_path = f"{self._HDFS_SCHEME}:///{executor_output_path_raw.strip('/')}"

    @staticmethod
    def _run_hdfs_command(cmd_args: list, description: str):
        """Run an HDFS command and log its description."""
        command = [f"{os.getenv('HADOOP_HOME')}/bin/hdfs"] + cmd_args
        try:
            return Utilities.run_cmd(command, description)
        except Exception as e:
            raise RuntimeError(f"Failed to run HDFS command: {description}, Error: {str(e)}")


@dataclass
class InputFsManager:
    input_fs: fs.FileSystem = field(init=False)

    def __post_init__(self):
        self.input_fs = fs.HadoopFileSystem("default")

    def get_files_from_path(self, directory: str) -> list:
        """Retrieve the list of files from a given directory in HDFS."""
        parsed_url = urlparse(directory)
        file_infos = self.input_fs.get_file_info(fs.FileSelector(parsed_url.path, recursive=False))
        uris = []
        for info in file_infos:
            if info.type == fs.FileType.File:
                uri = f"{parsed_url.scheme}://{parsed_url.netloc}/{info.path.strip('/')}"
                uris.append(uri)
        return uris
