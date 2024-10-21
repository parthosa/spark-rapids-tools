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
    hdfs_base_dir: str = field(init=False)
    hadoop_home: str = field(init=False, default=os.getenv("HADOOP_HOME"))

    def __post_init__(self):
        if not self.hadoop_home:
            raise ValueError("HADOOP_HOME environment variable is not set")
        self.hdfs_base_dir = self.get_hdfs_nn_addr() + os.path.join(
            Utilities.get_cache_dir(),
            self.output_folder_name,
            Utilities.get_executor_output_dir_name()
        )

    def init_setup(self):
        """Initial setup to remove any existing directory and create a new one in HDFS."""
        try:
            self._run_hdfs_command(
                ["dfs", "-rm", "-r", self.hdfs_base_dir],
                f"Removing HDFS directory {self.hdfs_base_dir}"
            )
        except Exception:  # pylint: disable=broad-except
            pass
        self._run_hdfs_command(
            ["dfs", "-mkdir", "-p", self.hdfs_base_dir],
            f"Creating HDFS directory {self.hdfs_base_dir}"
        )

    @classmethod
    def get_hdfs_nn_addr(cls) -> str:
        """Get the HDFS NameNode address."""
        return cls._run_hdfs_command(
            ["getconf", "-confKey", "fs.defaultFS"],
            "Getting HDFS NameNode address"
        ).stdout.strip()

    def copy_output_hdfs_to_local(self, executor_output_dir: str):
        """Copy the output from HDFS to a local directory."""
        self._run_hdfs_command(
            ["dfs", "-copyToLocal", self.hdfs_base_dir, executor_output_dir],
            f"Copying HDFS output {self.hdfs_base_dir} to directory {executor_output_dir}"
        )

    @staticmethod
    def _run_hdfs_command(hdfs_args: list, description: str):
        """Run an HDFS command and log its description."""
        hadoop_home = os.getenv("HADOOP_HOME")
        if not hadoop_home:
            raise ValueError("HADOOP_HOME environment variable is not set")

        command = [f"{hadoop_home}/bin/hdfs"] + hdfs_args
        try:
            return Utilities.run_cmd(command, description)
        except Exception as e:
            raise RuntimeError(f"Failed to run HDFS command: {description}, Error: {str(e)}")


@dataclass
class InputFsManager:
    input_fs: fs.FileSystem = field(init=False)

    def __post_init__(self):
        self._init_env()
        self.input_fs = fs.HadoopFileSystem("default")

    @staticmethod
    def _init_env():
        """Set up the HADOOP classpath environment variable."""
        hadoop_home = os.getenv("HADOOP_HOME")
        if not hadoop_home:
            raise ValueError("HADOOP_HOME environment variable is not set")
        try:
            classpath_output = subprocess.check_output(
                [f"{hadoop_home}/bin/hadoop", "classpath", "--glob"]
            ).decode().strip()
            os.environ['CLASSPATH'] = classpath_output
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Error retrieving Hadoop classpath: {e}")

    @staticmethod
    def extract_directory(directory: str) -> str:
        """Extract the path from a HDFS URL if present."""
        return urlparse(directory).path if directory.startswith("hdfs://") else directory

    def get_files_from_path(self, directory: str) -> list:
        """Retrieve the list of files from a given directory in HDFS."""
        directory_path = self.extract_directory(directory)
        hdfs_nn_addr = HdfsManager.get_hdfs_nn_addr()

        file_infos = self.input_fs.get_file_info(fs.FileSelector(directory_path, recursive=False))
        return [f"{hdfs_nn_addr}{file_info.path}" for file_info in file_infos if file_info.type == fs.FileType.File]