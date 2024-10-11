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
from utils import Utilities
from dataclasses import dataclass, field
import pyarrow.fs as fs
import subprocess
from urllib.parse import urlparse

@dataclass
class HdfsManager:
    local_output_dir: str = field(init=True)
    hdfs_base_dir: str = field(init=False)
    HADOOP_HOME: str = "/home/psarthi/Work/hadoop"

    def init_hdfs_directory(self):
        hdfs_nn_addr = self.get_hdfs_nn_addr()
        self.hdfs_base_dir = f"{hdfs_nn_addr}/{self.local_output_dir}"

    def prepare_hdfs_directory(self):
        # Check if HDFS directory exists
        check_command = [f"{self.HADOOP_HOME}/bin/hadoop", "fs", "-test", "-d", self.hdfs_base_dir]
        try:
            Utilities.run_cmd(check_command)
            dir_exists = True
        except Exception as e:
            dir_exists = False

        # Delete if exists
        if dir_exists:
            delete_command = [f"{self.HADOOP_HOME}/bin/hadoop", "fs", "-rm", "-r", self.hdfs_base_dir]
            Utilities.run_cmd(delete_command, description=f"Removing HDFS directory {self.hdfs_base_dir}")

        # Create the directory
        mkdir_command = [f"{self.HADOOP_HOME}/bin/hadoop", "fs", "-mkdir", "-p", self.hdfs_base_dir]
        Utilities.run_cmd(mkdir_command, description=f"Creating HDFS directory {self.hdfs_base_dir}")

    def prepare_local_directory(self):
        # Delete local directory if exists
        if os.path.exists(self.local_output_dir):
            remove_command = ["rm", "-rf", self.local_output_dir]
            Utilities.run_cmd(remove_command, description=f"Removing local directory {self.local_output_dir}")

        # Create local directory
        mkdir_command = ["mkdir", "-p", self.local_output_dir]
        Utilities.run_cmd(mkdir_command, description=f"Creating local directory {self.local_output_dir}")

    def init_setup(self):
        self.init_hdfs_directory()
        self.prepare_hdfs_directory()
        self.prepare_local_directory()

    @staticmethod
    def get_hdfs_nn_addr():
        get_nn_addres_command = [f"{HdfsManager.HADOOP_HOME}/bin/hdfs", "getconf", "-confKey", "fs.defaultFS"]
        res = Utilities.run_cmd(get_nn_addres_command, description="Getting HDFS NameNode address")
        return res.stdout.strip()

    def copy_output_hdfs_to_local(self, local_dir):
        copy_to_local_command = [f"{self.HADOOP_HOME}/bin/hadoop", "fs", "-copyToLocal", self.hdfs_base_dir, local_dir]
        Utilities.run_cmd(copy_to_local_command, description=f"Copying HDFS output {self.hdfs_base_dir} to local")

    def __post_init__(self):
        check_command = [f"{self.HADOOP_HOME}/bin/hadoop", "fs", "-ls", "/"]
        # Utilities.check_cmd_availability("hadoop", check_command)


@dataclass
class InputFsManager:
    input_fs: fs.FileSystem = field(init=False)

    def __post_init__(self):
        self._init_env()
        self.input_fs = fs.HadoopFileSystem("default")

    def _init_env(self):
        hadoop_home = os.environ.get("HADOOP_HOME")
        if not hadoop_home:
            raise ValueError("HADOOP_HOME environment variable is not set")
        try:
            classpath_output = subprocess.check_output([f"{hadoop_home}/bin/hadoop", "classpath", "--glob"]).decode().strip()
            os.environ['CLASSPATH'] = classpath_output
        except subprocess.CalledProcessError as e:
            print(f"Error retrieving classpath: {e}")

    def extract_directory(self, directory):
        if directory.startswith("hdfs://"):
            hdfs_dir_parts = urlparse(directory)
            return hdfs_dir_parts.path
        return directory

    def get_files_from_path(self, directory):
        directory_path = self.extract_directory(directory)
        hdfs_nn_addr = HdfsManager.get_hdfs_nn_addr()
        files = []
        for file_info in self.input_fs.get_file_info(fs.FileSelector(directory_path, recursive=False)):
            if file_info.type == fs.FileType.File:
                full_path = f"{hdfs_nn_addr}{file_info.path}"
                files.append(full_path)
        return files
