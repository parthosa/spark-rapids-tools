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

""" HDFS Manager module for managing HDFS operations. """

import os
import subprocess
from dataclasses import dataclass, field
from urllib.parse import urlparse

from pyarrow import fs

from spark_rapids_tools.tools.distributed.utils import Utilities


@dataclass
class HdfsManager:
    """ HDFS Manager class for managing HDFS operations. """
    output_folder_name: str
    executor_output_path: str = field(init=False)
    hdfs_fs: fs.HadoopFileSystem = field(init=False)

    _hdfs_scheme: str = 'hdfs'

    def __post_init__(self):
        assert os.getenv('HADOOP_HOME') is not None, 'HADOOP_HOME environment variable is not set'
        # Set the CLASSPATH environment variable. This is required by pyarrow to access HDFS.
        try:
            result = self._run_hdfs_command(['classpath', '--glob'], 'Setting CLASSPATH')
            os.environ['CLASSPATH'] = result.stdout.strip()
        except subprocess.CalledProcessError as e:
            raise RuntimeError('Error retrieving Hadoop classpath') from e

        self.hdfs_fs = fs.HadoopFileSystem('default')
        executor_output_path_raw = Utilities.get_executor_output_path(self.output_folder_name)
        self.executor_output_path = f'{self._hdfs_scheme}:///{executor_output_path_raw.strip("/")}'
        self.hdfs_fs.create_dir(self.executor_output_path, recursive=True)

    @staticmethod
    def _run_hdfs_command(cmd_args: list, description: str):
        """Run an HDFS command and log its description."""
        command = [f'{os.getenv("HADOOP_HOME")}/bin/hdfs'] + cmd_args
        try:
            return Utilities.run_cmd(command, description)
        except Exception as e:
            raise RuntimeError(f'Failed to run HDFS command: {description}, Error: {str(e)}') from e

    def get_hdfs_fs(self) -> fs.HadoopFileSystem:
        return self.hdfs_fs

    @staticmethod
    def get_local_fs() -> fs.LocalFileSystem:
        return fs.LocalFileSystem()


@dataclass
class InputFsManager:
    """ Input FileSystem Manager class for managing file operations. """

    input_fs: fs.FileSystem = field(init=True)

    def get_files_from_path(self, directory: str) -> list:
        """Retrieve the list of files from a given directory in HDFS."""
        parsed_url = urlparse(directory)
        file_infos = self.input_fs.get_file_info(fs.FileSelector(parsed_url.path))
        uris = []
        for info in file_infos:
            if info.type == fs.FileType.File:
                uri = f'{parsed_url.scheme}://{parsed_url.netloc}/{info.path.strip("/")}'
                uris.append(uri)
        return uris
