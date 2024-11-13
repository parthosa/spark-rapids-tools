# Copyright (c) 2023-2024, NVIDIA CORPORATION.
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

""" ToolSubmissionCommand class definition """
import logging
import os
import re
from dataclasses import dataclass, field
from typing import List


@dataclass
class ToolSubmissionCommand:
    """
    Wrapper class to store the arguments required to run the Tools JAVA application.
    """

    jvm_args: List[str]
    classpath_arr: List[str]
    hadoop_classpath: str
    jar_main_class: str
    output_dir_args: List[str]
    extra_rapids_args: List[str]
    output_folder: str
    work_dir: str
    jvm_log_file: str = field(default=None, init=False)
    dependencies_paths: List[str] = field(default=None, init=False)

    def __post_init__(self):
        for arg in self.jvm_args:
            # check for log4j properties file
            if 'Dlog4j.configuration' in arg:
                self.jvm_log_file = arg.split('=')[1]
        # We need to filter out the hadoop and spark jars from the classpath
        exclusion_regex = r'(spark-\d+\.\d+\.\d+-bin-hadoop\d+|/[^/]*hadoop[^/]*)'
        classpath_arr_list = self.classpath_arr[1].split(':')
        self.dependencies_paths = []
        for path in classpath_arr_list:
            file_name = os.path.basename(path)
            if not (re.search(exclusion_regex, file_name) or os.path.isdir(path) or file_name == '*'):
                self.dependencies_paths.append(path)
        logging.info('Filtered dependencies paths: %s', list(self.dependencies_paths))

    def build_cmd_local(self) -> List[str]:
        """
        Constructs the command for running the application in a local environment.
        """
        cmd_arg = ['java']
        cmd_arg.extend(self.jvm_args)
        cmd_arg.extend(self.classpath_arr)
        cmd_arg.append(self.jar_main_class)
        cmd_arg.extend(self.output_dir_args)
        cmd_arg.extend(self.extra_rapids_args)
        return cmd_arg
