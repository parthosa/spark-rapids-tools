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

import os
import glob
import re
from dataclasses import dataclass, field
from typing import List


@dataclass
class ToolSubmissionCommand:
    jvm_args: List[str]
    classpath_arr: List[str]
    hadoop_classpath: str
    jar_main_class: str
    rapids_arguments: List[str]
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
        exclusion_regex = r'(spark-\d+\.\d+\.\d+-bin-hadoop\d+|/[^/]*hadoop[^/]*)'  # Filter out spark and hadoop jars
        classpath_arr_list = self.classpath_arr[1].split(':')
        self.dependencies_paths = [path for path in classpath_arr_list if not re.search(exclusion_regex, path)]

    def build_cmd_local(self) -> List[str]:
        """
        Constructs the command for running the application in a local environment.
        """
        cmd_arg = ['java']
        cmd_arg.extend(self.jvm_args)
        cmd_arg.extend(self.classpath_arr)
        cmd_arg.append(self.jar_main_class)
        cmd_arg.extend(self.rapids_arguments)
        cmd_arg.extend(self.extra_rapids_args)
        return cmd_arg

    def build_cmd_distributed(self) -> List[str]:
        """
        Constructs the command for running the application in a distributed environment.
        """
        cmd_arg = ['java']
        cmd_arg.extend(self.jvm_args)
        cmd_arg.extend(self.classpath_arr)
        cmd_arg.append(self.jar_main_class)
        cmd_arg.extend(self.rapids_arguments)
        cmd_arg.extend(self.extra_rapids_args)

        # Add any additional arguments specific to the distributed environment here
        cmd_arg.append("--mode=distributed")
        cmd_arg.append("--master=yarn")  # Example for Hadoop/YARN; adjust as needed

        return cmd_arg

    def extract_classpath_files(self) -> dict:
        """
        Extracts the list of files from the classpath.
        """
        result_map = {}
        paths = self.classpath_arr[1].split(':')

        # Process each path
        for path in paths:
            # Use glob to expand wildcards and match file patterns
            matched_files = glob.glob(path)

            for file in matched_files:
                if os.path.isfile(file):
                    # Extract the base directory dynamically from the file path
                    common_base_path = os.path.commonpath([file]).split("qual_")[1]
                    # The part after the "qual_" would serve as the base for the key
                    filename = os.path.basename(file)
                    relative_path = f"qual_{common_base_path}/{filename}"
                    result_map[relative_path] = file
        return result_map
