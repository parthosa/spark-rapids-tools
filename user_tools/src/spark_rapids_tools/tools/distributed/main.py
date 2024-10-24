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
from datetime import datetime
from typing import List

from pyspark import SparkFiles

from spark_rapids_pytools.rapids.tools_submission_cmd import ToolSubmissionCommand
from spark_rapids_tools.tools.distributed.hdfs_manager import HdfsManager, InputFsManager
from spark_rapids_tools.tools.distributed.result_combiner import ResultCombiner
from spark_rapids_tools.tools.distributed.spark_job_manager import SparkJobManager

SPARK_HOME = os.environ.get("SPARK_HOME")
HADOOP_HOME = os.environ.get("HADOOP_HOME")
JAVA_HOME = os.environ.get("JAVA_HOME")


@dataclass
class DistributedJarExecutor:
    spark_master: str = field(init=True)
    submission_cmd: ToolSubmissionCommand = field(init=True)
    hdfs_manager: HdfsManager = field(init=False)
    spark_manager: SparkJobManager = field(init=False)
    input_fs_manager: InputFsManager = field(init=False)

    def __post_init__(self):
        assert SPARK_HOME, "SPARK_HOME environment variable is not set."
        assert HADOOP_HOME, "HADOOP_HOME environment variable is not set."
        assert JAVA_HOME, "JAVA_HOME environment variable is not set."
        self.rapids_args = self.submission_cmd.extra_rapids_args[:-1]
        self.event_logs_path = self.submission_cmd.extra_rapids_args[-1]

    def run_tool_as_spark_app(self):
        output_folder_name = os.path.basename(self.submission_cmd.output_folder)

        self.hdfs_manager = HdfsManager(output_folder_name=output_folder_name)
        self.hdfs_manager.init_setup()

        self.input_fs_manager = InputFsManager()
        eventlog_files = self.input_fs_manager.get_files_from_path(self.event_logs_path)

        self.spark_manager = SparkJobManager(self.spark_master,
                                             self.submission_cmd.dependencies_paths,
                                             self.submission_cmd.jvm_log_file,
                                             self.submission_cmd.output_folder)
        run_jar_command = self._create_run_jar_map_func(self.hdfs_manager.executor_output_path)
        self.spark_manager.submit_map_job(map_func=run_jar_command, input_list=eventlog_files)

        result_combiner = ResultCombiner(output_folder=self.submission_cmd.output_folder,
                                         executor_output_dir=self.hdfs_manager.executor_output_path)
        result_combiner.combine_results()

        self._cleanup()

    def _create_run_jar_map_func(self, hdfs_base_dir: str):
        def run_jar_map_func(file_path: str):
            logs = [f"Processing {file_path}"]

            # Generate unique executor output directory
            executor_output_dir = os.path.join(hdfs_base_dir, os.path.basename(file_path))
            logs.append(f"Executor output directory: {executor_output_dir}")

            # Run the JAR command
            jar_command = self._get_jar_command(file_path, executor_output_dir)
            logs.extend(self._submit_jar_cmd(jar_command))
            return logs, executor_output_dir
        return run_jar_map_func

    def _get_jar_command(self, file_path: str, executor_output_dir: str) -> List[str]:
        local_deps_path = [SparkFiles.get(os.path.basename(dep)) for dep in self.submission_cmd.dependencies_paths]
        local_deps_path.append(f'{SPARK_HOME}/jars/*')
        jars = ":".join(local_deps_path)

        java_exec = f"{os.environ['JAVA_HOME']}/bin/java"
        local_jvm_log_file = SparkFiles.get(os.path.basename(self.submission_cmd.jvm_log_file))

        # Update JVM log configuration
        jvm_log_file_index = next(i for i, arg in enumerate(self.submission_cmd.jvm_args) if "-Dlog4j.configuration" in arg)
        self.submission_cmd.jvm_args[jvm_log_file_index] = f"-Dlog4j.configuration=file:{local_jvm_log_file}"

        tool_args = ["--output-directory", executor_output_dir, file_path]

        return [java_exec] + self.submission_cmd.jvm_args + ["-cp", jars, self.submission_cmd.jar_main_class] + self.rapids_args + tool_args

    @staticmethod
    def _submit_jar_cmd(jar_command: List[str]) -> list:
        logs = []
        start_time = datetime.now()
        command_str = ' '.join(jar_command)

        logs.append(f"Starting execution of command: {command_str}")
        try:
            result = subprocess.run(jar_command, check=True, capture_output=True, text=True)
            logs.append(f"Command succeeded with stdout:\n{result.stdout}")
            if result.stderr:
                logs.append(f"Command stderr:\n{result.stderr}")
        except subprocess.CalledProcessError as ex:
            logs.append(f"Command failed with exit code {ex.returncode}.")
            if ex.stdout:
                logs.append(f"Command stdout:\n{ex.stdout}")
            if ex.stderr:
                logs.append(f"Command stderr:\n{ex.stderr}")
        finally:
            processing_time = datetime.now() - start_time
            logs.append(f"Total processing time: {processing_time}")

        return logs

    def _cleanup(self):
        if self.spark_manager:
            self.spark_manager.cleanup()
