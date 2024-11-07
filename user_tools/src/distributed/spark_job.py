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
"""
Module containing the JarRunner class to run a JAR file in a Spark job.
These functions will be executed by the workers in the cluster.
"""

import os
import socket
import subprocess
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Tuple

from pyspark import SparkFiles
from distributed.status_reporter import AppStatus, AppStatusResult


@dataclass
class SparkJobConfig:
    """ Configuration for a Spark job. """

    output_dir: str
    dependencies_paths: List[str]
    hadoop_classpath: str
    jvm_log_file: str
    jvm_args: List[str]
    jar_main_class: str
    rapids_args: List[str]


@dataclass
class SparkJobRunner:
    """ Class to run a JAR file in a Spark job. """

    config: SparkJobConfig = field(init=True)

    def create_run_jar_map_func(self):
        """
        Creates a function to be used as a map function for running JAR files.
        """
        def run_jar_map_func(file_path: str):
            hostname = socket.gethostname()
            logs = [f'Host: {hostname}, Processing file: {file_path}']

            # Generate unique executor output directory
            executor_output_dir = os.path.join(self.config.output_dir, os.path.basename(file_path))
            logs.append(f'Executor output directory: {executor_output_dir}')

            # Run the JAR command
            jar_command = self._get_jar_command(file_path, executor_output_dir)
            exec_logs, app_status = self._submit_jar_cmd(jar_command)
            # if app_status.status == AppStatus.FAILURE:
            #     app_status.write_to_csv(executor_output_dir, self.hdfs_manager.get_fs())
            logs.extend(exec_logs)
            return logs, app_status

        return run_jar_map_func

    def _get_jar_command(self, file_path: str, executor_output_dir: str) -> List[str]:
        """
        Generates the command to run the JAR file with the necessary arguments.
        """
        local_deps_path = [SparkFiles.get(os.path.basename(dep)) for dep in self.config.dependencies_paths]
        local_deps_path.append(self.config.hadoop_classpath)
        local_deps_path.append(f'{os.getenv("SPARK_HOME")}/jars/*')
        jars = ':'.join(local_deps_path)

        java_exec = f'{os.environ["JAVA_HOME"]}/bin/java'
        local_jvm_log_file = SparkFiles.get(os.path.basename(self.config.jvm_log_file))

        # Update JVM log configuration
        jvm_log_file_index = next(
            i for i, arg in enumerate(self.config.jvm_args) if '-Dlog4j.configuration' in arg
        )
        self.config.jvm_args[jvm_log_file_index] = f'-Dlog4j.configuration=file:{local_jvm_log_file}'

        tool_args = ['--output-directory', executor_output_dir, file_path]

        return ([java_exec] + self.config.jvm_args + ['-cp', jars, self.config.jar_main_class]
                + self.config.rapids_args + tool_args)

    @staticmethod
    def _submit_jar_cmd(jar_command: List[str]) -> Tuple[List[str], AppStatusResult]:
        """
        Executes a JAR command and captures its status, output, and execution time.

        :param jar_command: The JAR command to execute.
        :return: A tuple containing the logs generated during the execution and the status of the application.
        """
        logs = []
        start_time = datetime.now()
        command_str = ' '.join(jar_command)

        logs.append(f'Starting execution of command: {command_str}')
        try:
            result = subprocess.run(jar_command, check=True, capture_output=True, text=True)
            logs.append('Command succeeded.')

            if result.stdout:
                logs.append(f'stdout:\n{result.stdout}')
            if result.stderr:
                logs.append(f'stderr:\n{result.stderr}')

            app_status = AppStatusResult(path=jar_command[-1],
                                         status=AppStatus.SUCCESS if result.returncode == 0 else AppStatus.FAILURE,
                                         message=result.stderr if result.returncode != 0 else '')
        except subprocess.CalledProcessError as ex:
            logs.append(f'Command failed with exit code {ex.returncode}.')
            if ex.stdout:
                logs.append(f'stdout:\n{ex.stdout}')
            if ex.stderr:
                logs.append(f'stderr:\n{ex.stderr}')
            app_status = AppStatusResult(path=jar_command[-1], status=AppStatus.FAILURE,
                                         message=ex.stderr or 'Error during command execution.')
        except Exception as ex:  # pylint: disable=broad-except
            logs.append(f'Unexpected error: {ex}')
            app_status = AppStatusResult(path=jar_command[-1], status=AppStatus.FAILURE,
                                         message=str(ex))

        finally:
            processing_time = datetime.now() - start_time
            logs.append(f'Total processing time: {processing_time}')

        return logs, app_status
