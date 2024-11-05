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
Main module for distributed execution of JAR files on Spark.
"""

import os
import subprocess
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Tuple

from pyspark import SparkFiles

from spark_rapids_pytools.rapids.tools_submission_cmd import ToolSubmissionCommand
from spark_rapids_tools.tools.distributed.hdfs_manager import HdfsManager, InputFsManager
from spark_rapids_tools.tools.distributed.result_combiner import ResultCombiner
from spark_rapids_tools.tools.distributed.spark_job_manager import SparkJobManager
from spark_rapids_tools.tools.distributed.status_reporter import AppStatusResult, AppStatus

SPARK_HOME = os.environ.get('SPARK_HOME')
HADOOP_HOME = os.environ.get('HADOOP_HOME')
JAVA_HOME = os.environ.get('JAVA_HOME')


@dataclass
class DistributedJarExecutor:
    """
    Class to orchestrate the execution of the Tools JAR on Spark.
    """

    spark_config_file: str = field(init=True)
    submission_cmd: ToolSubmissionCommand = field(init=True)
    hdfs_manager: HdfsManager = field(init=False)
    spark_manager: SparkJobManager = field(init=False)
    input_fs_manager: InputFsManager = field(init=False)
    rapids_args: List[str] = field(init=False)
    event_logs_path: str = field(init=False)

    def __post_init__(self):
        assert SPARK_HOME, 'SPARK_HOME environment variable is not set.'
        assert HADOOP_HOME, 'HADOOP_HOME environment variable is not set.'
        assert JAVA_HOME, 'JAVA_HOME environment variable is not set.'
        self.rapids_args = self.submission_cmd.extra_rapids_args[:-1]
        self.event_logs_path = self.submission_cmd.extra_rapids_args[-1]

    def run_as_spark_app(self):
        try:
            self._run_as_spark_app_internal()
        except Exception as e:  # pylint: disable=broad-except
            exception_msg = f'Failed to run the tool as a Spark application: {str(e)}'
            failed_app = AppStatusResult(path=self.event_logs_path, status=AppStatus.FAILURE, message=exception_msg)
            failed_app.write_to_csv(self.submission_cmd.output_folder, self.hdfs_manager.get_local_fs())

    def _run_as_spark_app_internal(self):
        output_folder_name = os.path.basename(self.submission_cmd.output_folder)

        self.hdfs_manager = HdfsManager(output_folder_name=output_folder_name)

        # TODO: Add support for other file systems as input paths (e.g., S3, GCS)
        self.input_fs_manager = InputFsManager(self.hdfs_manager.get_hdfs_fs())
        eventlog_files = self.input_fs_manager.get_files_from_path(self.event_logs_path)

        self.spark_manager = SparkJobManager(self.spark_config_file,
                                             self.submission_cmd.dependencies_paths,
                                             self.submission_cmd.jvm_log_file,
                                             self.submission_cmd.output_folder)
        run_jar_command = self._create_run_jar_map_func(self.hdfs_manager.executor_output_path)
        self.spark_manager.submit_map_job(map_func=run_jar_command, input_list=eventlog_files)

        result_combiner = ResultCombiner(output_folder=self.submission_cmd.output_folder,
                                         executor_output_dir=self.hdfs_manager.executor_output_path,
                                         hdfs_fs=self.hdfs_manager.get_hdfs_fs())
        result_combiner.combine_results()

        self._cleanup()

    def _create_run_jar_map_func(self, hdfs_base_dir: str):
        def run_jar_map_func(file_path: str):
            logs = [f'Processing {file_path}']

            # Generate unique executor output directory
            executor_output_dir = os.path.join(hdfs_base_dir, os.path.basename(file_path))
            logs.append(f'Executor output directory: {executor_output_dir}')

            # Run the JAR command
            jar_command = self._get_jar_command(file_path, executor_output_dir)
            exec_logs, app_status = self._submit_jar_cmd(jar_command)
            logs.extend(exec_logs)
            if app_status.status == AppStatus.FAILURE:
                app_status.write_to_csv(executor_output_dir, self.hdfs_manager.get_hdfs_fs())
            return logs, executor_output_dir

        return run_jar_map_func

    def _get_jar_command(self, file_path: str, executor_output_dir: str) -> List[str]:
        local_deps_path = [SparkFiles.get(os.path.basename(dep)) for dep in self.submission_cmd.dependencies_paths]
        local_deps_path.append(self.submission_cmd.hadoop_classpath)
        local_deps_path.append(f'{SPARK_HOME}/jars/*')
        jars = ':'.join(local_deps_path)

        java_exec = f'{os.environ["JAVA_HOME"]}/bin/java'
        local_jvm_log_file = SparkFiles.get(os.path.basename(self.submission_cmd.jvm_log_file))

        # Update JVM log configuration
        jvm_log_file_index = next(
            i for i, arg in enumerate(self.submission_cmd.jvm_args) if '-Dlog4j.configuration' in arg)
        self.submission_cmd.jvm_args[jvm_log_file_index] = f'-Dlog4j.configuration=file:{local_jvm_log_file}'

        tool_args = ['--output-directory', executor_output_dir, file_path]

        return [java_exec] + self.submission_cmd.jvm_args + ['-cp', jars, self.submission_cmd.jar_main_class] \
            + self.rapids_args + tool_args

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

    def _cleanup(self):
        if self.spark_manager:
            self.spark_manager.cleanup()
