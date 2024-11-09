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
from dataclasses import dataclass, field
from typing import List
from urllib.parse import urlparse

import pandas as pd
from pyarrow import fs

from spark_rapids_pytools.common.prop_manager import YAMLPropertiesContainer
from spark_rapids_pytools.common.sys_storage import FSUtil
from spark_rapids_pytools.common.utilities import Utils
from spark_rapids_pytools.rapids.tools_submission_cmd import ToolSubmissionCommand
from spark_rapids_tools.tools.distributed.hdfs_manager import HdfsManager, InputFsManager, LocalFsManager, FsManager
from spark_rapids_tools.tools.distributed.result_combiner import ResultCombiner
from spark_rapids_tools.tools.distributed.spark_job_manager import SparkJobManager
from distributed.status_reporter import AppStatusResult, AppStatus
from distributed.spark_job import SparkJobConfig, SparkJobRunner


@dataclass
class DistributedJarExecutor:
    """
    Class to orchestrate the execution of the Tools JAR on Spark.
    """
    spark_config_file: str = field(init=True)
    platform: str = field(init=True)
    submission_cmd: ToolSubmissionCommand = field(init=True)
    hdfs_manager: HdfsManager = field(init=False)
    local_fs_manager: LocalFsManager = field(init=False)
    spark_manager: SparkJobManager = field(init=False)
    input_fs_manager: InputFsManager = field(init=False)
    rapids_args: List[str] = field(init=False)
    event_logs_path: str = field(init=False)
    props: YAMLPropertiesContainer = field(init=False)
    name: str = 'distributed-tool'

    def __post_init__(self):
        assert os.getenv('SPARK_HOME') is not None, 'SPARK_HOME environment variable is not set.'
        assert os.getenv('HADOOP_HOME') is not None, 'HADOOP_HOME environment variable is not set.'
        assert os.getenv('JAVA_HOME') is not None, 'JAVA_HOME environment variable is not set.'
        assert os.getenv('PYTHONPATH') is not None, ('PYTHONPATH environment variable is not set. '
                                                     'Include \'$SPARK_HOME/python\' in PYTHONPATH.')
        assert os.getenv('SPARK_HOME') in os.getenv('PYTHONPATH'), ('SPARK_HOME is not in PYTHONPATH. '
                                                                    'Include \'$SPARK_HOME/python\' in PYTHONPATH.')

        self.rapids_args = self.submission_cmd.extra_rapids_args[:-1]
        self.event_logs_path = self.submission_cmd.extra_rapids_args[-1]
        config_path = Utils.resource_path(f'{self.name}-conf.yaml')
        self.props = YAMLPropertiesContainer(prop_arg=config_path)
        self.hdfs_manager = HdfsManager()
        self.local_fs_manager = LocalFsManager()
        cache_dir = self.props.get_value('cacheDir')
        if not FSUtil.resource_exists(cache_dir):
            FSUtil.make_dirs(cache_dir)

    def run_as_spark_app(self):
        jar_output_path = self._get_jar_output_path()
        self.local_fs_manager.get_fs().create_dir(jar_output_path, recursive=True)
        try:
            self._run_as_spark_app_internal()
        except Exception as e:  # pylint: disable=broad-except
            exception_msg = f'Failed to run the tool as a Spark application: {str(e)}'
            failed_app = AppStatusResult(path=self.event_logs_path, status=AppStatus.FAILURE, message=exception_msg)
            self._write_to_csv(failed_app, jar_output_path, self.local_fs_manager.get_fs())

    def _run_as_spark_app_internal(self):
        executor_output_path = self._get_hdfs_executor_output_path()
        self.hdfs_manager.get_fs().create_dir(executor_output_path, recursive=True)

        # TODO: Add support for other file systems as input paths (e.g., S3, GCS)
        self.input_fs_manager = InputFsManager(input_fs_manager=self._get_fs())
        eventlog_files = self.input_fs_manager.get_files_from_path(self.event_logs_path)

        self.spark_manager = SparkJobManager(self.spark_config_file,
                                             self.submission_cmd.dependencies_paths,
                                             self.submission_cmd.jvm_log_file,
                                             self._get_log_file_path(),
                                             self._get_local_cache_dir())
        # Define the dictionary with arguments
        config_instance = SparkJobConfig(
            output_dir=executor_output_path,
            dependencies_paths=self.submission_cmd.dependencies_paths,
            hadoop_classpath=self.submission_cmd.hadoop_classpath,
            jvm_log_file=self.submission_cmd.jvm_log_file,
            jvm_args=self.submission_cmd.jvm_args,
            jar_main_class=self.submission_cmd.jar_main_class,
            rapids_args=self.rapids_args
        )

        # Pass the dictionary to create_run_jar_map_func
        jar_runner = SparkJobRunner(platform=self.platform, config=config_instance)
        run_jar_command = jar_runner.create_run_jar_map_func()
        app_statuses = self.spark_manager.submit_map_job(map_func=run_jar_command, input_list=eventlog_files)
        self._write_failed_app_statuses_to_hdfs(app_statuses, executor_output_path)
        # result_combiner = ResultCombiner(jar_output_folder=self._get_jar_output_path(),
        #                                  executor_output_dir=executor_output_path,
        #                                  hdfs_fs=self.hdfs_manager.get_fs())
        # result_combiner.combine_results()

        self._cleanup()

    # Define getters for the output paths
    def _get_local_cache_dir(self) -> str:
        return self.props.get_value('cacheDir')

    def _get_hdfs_executor_output_path(self) -> str:
        output_folder_name = os.path.basename(self.submission_cmd.output_folder)
        cache_dir = self._get_local_cache_dir()
        executor_output_dir_name = self.props.get_value('executorOutputDirName')
        executor_output_path_raw = os.path.join(cache_dir, output_folder_name, executor_output_dir_name)
        return f'{self.hdfs_manager.get_scheme()}:///{executor_output_path_raw.strip("/")}'

    def _get_jar_output_path(self) -> str:
        jar_output_dir_name = self.props.get_value('jarOutputDirName')
        return os.path.join(self.submission_cmd.output_folder, jar_output_dir_name)

    def _get_log_file_path(self) -> str:
        log_file_name = self.props.get_value('logFileName')
        return os.path.join(self.submission_cmd.output_folder, log_file_name)

    def _get_fs(self) -> FsManager:
        scheme = urlparse(self.event_logs_path).scheme
        if scheme in {'file', ''}:
            return self.local_fs_manager
        if scheme == 'hdfs':
            return self.hdfs_manager
        raise ValueError(f'Unsupported scheme: {scheme}')

    def _write_failed_app_statuses_to_hdfs(self, app_statuses: List[AppStatusResult], executor_output_path: str):
        jar_output_dir_name = self.props.get_value('jarOutputDirName')
        for app_status in app_statuses:
            if app_status.status == AppStatus.FAILURE:
                file_name = os.path.basename(app_status.path)
                jar_output_dir = os.path.join(executor_output_path, file_name, jar_output_dir_name)
                self._write_to_csv(app_status, jar_output_dir, self.hdfs_manager.get_fs())

    def _write_to_csv(self, app_status: AppStatusResult, jar_output_path: str, output_fs: fs.FileSystem) -> None:
        status_csv_file_name = self.props.get_value('statusCsvFileName')
        status_csv_file_path = os.path.join(jar_output_path, status_csv_file_name)
        with output_fs.open_output_stream(status_csv_file_path) as f:
            pd.DataFrame([app_status.to_dict()]).to_csv(f, index=False)

    def _cleanup(self):
        pass
