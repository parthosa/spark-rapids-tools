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

""" SparkJobManager class to manage Spark jobs """

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Callable, List, Tuple

from pyspark import SparkContext, RDD
from pyspark.sql import SparkSession

from distributed.status_reporter import AppStatusResult
from spark_rapids_tools.tools.distributed.utils import Utilities


@dataclass
class SparkSessionManager:
    spark_config_file: str = field(init=True)
    cache_dir: str = field(init=True)
    dependencies_paths: List[str] = field(default=None, init=True)
    jvm_log_file: str = field(default=None, init=True)
    _spark_context: SparkContext = field(default=None, init=False)

    @classmethod
    def _set_spark_context(cls, spark_context: SparkContext) -> None:
        cls._spark_context = spark_context

    @classmethod
    def _get_spark_context(cls) -> SparkContext:
        return cls._spark_context

    @staticmethod
    def _set_env():
        spark_home = os.environ.get('SPARK_HOME')
        if spark_home:
            python_path = os.path.join(spark_home, 'python')
            os.environ['PYTHONPATH'] = f'{python_path}:{os.environ.get("PYTHONPATH", "")}'

    def _get_python_dependencies(self) -> List[str]:
        folder_path = os.path.join(Utilities.get_project_root(), 'distributed')
        dest_zip_path = os.path.join(self.cache_dir, 'distributed.zip')
        zip_path = Utilities.zip_folder(folder_path, dest_zip_path)
        return [zip_path]

    def _add_files_to_spark_context(self):
        if self.dependencies_paths:
            for dep_path in self.dependencies_paths:
                self._get_spark_context().addFile(dep_path)
        for dep_path in self._get_python_dependencies():
            self._get_spark_context().addPyFile(dep_path)
        if self.jvm_log_file:
            self._get_spark_context().addFile(self.jvm_log_file)

    def _parse_spark_conf(self) -> dict:
        spark_conf = {}
        try:
            # Read the config file and extract key-value pairs
            with open(self.spark_config_file, 'r', encoding='utf-8') as conf_file:
                for line in conf_file:
                    if line.strip() and not line.strip().startswith('#'):
                        key, value = line.strip().split(None, 1)
                        spark_conf[key] = value
        except Exception as e:  # pylint: disable=broad-except
            logging.error('Error reading Spark configuration file: %s', e)
        return spark_conf

    def _generate_spark_conf(self,
                             min_heap_memory_per_task=4,
                             task_cpus=1) -> dict:
        user_spark_confs = self._parse_spark_conf()
        # Create or retrieve the Spark session
        spark = SparkSession.builder.appName('Generate Spark Config').getOrCreate()

        # Get executor memory from user provided config or existing Spark conf
        if user_spark_confs and 'spark.executor.memory' in user_spark_confs:
            executor_memory_str = user_spark_confs['spark.executor.memory']
        else:
            executor_memory_str = spark.conf.get('spark.executor.memory', '32g')  # Default to 32 GB if not set

        executor_memory_gb = Utilities.parse_memory_size(executor_memory_str)

        # Calculate maximum number of tasks per executor
        max_tasks_per_executor = int(executor_memory_gb // min_heap_memory_per_task)
        executor_instances = 1  # One executor per node

        # Configure Spark parameters
        spark_conf = {
            'spark.executor.instances': executor_instances,
            'spark.executor.cores': max_tasks_per_executor,  # Each task gets a core
            'spark.executor.memory': f'{int(executor_memory_gb)}g',
            'spark.task.cpus': task_cpus
        }

        # Merge user provided Spark configurations if any
        if user_spark_confs:
            spark_conf.update(user_spark_confs)

        spark.stop()
        return spark_conf

    def _create_spark_session_internal(self):
        spark_builder = SparkSession.builder.appName('Distributed Qualification Tool')
        spark_confs = self._generate_spark_conf()
        logging.info('Setting Spark configurations\n: %s', json.dumps(spark_confs, indent=4))
        for key, value in spark_confs.items():
            spark_builder.config(key, value)
        spark_builder.config('spark.submit.deployMode', 'client')
        spark_builder.config('spark.executorEnv.PYTHONPATH', os.environ['PYTHONPATH'])
        spark = spark_builder.getOrCreate()
        return spark

    def create_spark_session(self):
        spark = self._create_spark_session_internal()
        self._set_spark_context(spark.sparkContext)
        self._add_files_to_spark_context()
        self._set_env()
        return spark


@dataclass
class SparkJobManager:
    """ Class to manage Spark jobs """
    spark_config_file: str
    dependencies_paths: List[str]
    jvm_log_file: str
    log_file_path: str
    cache_dir: str
    _spark_context: SparkContext = field(default=None, init=False)

    @classmethod
    def _set_spark_context(cls, spark_context: SparkContext) -> None:
        cls._spark_context = spark_context

    @classmethod
    def _get_spark_context(cls) -> SparkContext:
        return cls._spark_context

    def __post_init__(self):
        logging.getLogger('py4j').setLevel(logging.ERROR)
        self._check_spark_submit_availability()
        self.initialize_spark_context()

    @staticmethod
    def _check_spark_submit_availability():
        check_command = ['spark-submit', '--version']
        Utilities.check_cmd_availability('spark-submit', check_command)
        # Add Spark Python path to PYTHONPATH
        os.environ['PYTHONPATH'] = os.pathsep.join(filter(None, [
            f"{os.environ['SPARK_HOME']}/python",
            os.environ.get('PYTHONPATH', '')
        ]))

    def initialize_spark_context(self):
        spark_session_manager = SparkSessionManager(
            spark_config_file=self.spark_config_file,
            cache_dir=self.cache_dir,
            dependencies_paths=self.dependencies_paths,
            jvm_log_file=self.jvm_log_file)
        os.environ['DIST_QUAL_CACHE_DIR'] = self.cache_dir
        if self.spark_config_file and len(self.spark_config_file) > 0:
            os.environ['DIST_SPARK_CONF_FILE'] = self.spark_config_file
        spark = spark_session_manager.create_spark_session()
        self._set_spark_context(spark.sparkContext)

    def _convert_input_to_rdd(self, input_list: list) -> RDD:
        num_partitions = len(input_list)
        return self._get_spark_context().parallelize(input_list, numSlices=num_partitions)

    @classmethod
    def _run_map_job(cls, map_func: Callable, input_list_rdd) -> Tuple[list, timedelta]:
        start_time = datetime.now()
        map_fn_result = input_list_rdd.map(map_func).collect()
        total_time = datetime.now() - start_time
        return map_fn_result, total_time

    @staticmethod
    def _write_output(log_file_path: str, logs_arr_list: List[List[str]], total_time: timedelta):
        logging.info('Saving logs to %s', log_file_path)
        logs_list = ['\n'.join(logs) for logs in logs_arr_list]
        output_str = '\n\n'.join(logs_list) + f'\nJob took {total_time} to complete'

        try:
            with open(log_file_path, 'w', encoding='utf-8') as f:
                f.write(output_str)
        except IOError as e:
            logging.error('Failed to write to log file %s. Error: %s', log_file_path, e)
            raise

    def submit_map_job(self, map_func: Callable, input_list: list) -> List[AppStatusResult]:
        input_list_rdd = self._convert_input_to_rdd(input_list)
        try:
            map_fn_result, total_time = self._run_map_job(map_func, input_list_rdd)
            logs_arr_list, app_statuses = zip(*map_fn_result)
            self._write_output(self.log_file_path, logs_arr_list, total_time)
            return list(app_statuses)
        except Exception as e:
            logging.error('Error during map job submission: %s', e)
            raise

    def cleanup(self):
        self._get_spark_context().stop()
