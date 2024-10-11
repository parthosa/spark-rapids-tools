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

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from utils import Utilities
from typing import Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, field
import os

@dataclass
class SparkJobManager:
    spark_master: str = field(init=True)
    tools_jar_path: str = field(init=True)
    local_output_dir: str = field(init=True)
    defaut_log_file: str = field(default="distributed_qual_tool.log", init=False)
    sc: SparkContext = None

    def __post_init__(self):
        check_command = ["spark-submit", "--version"]
        Utilities.check_cmd_availability("spark-submit", check_command)
        spark = SparkSession.builder \
            .appName("Distributed Qualification Tool") \
            .master(self.spark_master) \
            .getOrCreate()
        self.sc = spark.sparkContext
        self.sc.addFile(self.tools_jar_path)
        self._set_env()

    def _set_env(self):
        spark_home = os.environ.get("SPARK_HOME")
        if spark_home:
            python_path = os.path.join(spark_home, "python")
            os.environ["PYTHONPATH"] = f"{python_path}:{os.environ.get('PYTHONPATH')}"

    def _convert_input_to_rdd(self, input_list: list):
        num_partitions = len(input_list)
        return self.sc.parallelize(input_list, numSlices=num_partitions)

    def _run_map_job(self, map_func: Callable, input_list_rdd):
        start_time = datetime.now()
        map_fn_result = input_list_rdd.map(map_func).collect()
        end_time = datetime.now()
        total_time = (end_time - start_time)
        return map_fn_result, total_time

    def _write_output(self, log_file_path, logs_arr_list, total_time: timedelta):
        print(f"Savings logs to {log_file_path}")
        logs_list = ["\n".join(logs) for logs in logs_arr_list]
        output_str = "\n\n".join(logs_list)
        output_str += f"\nJob took {total_time} to complete"
        with open(log_file_path, "w") as f:
            f.write(output_str)

    def submit_map_job(self, map_func: Callable, input_list: list, log_file_path: str = None) -> list:
        log_file_path = log_file_path or self.defaut_log_file
        input_list_rdd = self._convert_input_to_rdd(input_list)
        map_fn_result, total_time = self._run_map_job(map_func, input_list_rdd)
        logs_arr_list, result = list(zip(*map_fn_result))
        self._write_output(log_file_path, logs_arr_list, total_time)
        return result

    def cleanup(self):
        self.sc.stop()
