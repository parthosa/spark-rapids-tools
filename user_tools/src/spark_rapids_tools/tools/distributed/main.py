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
import sys
import subprocess
import traceback
from datetime import datetime
from pyspark import SparkFiles

from hdfs_manager import HdfsManager, InputFsManager
from spark_job_manager import SparkJobManager
from result_combiner import ResultCombiner


TMP_OUTPUT_DIR = "tools_output_tmp"
SPARK_HOME = os.environ.get("SPARK_HOME")
HADOOP_HOME = os.environ.get("HADOOP_HOME")

def copy_output_dir_to_hdfs(output_dir) -> list:
    logs = []
    start = datetime.now()
    if not os.path.exists(output_dir):
        logs.append(f"Output directory {output_dir} does not exist")
        return logs
    try:
        hdfs_output_dir = f'/{TMP_OUTPUT_DIR}/{os.path.basename(output_dir)}'
        logs.append(f"Copying {output_dir} to HDFS {hdfs_output_dir}")
        copy_command = [f"{HADOOP_HOME}/bin/hadoop", "fs", "-copyFromLocal", output_dir, hdfs_output_dir]
        subprocess.run(copy_command, check=True, text=True)
    except Exception:
        error_msg = f"Error copying {output_dir} to HDFS: {traceback.format_exc()}"
        logs.append(error_msg)
    end = datetime.now()
    logs.append(f"Copying Time: {end - start}")
    return logs

def submit_jar_cmd(jar_command) -> list:
    logs = []
    start = datetime.now()
    logs.append(f"Running command: {' '.join(jar_command)}")
    try:
        result = subprocess.run(jar_command, check=True, capture_output=True, text=True)
        logs.append(f"Command output:\n {result.stdout}")
    except Exception:
        error_msg = f"Error processing CMD. Reason: \n{traceback.format_exc()}"
        logs.append(error_msg)
    end = datetime.now()
    logs.append(f"Processing Time: {end - start}")
    return logs

def get_jar_command(tools_jar_name, platform, file_path, executor_output_dir):
    local_jar_path = SparkFiles.get(tools_jar_name)
    jars = f"{local_jar_path}:{SPARK_HOME}/jars/*"
    tool_args = ["--output-directory", executor_output_dir, "--platform", platform, file_path]
    java_exec = f"{os.environ['JAVA_HOME']}/bin/java"
    return [java_exec, "-Xmx24g", "-cp", jars, "com.nvidia.spark.rapids.tool.qualification.QualificationMain"] + tool_args

def create_run_jar_map_func(tools_jar_name, platform):
    def run_jar_map_func(file_path):
        logs = [f"Processing {file_path}"]
        # Output directory for the executor
        executor_output_dir = f"/tmp/{os.path.basename(file_path)}"

        # Run the JAR command
        jar_command = get_jar_command(tools_jar_name, platform, file_path, executor_output_dir)
        jar_cmd_logs = submit_jar_cmd(jar_command)
        logs.extend(jar_cmd_logs)

        # Copy the output to HDFS
        copy_logs = copy_output_dir_to_hdfs(executor_output_dir)
        logs.extend(copy_logs)
        return logs, executor_output_dir
    return run_jar_map_func


def init_setup():
    java_home = os.environ.get("JAVA_HOME")
    if not java_home:
        raise ValueError("JAVA_HOME is not set")


def run_tool_as_spark_app(spark_master, tools_jar_path, platform, eventlogs_path):
    tools_jar_name = os.path.basename(tools_jar_path)

    init_setup()

    # Initialize HDFS manager
    hdfs_manager = HdfsManager(TMP_OUTPUT_DIR)
    hdfs_manager.init_setup()

    # Get the list of eventlog file paths
    input_fs_manager = InputFsManager()
    eventlog_files = input_fs_manager.get_files_from_path(eventlogs_path)

    # Run the map job on Spark
    spark_manager = SparkJobManager(spark_master, tools_jar_path, TMP_OUTPUT_DIR)
    run_jar_command = create_run_jar_map_func(tools_jar_name, platform)
    spark_manager.submit_map_job(map_func=run_jar_command, input_list=eventlog_files)

    # Copy the output from HDFS to local
    hdfs_manager.copy_output_hdfs_to_local(local_dir='.')

    # Combine the results
    result_combiner = ResultCombiner(TMP_OUTPUT_DIR)
    result_combiner.combine_results()

    # Cleanup resources
    spark_manager.cleanup()

if __name__ == "__main__":
    if len(sys.argv) != 5:
        usage = f"Usage: {sys.argv[0]} <spark_master> <tools_jar_path> <platform> <eventlogs_path>"
        print(usage)
        sys.exit(1)

    spark_master = sys.argv[1]
    tools_jar_path = sys.argv[2]
    platform = sys.argv[3]
    eventlogs_path = sys.argv[4]

    if not os.path.exists(tools_jar_path):
        print(f"Error: Tools JAR does not exist: {tools_jar_path}")
        sys.exit(1)

    run_tool_as_spark_app(spark_master, tools_jar_path, platform, eventlogs_path)
