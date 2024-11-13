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

""" Module to combine results from multiple executors. """

import fnmatch
import json
import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
from pyarrow import fs
from pyarrow.fs import FileInfo

from spark_rapids_tools.tools.distributed.utils import Utilities


# Base class for all file processors
class FileProcessor(ABC):
    """ Base class for all file processors. """

    def __init__(self, inner_directory: FileInfo, hdfs_fs: fs.FileSystem, jar_output_folder: str):
        self.inner_directory = inner_directory
        self.hdfs_fs = hdfs_fs
        self.jar_output_folder = jar_output_folder

    def get_matching_files(self, pattern: str):
        try:
            file_info = self.hdfs_fs.get_file_info(fs.FileSelector(self.inner_directory.path))
        except Exception as e:  # pylint: disable=broad-except
            logging.error('Error getting file info for %s: %s', self.inner_directory.path, e)
            file_info = []
        return [info.path for info in file_info if info.is_file and fnmatch.fnmatch(info.path, pattern)]

    @abstractmethod
    def process(self):
        """Abstract method for processing files."""


# CSV Processor
class CSVProcessor(FileProcessor):
    """ Class to process CSV files. """

    def __init__(self, inner_directory: FileInfo, hdfs_fs: fs.FileSystem,
                 jar_output_folder: str, combined_dataframes: dict):
        super().__init__(inner_directory, hdfs_fs, jar_output_folder)
        self.combined_dataframes = combined_dataframes

    def process(self):
        #  list all the csv files in the inner directory
        csv_files = self.get_matching_files(pattern='*.csv')
        for file_info in csv_files:
            file_path = Path(file_info)
            with self.hdfs_fs.open_input_file(file_info) as file:
                try:
                    csv_data = pd.read_csv(file)
                    if file_path.name in self.combined_dataframes:
                        self.combined_dataframes[file_path.name] = pd.concat(
                            [self.combined_dataframes[file_path.name], csv_data], ignore_index=True
                        )
                    else:
                        self.combined_dataframes[file_path.name] = csv_data
                except Exception as e:  # pylint: disable=broad-except
                    raise RuntimeError(f'Error processing CSV {file_path}: {e}') from e


# JSON Processor
class JSONProcessor(FileProcessor):
    """ Class to process JSON files. """

    def __init__(self, inner_directory: FileInfo, hdfs_fs: fs.FileSystem,
                 jar_output_folder: str, combined_json_data: dict):
        super().__init__(inner_directory, hdfs_fs, jar_output_folder)
        self.combined_json_data = combined_json_data

    def process(self):
        json_files = self.get_matching_files(pattern='*.json')
        for file_info in json_files:
            file_path = Path(file_info)
            with self.hdfs_fs.open_input_file(file_info) as file:
                try:
                    data = json.load(file)
                    if not (isinstance(data, list) and all(isinstance(item, dict) for item in data)):
                        raise ValueError(f'Unexpected format in {file_path}: expected list of dictionaries.')

                    if file_path.name in self.combined_json_data:
                        self.combined_json_data[file_path.name].extend(data)
                    else:
                        self.combined_json_data[file_path.name] = data
                except Exception as e:  # pylint: disable=broad-except
                    raise RuntimeError(f'Error processing JSON {file_path}: {e}') from e


# Log Processor
class LogProcessor(FileProcessor):
    """ Class to process log files. """

    def process(self):
        log_files = self.get_matching_files(pattern='*.log')
        for file_info in log_files:
            file_path = Path(file_info)
            with self.hdfs_fs.open_input_file(file_info) as file:
                try:
                    content = file.read().decode('utf-8')
                    output_file = os.path.join(self.jar_output_folder, file_path.name)
                    with open(output_file, 'a', encoding='utf-8') as out_file:
                        out_file.write(content)
                except Exception as e:  # pylint: disable=broad-except
                    raise RuntimeError(f'Error processing log file {file_path}: {e}') from e


# Raw Metrics Processor
class RawMetricsProcessor(FileProcessor):
    """ Class to process raw metrics folder. """

    def process(self):
        raw_metrics_src_path = os.path.join(self.inner_directory.path, 'raw_metrics')
        raw_metrics_dest_path = os.path.join(self.jar_output_folder, 'raw_metrics')
        # Copy the raw metrics directory to the combined output path using pyarrow.fs.copy_files()
        # check if the raw_metrics directory exists using pyarrow
        if Utilities.resource_exists(raw_metrics_src_path, self.hdfs_fs):
            fs.copy_files(source=raw_metrics_src_path,
                          destination=raw_metrics_dest_path,
                          source_filesystem=self.hdfs_fs)


# Raw Metrics Processor
class TuningProcessor(FileProcessor):
    """ Class to process tuning folder. """

    def process(self):
        tuning_src_path = os.path.join(self.inner_directory.path, 'tuning')
        tuning_dest_path = os.path.join(self.jar_output_folder, 'tuning')
        if not os.path.exists(tuning_dest_path):
            os.mkdir(tuning_dest_path)
        # Copy the tuning directory to the combined output path using pyarrow.fs.copy_files()
        # check if the tuning directory exists using pyarrow
        if Utilities.resource_exists(tuning_src_path, self.hdfs_fs):
            fs.copy_files(source=tuning_src_path,
                          destination=tuning_dest_path,
                          source_filesystem=self.hdfs_fs)


# Runtime Properties Processor
class RuntimePropertiesProcessor(FileProcessor):
    """ Class to process runtime properties file. """

    def process(self):
        runtime_prop_file_path = os.path.join(self.inner_directory.path, 'runtime.properties')
        if Utilities.resource_exists(runtime_prop_file_path, self.hdfs_fs):
            self.hdfs_fs.copy_file(runtime_prop_file_path, self.jar_output_folder)


@dataclass
class ResultCombiner:
    """ Class to combine results from multiple executors. """

    jar_output_folder: str = field(init=True)
    executor_output_dir: str = field(init=True)
    hdfs_fs: fs.FileSystem = field(init=True)
    combined_dataframes: dict = field(default_factory=dict, init=False)
    combined_json_data: dict = field(default_factory=dict, init=False)

    def combine_results(self):
        """Main method to combine all results."""
        print(f'Combining results from {self.executor_output_dir} to {self.jar_output_folder}')
        executor_output_dir_no_scheme = urlparse(self.executor_output_dir).path
        # list of directories in the executor output directory (it is a hdfs path)
        directories = self.hdfs_fs.get_file_info(fs.FileSelector(executor_output_dir_no_scheme))
        for directory in directories:
            inner_dir_info = self.hdfs_fs.get_file_info(fs.FileSelector(directory.path))
            if not inner_dir_info:
                continue

            inner_dir_info = inner_dir_info[0]

            # # Process runtime properties once
            if not self.combined_dataframes:
                RuntimePropertiesProcessor(inner_dir_info, self.hdfs_fs, self.jar_output_folder).process()

            # Use the specific processors for different file types
            CSVProcessor(inner_dir_info, self.hdfs_fs, self.jar_output_folder, self.combined_dataframes).process()
            JSONProcessor(inner_dir_info, self.hdfs_fs, self.jar_output_folder, self.combined_json_data).process()
            LogProcessor(inner_dir_info, self.hdfs_fs, self.jar_output_folder).process()
            RawMetricsProcessor(inner_dir_info, self.hdfs_fs, self.jar_output_folder).process()
            TuningProcessor(inner_dir_info, self.hdfs_fs, self.jar_output_folder).process()

        # Write the combined CSV and JSON data
        self._write_combined_csv()
        self._write_combined_json()

    def _write_combined_csv(self):
        """Write the combined CSV data to the output folder."""
        for filename, dataframe in self.combined_dataframes.items():
            output_path = os.path.join(self.jar_output_folder, filename)
            dataframe.to_csv(output_path, index=False)

    def _write_combined_json(self):
        """Write the combined JSON data to the output folder."""
        for filename, data in self.combined_json_data.items():
            output_path = os.path.join(self.jar_output_folder, filename)
            with open(output_path, 'w', encoding='utf-8') as file:
                json.dump(data, file, indent=2)
