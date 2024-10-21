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

import fnmatch
import json
import pandas as pd
from pathlib import Path
from abc import ABC, abstractmethod
from urllib.parse import urlparse

from pyarrow import fs
from pyarrow.fs import FileInfo

from dataclasses import dataclass, field


# Base class for all file processors
class FileProcessor(ABC):
    def __init__(self, inner_directory: FileInfo, hdfs_fs: fs.HadoopFileSystem, combined_output_path: Path):
        self.inner_directory = inner_directory
        self.hdfs_fs = hdfs_fs
        self.combined_output_path = combined_output_path

    def get_matching_files(self, pattern: str):
        file_info = self.hdfs_fs.get_file_info(fs.FileSelector(self.inner_directory.path, recursive=False))
        return [info.path for info in file_info if info.is_file and fnmatch.fnmatch(info.path, pattern)]

    @abstractmethod
    def process(self):
        """Abstract method for processing files."""
        pass


# CSV Processor
class CSVProcessor(FileProcessor):
    def __init__(self, inner_directory: FileInfo, hdfs_fs: fs.HadoopFileSystem,
                 combined_output_path: Path, combined_dataframes: dict):
        super().__init__(inner_directory, hdfs_fs, combined_output_path)
        self.combined_dataframes = combined_dataframes

    def process(self):
        #  list all the csv files in the inner directory
        csv_files = self.get_matching_files(pattern="*.csv")
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
                except Exception as e:
                    raise RuntimeError(f"Error processing CSV {file_path}: {e}")


# JSON Processor
class JSONProcessor(FileProcessor):
    def __init__(self, inner_directory: FileInfo, hdfs_fs: fs.HadoopFileSystem,
                 combined_output_path: Path, combined_json_data: dict):
        super().__init__(inner_directory, hdfs_fs, combined_output_path)
        self.combined_json_data = combined_json_data

    def process(self):
        json_files = self.get_matching_files(pattern="*.json")
        for file_info in json_files:
            file_path = Path(file_info)
            with self.hdfs_fs.open_input_file(file_info) as file:
                try:
                    data = json.load(file)
                    if not (isinstance(data, list) and all(isinstance(item, dict) for item in data)):
                        raise ValueError(f"Unexpected format in {file_path}: expected list of dictionaries.")

                    if file_path.name in self.combined_json_data:
                        self.combined_json_data[file_path.name].extend(data)
                    else:
                        self.combined_json_data[file_path.name] = data
                except Exception as e:
                    raise RuntimeError(f"Error processing JSON {file_path}: {e}")


# Log Processor
class LogProcessor(FileProcessor):
    def process(self):
        log_files = self.get_matching_files(pattern="*.log")
        for file_info in log_files:
            file_path = Path(file_info)
            with self.hdfs_fs.open_input_file(file_info) as file:
                try:
                    content = file.read().decode('utf-8')
                    output_file = self.combined_output_path / file_path.name
                    with output_file.open('a') as out_file:
                        out_file.write(content)
                except Exception as e:
                    raise RuntimeError(f"Error processing log file {file_path}: {e}")


# Raw Metrics Processor
class RawMetricsProcessor(FileProcessor):
    def process(self):
        raw_metrics_path = Path(self.inner_directory.path) / "raw_metrics"
        fs.copy_files(raw_metrics_path.as_posix(), self.combined_output_path.as_posix(), source_filesystem=self.hdfs_fs)


# Runtime Properties Processor
class RuntimePropertiesProcessor(FileProcessor):
    def process(self):
        runtime_prop_file_path = Path(self.inner_directory.path) / 'runtime.properties'
        self.hdfs_fs.copy_file(runtime_prop_file_path.as_posix(), self.combined_output_path.as_posix())

# Main ResultCombiner class that uses different processors
@dataclass
class ResultCombiner:
    output_folder: str = field(init=True)
    executor_output_dir: str = field(init=True)
    jar_output_dir_name: str = field(default='rapids_4_spark_qualification_output', init=False)
    combined_dataframes: dict = field(default_factory=dict, init=False)
    combined_json_data: dict = field(default_factory=dict, init=False)
    combined_output_path: Path = field(init=False)
    hdfs: fs.HadoopFileSystem = field(init=False)

    def __post_init__(self):
        # Set up paths
        self.hdfs = fs.HadoopFileSystem("default")
        self.combined_output_path = Path(self.output_folder) / self.jar_output_dir_name
        self.combined_output_path.mkdir(parents=True, exist_ok=True)

    def combine_results(self):
        """Main method to combine all results."""
        print(f"Combining results from {self.executor_output_dir} to {self.combined_output_path}")
        executor_output_dir_no_scheme = urlparse(self.executor_output_dir).path
        # list of directories in the executor output directory (it is an hdfs path)
        directories = self.hdfs.get_file_info(fs.FileSelector(executor_output_dir_no_scheme, recursive=False))
        for directory in directories:
            inner_dir_info = self.hdfs.get_file_info(fs.FileSelector(directory.path, recursive=False))
            if not inner_dir_info:
                continue

            inner_dir_info = inner_dir_info[0]

            # # Process runtime properties once
            if not self.combined_dataframes:
                RuntimePropertiesProcessor(inner_dir_info,  self.hdfs, self.combined_output_path).process()

            # Use the specific processors for different file types
            CSVProcessor(inner_dir_info, self.hdfs, self.combined_output_path, self.combined_dataframes).process()
            JSONProcessor(inner_dir_info, self.hdfs, self.combined_output_path, self.combined_json_data).process()
            LogProcessor(inner_dir_info, self.hdfs, self.combined_output_path).process()
            RawMetricsProcessor(inner_dir_info, self.hdfs, self.combined_output_path).process()

        # Write the combined CSV and JSON data
        self._write_combined_csv()
        self._write_combined_json()

    def _write_combined_csv(self):
        """Write the combined CSV data to the output folder."""
        for filename, dataframe in self.combined_dataframes.items():
            output_path = self.combined_output_path / filename
            dataframe.to_csv(output_path, index=False)

    def _write_combined_json(self):
        """Write the combined JSON data to the output folder."""
        for filename, data in self.combined_json_data.items():
            output_path = self.combined_output_path / filename
            with output_path.open('w') as file:
                json.dump(data, file, indent=2)
