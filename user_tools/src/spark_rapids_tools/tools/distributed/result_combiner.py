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

import json
import pandas as pd
import shutil
from pathlib import Path
from abc import ABC, abstractmethod
from spark_rapids_tools.tools.distributed.utils import Utilities
from dataclasses import dataclass, field


# Base class for all file processors
class FileProcessor(ABC):
    def __init__(self, inner_directory: Path, combined_output_path: Path):
        self.inner_directory = inner_directory
        self.combined_output_path = combined_output_path

    @abstractmethod
    def process(self):
        """Abstract method for processing files."""
        pass


# CSV Processor
class CSVProcessor(FileProcessor):
    def __init__(self, inner_directory: Path, combined_output_path: Path, combined_dataframes: dict):
        super().__init__(inner_directory, combined_output_path)
        self.combined_dataframes = combined_dataframes

    def process(self):
        for file_path in self.inner_directory.glob("*.csv"):
            try:
                csv_data = pd.read_csv(file_path)
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
    def __init__(self, inner_directory: Path, combined_output_path: Path, combined_json_data: dict):
        super().__init__(inner_directory, combined_output_path)
        self.combined_json_data = combined_json_data

    def process(self):
        for file_path in self.inner_directory.glob("*.json"):
            try:
                with file_path.open('r') as file:
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
        for file_path in self.inner_directory.glob("*.log"):
            try:
                output_file = self.combined_output_path / file_path.name
                with output_file.open('ab') as out_file, file_path.open('rb') as in_file:
                    shutil.copyfileobj(in_file, out_file)
            except Exception as e:
                raise RuntimeError(f"Error processing log file {file_path}: {e}")


# Raw Metrics Processor
class RawMetricsProcessor(FileProcessor):
    def process(self):
        raw_metrics_path = self.inner_directory / 'raw_metrics'
        copy_command = ["cp", "-r", str(raw_metrics_path), str(self.combined_output_path)]
        Utilities.run_cmd(copy_command, description=None)


# Runtime Properties Processor
class RuntimePropertiesProcessor(FileProcessor):
    def process(self):
        runtime_prop_file_name = 'runtime.properties'
        runtime_prop_file_path = self.inner_directory / runtime_prop_file_name
        if runtime_prop_file_path.exists():
            copy_command = ["cp", str(runtime_prop_file_path), str(self.combined_output_path)]
            Utilities.run_cmd(copy_command, description=None)


# Main ResultCombiner class that uses different processors
@dataclass
class ResultCombiner:
    output_folder: str
    jar_output_dir_name: str = field(default='rapids_4_spark_qualification_output', init=False)
    combined_dataframes: dict = field(default_factory=dict, init=False)
    combined_json_data: dict = field(default_factory=dict, init=False)
    executor_output_dir: Path = field(init=False)
    combined_output_path: Path = field(init=False)

    def __post_init__(self):
        # Set up paths
        self.executor_output_dir = Path(self.output_folder) / Utilities.get_executor_output_dir_name()
        self.combined_output_path = Path(self.output_folder) / self.jar_output_dir_name
        self.combined_output_path.mkdir(parents=True, exist_ok=True)

    def combine_results(self):
        """Main method to combine all results."""
        print(f"Combining results from {self.executor_output_dir} to {self.combined_output_path}")
        for directory in self.executor_output_dir.iterdir():
            inner_directory = directory / self.jar_output_dir_name
            if not inner_directory.is_dir():
                continue

            # Process runtime properties once
            if not self.combined_dataframes:
                RuntimePropertiesProcessor(inner_directory, self.combined_output_path).process()

            # Use the specific processors for different file types
            CSVProcessor(inner_directory, self.combined_output_path, self.combined_dataframes).process()
            JSONProcessor(inner_directory, self.combined_output_path, self.combined_json_data).process()
            LogProcessor(inner_directory, self.combined_output_path).process()
            RawMetricsProcessor(inner_directory, self.combined_output_path).process()

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
