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
from utils import Utilities
from dataclasses import dataclass, field

@dataclass
class ResultCombiner:
    local_output_dir_str: str = field(default='', init=True)
    jar_output_dir_name: str = field(default='rapids_4_spark_qualification_output', init=False)
    combined_dataframes: dict = field(default_factory=dict, init=False)
    combined_json_data: dict = field(default_factory=dict, init=False)
    local_output_dir: Path = field(init=False)
    combined_output_path: Path = field(init=False)

    def __post_init__(self):
        self.local_output_dir = Path(self.local_output_dir_str)
        self.combined_output_path = Path(self.jar_output_dir_name)
        self.combined_output_path.mkdir(parents=True, exist_ok=True)

    def _process_raw_metrics_dir(self, inner_directory: Path):
        raw_metrics_path = inner_directory / 'raw_metrics'
        copy_command = ["cp", "-r", str(raw_metrics_path), str(self.combined_output_path)]
        Utilities.run_cmd(copy_command, description=None)

    def _process_csv_files(self, inner_directory: Path):
        for file_path in inner_directory.glob("*.csv"):
            csv_data = pd.read_csv(file_path)
            if file_path.name in self.combined_dataframes:
                self.combined_dataframes[file_path.name] = pd.concat(
                    [self.combined_dataframes[file_path.name], csv_data], ignore_index=True
                )
            else:
                self.combined_dataframes[file_path.name] = csv_data

    def _write_combined_csv(self):
        for filename, dataframe in self.combined_dataframes.items():
            output_path = self.combined_output_path / filename
            dataframe.to_csv(output_path, index=False)

    def _process_json_files(self, inner_directory: Path):
        for file_path in inner_directory.glob("*.json"):
            with file_path.open('r') as file:
                data = json.load(file)
                if isinstance(data, list) and all(isinstance(item, dict) for item in data):
                    if file_path.name in self.combined_json_data:
                        self.combined_json_data[file_path.name].extend(data)
                    else:
                        self.combined_json_data[file_path.name] = data
                else:
                    raise ValueError(f"Unexpected data format in {file_path}, expected a list of dictionaries.")

    def _write_combined_json(self):
        for filename, data in self.combined_json_data.items():
            output_path = self.combined_output_path / filename
            with output_path.open('w') as file:
                json.dump(data, file, indent=2)

    def _process_log_files(self, inner_directory: Path):
        for file_path in inner_directory.glob("*.log"):
            output_file = self.combined_output_path / file_path.name
            with output_file.open('ab') as out_file, file_path.open('rb') as in_file:
                shutil.copyfileobj(in_file, out_file)

    def _process_runtime_properties_file(self, inner_directory: Path):
        runtime_prop_file_name = 'runtime.properties'
        runtime_prop_file_path = inner_directory / runtime_prop_file_name
        copy_command = ["cp", str(runtime_prop_file_path), str(self.combined_output_path)]
        Utilities.run_cmd(copy_command, description=None)

    def combine_results(self):
        print(f"Combining results from {self.local_output_dir} to {self.combined_output_path}")
        for directory in self.local_output_dir.iterdir():
            inner_directory = directory / self.jar_output_dir_name
            if not inner_directory.is_dir():
                continue
            if len(self.combined_dataframes) == 0:
                self._process_runtime_properties_file(inner_directory)
            self._process_csv_files(inner_directory)
            self._process_json_files(inner_directory)
            self._process_raw_metrics_dir(inner_directory)
            self._process_log_files(inner_directory)

        self._write_combined_csv()
        self._write_combined_json()
