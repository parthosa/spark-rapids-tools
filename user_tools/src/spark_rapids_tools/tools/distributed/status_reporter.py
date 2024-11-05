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

""" Module for reporting the status of applications. """

import os

import pandas as pd
from pyarrow import fs

from spark_rapids_tools import EnumeratedType
from spark_rapids_tools.tools.distributed.utils import Utilities


class AppStatus(EnumeratedType):
    """ Enumerated type for the status of an application. """

    SUCCESS = 'SUCCESS'
    FAILURE = 'FAILURE'
    SKIPPED = 'SKIPPED'
    UNKNOWN = 'UNKNOWN'


class AppStatusResult:
    """ Class for storing the status of an application. """

    EVENT_LOG = 'Event Log'
    STATUS = 'Status'
    APP_ID = 'AppID'
    DESCRIPTION = 'Description'
    STATUS_CSV_FILE_NAME = 'rapids_4_spark_qualification_output_status.csv'

    def __init__(self, path: str, status: AppStatus, app_id: str = 'N/A', message: str = '') -> None:
        self.path: str = path
        self.status: AppStatus = status
        self.app_id: str = app_id
        self.message: str = message

    def to_dict(self) -> dict:
        """
        Convert the instance to a dictionary.
        """
        return {
            self.EVENT_LOG: self.path,
            self.STATUS: self.status.value,  # Use the value of the enum
            self.APP_ID: self.app_id,
            self.DESCRIPTION: self.message
        }

    def write_to_csv(self, tools_output_dir: str, output_fs: fs.FileSystem) -> None:
        """
        Write a list of application status results to a CSV file using Pandas.

        :param tools_output_dir: The directory to write the CSV file to.
        :param output_fs: The filesystem to write the CSV file to.
        """
        jar_output_dir = Utilities.get_jar_output_path(tools_output_dir)
        output_fs.create_dir(jar_output_dir)
        status_csv_file_path = os.path.join(jar_output_dir, self.STATUS_CSV_FILE_NAME)
        # Write the status to a CSV file
        with output_fs.open_output_stream(status_csv_file_path) as f:
            pd.DataFrame([self.to_dict()]).to_csv(f, index=False)
