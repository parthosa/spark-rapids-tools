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
from dataclasses import dataclass
from enum import Enum


class AppStatus(Enum):
    """ Enumerated type for the status of an application. """

    SUCCESS = 'SUCCESS'
    FAILURE = 'FAILURE'
    SKIPPED = 'SKIPPED'
    UNKNOWN = 'UNKNOWN'


@dataclass
class AppStatusResult:
    """ Class for storing the status of an application. """

    EVENT_LOG = 'Event Log'
    STATUS = 'Status'
    APP_ID = 'AppID'
    DESCRIPTION = 'Description'

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
