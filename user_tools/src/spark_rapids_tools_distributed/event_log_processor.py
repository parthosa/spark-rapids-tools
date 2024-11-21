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

""" This module contains the EventLogProcessor class that processes event logs. """
from queue import Queue
from typing import List, Optional, Set

from spark_rapids_tools import CspPath
from spark_rapids_tools.storagelib import CspFs


class EventLogProcessor:
    # Apache Spark event log prefixes
    EVENT_LOG_DIR_NAME_PREFIX = "eventlog_v2_"
    DB_EVENT_LOG_FILE_NAME_PREFIX = "eventlog"

    # Compression codec names
    SPARK_SHORT_COMPRESSION_CODEC_NAMES: Set[str] = {"lz4", "lzf", "snappy", "zstd"}

    # Apache Spark ones plus gzip
    SPARK_SHORT_COMPRESSION_CODEC_NAMES_FOR_FILTER: Set[str] = SPARK_SHORT_COMPRESSION_CODEC_NAMES.union({"gz"})

    # Files having these keywords are not considered as event logs
    EXCLUDED_EVENTLOG_NAME_KEYWORDS: Set[str] = {"stdout", "stderr", "log4j", ".log."}

    @classmethod
    def _is_event_log_dir(cls, input_path: CspPath) -> bool:
        """
        Check if the given status represents an event log directory.

        Args:
            input_path: File or directory status object

        Returns:
            True if it's an event log directory, False otherwise
        """
        return input_path.is_dir() and input_path.base_name().startswith(cls.EVENT_LOG_DIR_NAME_PREFIX)

    @classmethod
    def _is_db_event_log_file(cls, input_path: CspPath) -> bool:
        """
        Check if the given status represents a Databricks event log file.

        Args:
            status: File status object

        Returns:
            True if it's a Databricks event log file, False otherwise
        """
        return input_path.is_file() and input_path.base_name().startswith(cls.DB_EVENT_LOG_FILE_NAME_PREFIX)

    @classmethod
    def _event_log_name_filter(cls, log_file: CspPath) -> bool:
        """
        Filter to identify valid event log files based on specific criteria.

        Criteria:
        - File should either not have any suffix or have a supported compression codec suffix
        - File should not contain any of the EXCLUDED_EVENTLOG_NAME_KEYWORDS keywords

        Args:
            log_file: Path of the file to be filtered

        Returns:
            True if the file is a valid event log, False otherwise
        """
        # Extract codec name
        codec = log_file.extension

        # Check if codec is valid (either None or in supported codecs)
        has_valid_suffix = (
                len(codec) == 0 or
                codec in cls.SPARK_SHORT_COMPRESSION_CODEC_NAMES_FOR_FILTER
        )

        # Check if file name contains any excluded keywords
        has_excluded_keyword = any(
            keyword in log_file.base_name()
            for keyword in cls.EXCLUDED_EVENTLOG_NAME_KEYWORDS
        )

        return has_valid_suffix and not has_excluded_keyword

    @classmethod
    def _is_databricks_event_log_dir(cls, input_path: CspPath) -> bool:
        """
        Check if the directory is a Databricks event log directory.

        Databricks has the latest events in files named 'eventlog' and rolled files
        like 'eventlog-2021-06-14--20-00.gz'.
        We assume it's a Databricks event log directory if multiple files start with 'eventlog'.

        Args:
            input_path: Directory status object

        Returns:
            True if it's a Databricks event log directory, False otherwise
        """
        # Only process if it's a directory
        if not input_path.is_dir():
            return False

        # List all files in the directory
        db_log_files = [
            f for f in CspFs.list_all(input_path)
            if cls._is_db_event_log_file(f)
        ]

        # Consider it a Databricks event log directory if more than one matching file
        return len(db_log_files) > 1

    @classmethod
    def _identify_event_log(cls, input_path: CspPath) -> Optional[CspPath]:
        """
        Identifies if the input file or directory is a valid event log.

        Args:
            input_path: File or directory status to be identified.

        Returns:
            EventLogInfo if valid, None otherwise.
        """
        # Check if it's a file and passes name filter
        if input_path.is_file() and cls._event_log_name_filter(input_path):
            return input_path

        # Check if it's an Apache Spark event log directory
        if cls._is_event_log_dir(input_path):
            return input_path

        # Check if it's a Databricks event log directory
        if cls._is_databricks_event_log_dir(input_path):
            return input_path

        # Ignore other types of files
        return None

    @classmethod
    def get_event_log_info(
            cls,
            input_path: CspPath,
            recursive_search_enabled: bool = True
    ) -> List[CspPath]:
        """
        Retrieves all event logs from the input path.

        Args:
            input_path: Root path to search for event logs.
            recursive_search_enabled: If enabled, search for event logs in all subdirectories.

        Returns:
            List of (EventLogInfo, EventLogFileSystemInfo) tuples.
        """
        results = []
        queue: Queue[CspPath] = Queue()

        # Start with the initial input path
        queue.put(input_path)

        while not queue.empty():
            current_entry = queue.get()

            # Try to identify if the current entry is an event log
            event_log_info = cls._identify_event_log(current_entry)

            if event_log_info:
                # If it's an event log, add to results with filesystem info
                results.append(event_log_info)

            # If it's a directory, process its contents
            elif current_entry.is_dir():
                # List files/subdirectories in the current directory
                children = CspFs.list_all(current_entry)

                for child in children:  # type: CspPath
                    # Enqueue files or subdirectories based on recursive search setting
                    if child.is_file() or (child.is_dir() and recursive_search_enabled):
                        queue.put(child)

            else:
                # Log debug information for unsupported file types
                supported_types = ', '.join(cls.SPARK_SHORT_COMPRESSION_CODEC_NAMES_FOR_FILTER)
                print(f"Debug: File {current_entry} is not a supported file type. "
                      f"Supported compression types are: {supported_types}. Skipping this file.")

        return results
