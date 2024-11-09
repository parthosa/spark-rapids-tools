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

"""Implementation class for Additional Heuristics logic."""

import os
import re
from dataclasses import dataclass, field
from logging import Logger
from typing import Optional

import pandas as pd
from pyarrow import fs

from spark_rapids_pytools.common.prop_manager import JSONPropertiesContainer
from spark_rapids_pytools.common.utilities import ToolLogging
from spark_rapids_tools.tools.qualx.util import find_paths, RegexPattern, find_paths_hdfs
from spark_rapids_tools.utils import Utilities


@dataclass
class AdditionalHeuristics:
    """
    Encapsulates the logic to apply additional heuristics to skip applications.
    """
    logger: Logger = field(default=None, init=False)
    props: JSONPropertiesContainer = field(default=None, init=False)
    tools_output_dir: str = field(default=None, init=False)
    output_file: str = field(default=None, init=False)
    hdfs_fs: Optional[fs.FileSystem] = field(default=None, init=False)
    # Contains apps info needed for applying heuristics
    all_apps: pd.DataFrame = field(default=None, init=False)

    def __init__(self, props: dict, tools_output_dir: str, output_file: str, hdfs_fs: Optional[fs.FileSystem] = None):
        self.props = JSONPropertiesContainer(props, file_load=False)
        self.tools_output_dir = tools_output_dir
        self.output_file = output_file
        self.hdfs_fs = hdfs_fs
        self.logger = ToolLogging.get_and_setup_logger(f'rapids.tools.{self.__class__.__name__}')

    def _get_all_heuristics_functions(self) -> list:
        """
        Returns a list of heuristics functions to apply to each application.
        """
        return [self.heuristics_based_on_spills]

    def is_metrics_path_empty(self, metrics_path: str, app_ids: list) -> bool:
        if self.hdfs_fs is None:
            return not os.listdir(metrics_path) or len(app_ids) == 0
        # Check if the directory is empty using pyarrow
        metrics_list = self.hdfs_fs.get_file_info(fs.FileSelector(metrics_path))
        return not len(metrics_list) > 0 or len(app_ids) == 0

    def _apply_heuristics_local(self, app_ids: list, metrics_path: str):
        result_arr = []
        for app_id in app_ids:
            app_id_path = os.path.join(metrics_path, app_id)
            # Apply a list of heuristics and determine if the application should be skipped.
            should_skip_overall = False
            reasons = []
            for heuristic_func in self._get_all_heuristics_functions():
                try:
                    should_skip, reason = heuristic_func(app_id_path)
                except Exception as e:  # pylint: disable=broad-except
                    should_skip = False
                    reason = f' Cannot apply heuristics for qualification. Reason - {type(e).__name__}:{e}.'
                    # self.logger.error(reason)
                should_skip_overall = should_skip_overall or should_skip
                reasons.append(reason)
            result_arr.append([app_id, should_skip_overall, ' '.join(reasons)])
        return result_arr

    def _apply_heuristics_hdfs(self, qual_metrics: list):
        result_arr = []
        for metric_dir in qual_metrics:
            selector = fs.FileSelector(metric_dir, recursive=False)
            subdir_list = self.hdfs_fs.get_file_info(selector)
            app_id_path = subdir_list[0].path
            app_id = os.path.basename(app_id_path)
            # Apply a list of heuristics and determine if the application should be skipped.
            should_skip_overall = False
            reasons = []
            for heuristic_func in self._get_all_heuristics_functions():
                try:
                    should_skip, reason = heuristic_func(app_id_path)
                except Exception as e:  # pylint: disable=broad-except
                    should_skip = False
                    reason = f' Cannot apply heuristics for qualification. Reason - {type(e).__name__}:{e}.'
                    # self.logger.error(reason)
                should_skip_overall = should_skip_overall or should_skip
                reasons.append(reason)
            result_arr.append([app_id, should_skip_overall, ' '.join(reasons)])
        return result_arr

    def _apply_heuristics(self, app_ids: list) -> pd.DataFrame:
        """
        Apply additional heuristics to applications to determine if they can be accelerated on GPU.
        """
        qual_output_dir = os.path.dirname(self.tools_output_dir)
        sub_folder = 'rapids_4_spark_qualification_output'
        qual_output_dir_name = os.path.basename(qual_output_dir)
        hdfs_cache_dir = '/var/tmp/spark_rapids_user_tools_distributed_cache'
        hdfs_qual_output_dir = f'{hdfs_cache_dir}/{qual_output_dir_name}/executor_output'

        selector = fs.FileSelector(hdfs_qual_output_dir, recursive=False)
        subdir_list = self.hdfs_fs.get_file_info(selector)

        qual_metrics = []

        for subdir in subdir_list:
            subdir_qual_path = os.path.join(subdir.path, sub_folder)
            qual_metrics_raw = find_paths_hdfs(
                subdir_qual_path,
                self.hdfs_fs,
                RegexPattern.qual_tool_metrics.match,
                return_directories=True,
            )
            qual_metrics.extend(qual_metrics_raw)

        if len(qual_metrics) == 0:
            self.logger.warning('No metrics found in output directory: %s', self.tools_output_dir)
            return pd.DataFrame(columns=self.props.get_value('resultCols'))

        if len(qual_metrics) > 1 and not self.hdfs_fs:
            # We don't expect multiple metrics directories. Log a warning and use the first one.
            self.logger.warning('Unexpected multiple metrics directories found. Using the first one: %s',
                                qual_metrics[0])

        metrics_path = qual_metrics[0]
        result_arr = []
        if self.is_metrics_path_empty(metrics_path, app_ids):
            self.logger.warning('Skipping empty metrics folder: %s', qual_metrics[0])
        elif not self.hdfs_fs:
            result_arr = self._apply_heuristics_local(app_ids, metrics_path)
        else:
            result_arr = self._apply_heuristics_hdfs(qual_metrics)


        return pd.DataFrame(result_arr, columns=self.props.get_value('resultCols'))

    def heuristics_based_on_spills(self, app_id_path: str) -> (bool, str):
        """
        Apply heuristics based on spills to determine if the app can be accelerated on GPU.
        """
        # Load stage aggregation metrics (this contains spill information)
        stage_agg_metrics_file = self.props.get_value('spillBased', 'stageAggMetrics', 'fileName')
        stage_agg_metrics_path = os.path.join(app_id_path, stage_agg_metrics_file)
        stage_agg_metrics = Utilities.read_csv(stage_agg_metrics_path, self.hdfs_fs)
        stage_agg_metrics = stage_agg_metrics[self.props.get_value('spillBased',
                                                                   'stageAggMetrics', 'columns')]

        # Load sql-to-stage information (this contains Exec names)
        sql_to_stage_info_file = self.props.get_value('spillBased', 'sqlToStageInfo', 'fileName')
        sql_to_stage_info_path = os.path.join(app_id_path, sql_to_stage_info_file)
        sql_to_stage_info = Utilities.read_csv(sql_to_stage_info_path, self.hdfs_fs)
        sql_to_stage_info = sql_to_stage_info[self.props.get_value('spillBased',
                                                                   'sqlToStageInfo', 'columns')]

        # Identify stages with significant spills
        # Convert the string to int because the parse_config method returns a string
        spill_threshold_bytes = int(self.props.get_value('spillBased', 'spillThresholdBytes'))
        spill_condition = stage_agg_metrics['memoryBytesSpilled_sum'] > spill_threshold_bytes
        stages_with_spills = stage_agg_metrics[spill_condition]

        # Merge stages with spills with SQL-to-stage information
        merged_df = pd.merge(stages_with_spills, sql_to_stage_info, on='stageId', how='inner')

        # Identify stages with spills caused by Execs other than the ones allowed (Join, Aggregate or Sort)
        # Note: Column 'SQL Nodes(IDs)' contains the Exec names
        pattern = '|'.join(map(re.escape, self.props.get_value('spillBased', 'allowedExecs')))
        relevant_stages_with_spills = merged_df[~merged_df['SQL Nodes(IDs)'].apply(
            lambda x: isinstance(x, str) and bool(re.search(pattern, x)))]
        # If there are any stages with spills caused by non-allowed Execs, skip the application
        if not relevant_stages_with_spills.empty:
            stages_str = '; '.join(relevant_stages_with_spills['stageId'].astype(str))
            spill_threshold_human_readable = Utilities.bytes_to_human_readable(spill_threshold_bytes)
            reason = f'Skipping due to spills in stages [{stages_str}] exceeding {spill_threshold_human_readable}.'
            return True, reason
        return False, ''

    def apply_heuristics(self, all_apps: pd.DataFrame) -> pd.DataFrame:
        try:
            self.all_apps = all_apps
            heuristics_df = self._apply_heuristics(all_apps['App ID'].unique())
            # Save the heuristics results to a file and drop the reason column
            heuristics_df.to_csv(self.output_file, index=False)
            heuristics_df.drop(columns=['Reason'], inplace=True)
            all_apps = pd.merge(all_apps, heuristics_df, on=['App ID'], how='left')
        except Exception as e:  # pylint: disable=broad-except
            import traceback
            traceback.print_exc()
            self.logger.error('Error occurred while applying additional heuristics. '
                              'Reason - %s:%s', type(e).__name__, e)
        return all_apps
