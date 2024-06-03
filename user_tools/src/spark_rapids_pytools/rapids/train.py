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

"""Implementation class representing wrapper around the RAPIDS acceleration QualX training tool."""

import os

from dataclasses import dataclass

from spark_rapids_tools.tools.qualx.qualx_main import train
from spark_rapids_pytools.cloud_api.sp_types import get_platform
from spark_rapids_pytools.common.sys_storage import FSUtil
from spark_rapids_pytools.common.utilities import Utils
from spark_rapids_pytools.rapids.rapids_tool import RapidsTool
from spark_rapids_pytools.rapids.tool_ctxt import ToolContext


@dataclass
class Train(RapidsTool):
    """
    Wrapper layer around QualX training tool.
    """
    dataset: str = None
    model: str = None
    n_trials: int = None

    name = 'train'

    def _check_environment(self):
        pass

    def _connect_to_execution_cluster(self):
        pass

    def _init_ctxt(self):
        # TODO: we are using qualification-conf.yaml for now, we need to change it to use the actual config file
        self.config_path = Utils.resource_path('qualification-conf.yaml')
        self.ctxt = ToolContext(platform_cls=get_platform(self.platform_type),
                                platform_opts=self.wrapper_options.get('platformOpts'),
                                prop_arg=self.config_path,
                                name=self.name)

    def _process_output_args(self):
        self.logger.debug('Processing Output Arguments')
        if self.output_folder is None:
            self.output_folder = os.getcwd()
        self.output_folder = FSUtil.get_abs_path(self.output_folder)
        exec_dir_name = f'{self.name}_{self.ctxt.uuid}'
        # It should never happen that the exec_dir_name exists
        self.output_folder = FSUtil.build_path(self.output_folder, exec_dir_name)
        FSUtil.make_dirs(self.output_folder, exist_ok=False)
        self.ctxt.set_local('outputFolder', self.output_folder)
        self.logger.info('Local output folder is set as: %s', self.output_folder)

    def _run_rapids_tool(self):
        try:
            train(dataset=self.dataset, model=self.model, output_dir=self.output_folder, n_trials=self.n_trials)
            self.logger.info('Training completed successfully.')
            self.logger.info('Trained XGBoost model is saved at: %s', self.model)
            self.logger.info('CSV file with training results are generated at: %s', self.output_folder)
        except Exception as e:  # pylint: disable=broad-except
            self.logger.error('Training failed with error: %s', e)
            raise e

    def _collect_result(self):
        pass

    def _archive_phase(self):
        pass

    def _finalize(self):
        pass
