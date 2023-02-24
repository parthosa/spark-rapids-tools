# Copyright (c) 2023, NVIDIA CORPORATION.
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

"""Abstract class of providing absolute costs of resources in CSP"""

import datetime
import math
import os
from dataclasses import dataclass, field
from logging import Logger

from spark_rapids_pytools.cloud_api.sp_types import ClusterBase
from spark_rapids_pytools.common.prop_manager import AbstractPropertiesContainer, JSONPropertiesContainer
from spark_rapids_pytools.common.sys_storage import FSUtil
from spark_rapids_pytools.common.utilities import ToolLogging, Utils


@dataclass
class PriceProvider:
    """
    An abstract class that represents interface to retrieve costs of hardware configurations.
    """
    region: str
    pricing_config: JSONPropertiesContainer
    cache_file: str = field(default=None, init=False)
    resource_url: str = field(default=None, init=False)
    name: str = field(default=None, init=False)
    cache_expiration_secs: int = field(default=604800, init=False)  # download the file once a week
    meta: dict = field(default_factory=dict)
    catalog: AbstractPropertiesContainer = field(default=None, init=False)
    comments: list = field(default_factory=lambda: [], init=False)
    cache_directory: str = field(default=None, init=False)
    logger: Logger = field(default=None, init=False)

    def _init_cache_file(self):
        if self._caches_expired(self.get_cached_files()):
            self._generate_cache_file()
        else:
            self.logger.info('The catalog files are loaded from the cache: %s',
                             '; '.join(self.get_cached_files()))

    def _generate_cache_file(self):
        files_updated = FSUtil.cache_from_url(self.resource_url, self.cache_file)
        self.logger.info('The catalog file %s is %s',
                         self.cache_file,
                         'updated' if files_updated else 'is not modified, using the cached content')

    def __post_init__(self):
        self.logger = ToolLogging.get_and_setup_logger(f'rapids.tools.price.{self.name}')
        self.cache_directory = Utils.get_rapids_tools_env('CACHE_FOLDER')
        self._process_configs()
        self._init_catalog()

    def get_cached_files(self) -> list:
        return [self.cache_file]

    def _caches_expired(self, cache_files: list) -> bool:
        for c_file in cache_files:
            if not os.path.exists(c_file):
                return True
            modified_time = os.path.getmtime(c_file)
            diff_time = int(datetime.datetime.now().timestamp() - modified_time)
            if diff_time > self.cache_expiration_secs:
                return True
        return False

    def _process_resource_configs(self):
        pass

    def _process_configs(self):
        self._process_resource_configs()

    def _create_catalog(self):
        pass

    def _init_catalog(self):
        self._init_cache_file()
        self._create_catalog()

    def get_cpu_price(self, machine_type: str) -> float:
        del machine_type  # Unused machine_type
        return 0.0

    def get_container_cost(self) -> float:
        return 0.0

    def get_ssd_price(self, machine_type: str) -> float:
        del machine_type  # Unused machine_type
        return 0.0

    def get_ram_price(self, machine_type: str) -> float:
        del machine_type  # Unused machine_type
        return 0.0

    def get_gpu_price(self, gpu_device: str) -> float:
        del gpu_device  # Unused gpu_device
        return 0.0

    def setup(self, **kwargs) -> None:
        for key, value in kwargs.items():
            self.meta[key] = value


@dataclass
class SavingsEstimator:
    """
    Implementation of model to get an estimate of cost savings.
    """
    price_provider: PriceProvider
    target_cluster: ClusterBase
    source_cluster: ClusterBase
    target_cost: float = field(default=None, init=False)
    source_cost: float = field(default=None, init=False)
    comments: list = field(default_factory=lambda: [], init=False)
    logger: Logger = field(default=None, init=False)

    def _setup_costs(self):
        # calculate target_cost
        pass

    def __post_init__(self):
        # when debug is set to true set it in the environment.
        self.logger = ToolLogging.get_and_setup_logger('rapids.tools.savings')
        self._setup_costs()

    def get_costs_and_savings(self,
                              app_duration_ms: float,
                              estimated_gpu_duration_ms: float) -> (float, float, float):
        """
        Calculates the cost of running an application for both clusters and returns the savings as a
        percentage.
        :param app_duration_ms: total execution time in milliseconds
        :param estimated_gpu_duration_ms: estimated execution time of the app if executed on GPU
        :return: a tuple of 3 floats representing cpu_cost, gpu_cost, and percent of savings
        """
        cpu_cost = self.source_cost * app_duration_ms / (60.0 * 60 * 1000)
        if cpu_cost <= 0.0:
            self.logger.info('Force costs to 0 because the original cost is %.6f', cpu_cost)
            # avoid division by zero
            return 0.0, 0.0, 0.0
        gpu_cost = self.target_cost * estimated_gpu_duration_ms / (60.0 * 60 * 1000)
        estimated_savings = 100.0 - ((100.0 * gpu_cost) / cpu_cost)
        cpu_cost_res = math.ceil(cpu_cost * 100) / 100
        gpu_cost_res = math.ceil(gpu_cost * 100) / 100
        estimated_savings_res = math.ceil(estimated_savings * 100) / 100
        return cpu_cost_res, gpu_cost_res, estimated_savings_res