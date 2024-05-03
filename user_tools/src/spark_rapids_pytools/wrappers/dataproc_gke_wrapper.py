# Copyright (c) 2023-2024, NVIDIA CORPORATION.
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

"""Wrapper class to run tools associated with RAPIDS Accelerator for Apache Spark plugin on Dataproc."""

from spark_rapids_pytools.cloud_api.sp_types import DeployMode
from spark_rapids_pytools.common.utilities import Utils, ToolLogging
from spark_rapids_pytools.rapids.qualification import QualFilterApp, QualificationAsLocal, QualGpuClusterReshapeType
from spark_rapids_tools import CspEnv
from spark_rapids_tools.utils import Utilities


class CliDataprocGKELocalMode:  # pylint: disable=too-few-public-methods
    """
    A wrapper that runs the RAPIDS Accelerator tools locally on the dev machine for Dataproc GKE.
    """

    @staticmethod
    def qualification(cpu_cluster: str = None,
                      eventlogs: str = None,
                      local_folder: str = None,
                      remote_folder: str = None,
                      gpu_cluster: str = None,
                      tools_jar: str = None,
                      credentials_file: str = None,
                      filter_apps: str = QualFilterApp.tostring(QualFilterApp.get_default()),
                      gpu_cluster_recommendation: str = QualGpuClusterReshapeType.tostring(
                          QualGpuClusterReshapeType.get_default()),
                      estimation_model: str = None,
                      jvm_heap_size: int = None,
                      verbose: bool = None,
                      cpu_discount: int = None,
                      gpu_discount: int = None,
                      global_discount: int = None,
                      **rapids_options) -> None:
        """
        The Qualification tool analyzes Spark events generated from CPU based Spark applications to
        help quantify the expected acceleration and costs savings of migrating a Spark application
        or query to GPU. The wrapper downloads dependencies and executes the analysis on the local
        dev machine
        :param cpu_cluster: The virtual Dataproc-cluster on which the Apache Spark applications were executed.
                Accepted values are a virtual Dataproc-cluster name, or a valid path to the cluster properties
                file (json format) generated by gcloud CLI command `gcloud dataproc clusters describe`. This
                should not be confused with the GKE cluster name.
        :param  eventlogs: Event log filenames or gcs storage directories
                containing event logs (comma separated). If missing, the wrapper Reads the Spark's
                property `spark.eventLog.dir` defined in `cpu_cluster`. This property should be included
                in the output of `gcloud dataproc clusters describe`
                Note that the wrapper will raise an exception if the property is not set
        :param local_folder: Local work-directory path to store the output and to be used as root
                directory for temporary folders/files. The final output will go into a subdirectory called
                ${local_folder}/qual-${EXEC_ID} where exec_id is an auto-generated unique identifier of the
                execution. If the argument is NONE, the default value is the env variable
                RAPIDS_USER_TOOLS_OUTPUT_DIRECTORY if any; or the current working directory
        :param remote_folder: A gcs folder where the output is uploaded at the end of execution.
                If no value is provided, the output will be only available on local disk
        :param gpu_cluster: The virtual Dataproc-cluster on which the Spark applications is planned to be
                migrated. The argument can be a virtual Dataproc-cluster or a valid path to the cluster's
                properties file (json format) generated by the gcloud CLI command `gcloud dataproc clusters
                describe`. This should not be confused with the GKE cluster name.
        :param tools_jar: Path to a bundled jar including Rapids tool. The path is a local filesystem,
                or remote gcs url. If missing, the wrapper downloads the latest rapids-4-spark-tools_*.jar
                from maven repo
        :param credentials_file: The local path of JSON file that contains the application credentials.
               If missing, the wrapper looks for "GOOGLE_APPLICATION_CREDENTIALS" environment variable
               to provide the location of a credential JSON file. The default credentials file exists as
               "$HOME/.config/gcloud/application_default_credentials.json"
        :param filter_apps: filtering criteria of the applications listed in the final STDOUT table
                is one of the following (all, speedups, savings, top_candidates).
                Note that this filter does not affect the CSV report.
                "all" means no filter applied. "speedups" lists all the apps that are either
                'Recommended', or 'Strongly Recommended' based on speedups. "savings"
                lists all the apps that have positive estimated GPU savings except for the apps that
                are "Not Applicable". "top_candidates" lists all apps that have unsupported operators
                stage duration less than 25% of app duration and speedups greater than 1.3x.
        :param gpu_cluster_recommendation: The type of GPU cluster recommendation to generate.
               It accepts one of the following ("CLUSTER", "JOB" and the default value "MATCH").
                "MATCH": keep GPU cluster same number of nodes as CPU cluster;
                "CLUSTER": recommend optimal GPU cluster by cost for entire cluster;
                "JOB": recommend optimal GPU cluster by cost per job
        :param estimation_model: Model used to calculate the estimated GPU duration and cost savings.
               It accepts one of the following:
               "xgboost": an XGBoost model for GPU duration estimation
               "speedups": set by default. It uses a simple static estimated speedup per operator.
        :param jvm_heap_size: The maximum heap size of the JVM in gigabytes
        :param verbose: True or False to enable verbosity to the wrapper script
        :param cpu_discount: A percent discount for the cpu cluster cost in the form of an integer value
                (e.g. 30 for 30% discount).
        :param gpu_discount: A percent discount for the gpu cluster cost in the form of an integer value
                (e.g. 30 for 30% discount).
        :param global_discount: A percent discount for both the cpu and gpu cluster costs in the form of an
                integer value (e.g. 30 for 30% discount).
        :param rapids_options: A list of valid Qualification tool options.
                Note that the wrapper ignores ["output-directory", "platform"] flags, and it does not support
                multiple "spark-property" arguments.
                For more details on Qualification tool options, please visit
                https://docs.nvidia.com/spark-rapids/user-guide/latest/qualification/jar-usage.html#running-the-qualification-tool-standalone-on-spark-event-logs
        """
        verbose = Utils.get_value_or_pop(verbose, rapids_options, 'v', False)
        remote_folder = Utils.get_value_or_pop(remote_folder, rapids_options, 'r')
        jvm_heap_size = Utils.get_value_or_pop(jvm_heap_size, rapids_options, 'j',
                                               Utilities.get_system_memory_in_gb())
        eventlogs = Utils.get_value_or_pop(eventlogs, rapids_options, 'e')
        filter_apps = Utils.get_value_or_pop(filter_apps, rapids_options, 'f')
        tools_jar = Utils.get_value_or_pop(tools_jar, rapids_options, 't')
        local_folder = Utils.get_value_or_pop(local_folder, rapids_options, 'l')
        if verbose:
            # when debug is set to true set it in the environment.
            ToolLogging.enable_debug_mode()
        wrapper_qual_options = {
            'platformOpts': {
                'credentialFile': credentials_file,
                'deployMode': DeployMode.LOCAL,
            },
            'migrationClustersProps': {
                'cpuCluster': cpu_cluster,
                'gpuCluster': gpu_cluster
            },
            'jobSubmissionProps': {
                'remoteFolder': remote_folder,
                'platformArgs': {
                    'jvmMaxHeapSize': jvm_heap_size
                }
            },
            'eventlogs': eventlogs,
            'filterApps': filter_apps,
            'toolsJar': tools_jar,
            'gpuClusterRecommendation': gpu_cluster_recommendation,
            'cpuDiscount': cpu_discount,
            'gpuDiscount': gpu_discount,
            'globalDiscount': global_discount,
            'estimationModel': estimation_model
        }

        tool_obj = QualificationAsLocal(platform_type=CspEnv.DATAPROC_GKE,
                                        output_folder=local_folder,
                                        wrapper_options=wrapper_qual_options,
                                        rapids_options=rapids_options)
        tool_obj.launch()


class DataprocGKEWrapper:  # pylint: disable=too-few-public-methods
    """
    A wrapper script to run RAPIDS Accelerator tool (Qualification) on Gcloud Dataproc GKE.
    """
    def __init__(self):
        self.qualification = CliDataprocGKELocalMode.qualification
