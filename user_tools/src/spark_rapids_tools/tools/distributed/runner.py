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

import os
import sys
from main import run_tool_as_spark_app

if __name__ == "__main__":
    if len(sys.argv) != 5:
        usage = f"Usage: {sys.argv[0]} <spark_master> <tools_jar_path> <platform> <eventlogs_path>"
        print(usage)
        sys.exit(1)

    spark_master = sys.argv[1]
    tools_jar_path = sys.argv[2]
    platform = sys.argv[3]
    eventlogs_path = sys.argv[4]

    if not os.path.exists(tools_jar_path):
        print(f"Error: Tools JAR does not exist: {tools_jar_path}")
        sys.exit(1)

    run_tool_as_spark_app(spark_master, tools_jar_path, platform, eventlogs_path)
