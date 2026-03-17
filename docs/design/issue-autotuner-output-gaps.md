<!--
  Copyright (c) 2026, NVIDIA CORPORATION.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# AutoTuner Inputs Missing from Tool Output

## Problem

Several inputs consumed by the AutoTuner are computed in-memory but never written
to tool output files. External consumers that read tool CSVs cannot access these
signals.

**4 missing inputs:**

1. **`maxTaskInputBytesRead`** — max per-task input bytes across all SQLs. Used by
   `calculateMaxPartitionBytesInMB()` to set `spark.sql.files.maxPartitionBytes`.
   Explicitly excluded from output: "Not added to the output since it is used only
   by the AutoTuner" (`ProfileClassWarehouse.scala:1084`).

2. **`shuffle_skew_check.csv` for qualification** — profiling produces this file
   but qualification does not, even though `QualAppSummaryInfoProvider` computes
   `taskShuffleSkew` in-memory. Used by `recommendShufflePartitionsInternal()` to
   detect skew-based partition adjustments.

3. **GPU scan OOM stage IDs** — `hasScanStagesWithGpuOom` is computed in-memory
   by `SingleAppSummaryInfoProvider` (profiling only). Used by
   `calculateMaxPartitionBytesInMB()` to halve partition size on GPU OOM.

4. **Shuffle OOM stage IDs** — `hasShuffleStagesWithOom` is computed in-memory
   by `SingleAppSummaryInfoProvider` (profiling only, YARN). Used by
   `recommendShufflePartitionsInternal()` to increase partitions on container OOM.

## Proposed Changes

### `application_information.csv` — 4 new columns

| Column | Type | Prof | Qual |
|--------|------|------|------|
| `maxTaskInputBytesRead` | double | from `SQLMaxTaskInputSizes` | from `rawAggMetrics.maxTaskInputSizes` |
| `maxColumnarExchangeDataSizeBytes` | long or empty | from sqlMetrics on `ColumnarExchange` nodes | empty |
| `scanStagesWithGpuOom` | comma-separated stage IDs | from `AppInfoGpuOomCheck` | empty |
| `shuffleStagesWithOom` | comma-separated stage IDs | from `AppInfoGpuOomCheck` | empty |

### `shuffle_skew_check.csv` — produce for qualification

Same schema as profiling. Data source: `rawAggMetrics.taskShuffleSkew` (already
computed by `QualAppSummaryInfoProvider`).

## Notes

- No computation logic changes — all values are already computed in-memory
- Only change is writing them to existing CSV files
- `AppInfoGpuOomCheck` trait is on `BaseProfilingAppSummaryInfoProvider` only;
  qualification hardcodes empty values
