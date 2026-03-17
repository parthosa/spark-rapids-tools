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

## Surface AutoTuner-only inputs in tool output CSVs

The AutoTuner computes several inputs in-memory that are never written to tool
output. This makes them inaccessible to external consumers reading CSVs.

**Issue**: #XXXX
**Design doc**: [`docs/design/issue-autotuner-output-gaps.md`](issue-autotuner-output-gaps.md)

---

### Changes

**1. `application_information.csv` — 4 new columns (both profiling and qualification)**

| Column | Source |
|--------|--------|
| `maxTaskInputBytesRead` | `SQLMaxTaskInputSizes` (prof) / `rawAggMetrics.maxTaskInputSizes` (qual) |
| `maxColumnarExchangeDataSizeBytes` | `sqlMetrics` filtered to `ColumnarExchange` + `data size` (prof only) |
| `scanStagesWithGpuOom` | `AppInfoGpuOomCheck.hasScanStagesWithGpuOom` stage IDs (prof only) |
| `shuffleStagesWithOom` | `AppInfoGpuOomCheck.hasShuffleStagesWithOom` stage IDs (prof only) |

Qualification writes empty values for OOM and ColumnarExchange columns (CPU event
logs don't produce these signals).

**2. `shuffle_skew_check.csv` — new file for qualification output**

Qualification already computes `taskShuffleSkew` in `rawAggMetrics` but never
writes it. This change produces the same `shuffle_skew_check.csv` that profiling
already outputs, using the same schema.

### What is NOT changing

- No computation logic changes — all values already computed in-memory
- No schema changes to `failed_stages.csv`, `failed_tasks.csv`, or any other file
- No behavioral changes to AutoTuner recommendations
- Profiling `shuffle_skew_check.csv` output unchanged

### Testing

- Verify new columns appear in `application_information.csv` for both profiling
  and qualification with correct values
- Verify `shuffle_skew_check.csv` is produced for qualification output
- Existing `ProfilingAutoTunerSuite` and `QualificationAutoTunerSuite` tests
  remain green (no behavioral change)
