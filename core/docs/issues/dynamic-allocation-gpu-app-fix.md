## Summary

The `recommendDynamicAllocationConfigs` function in `AutoTuner.scala` incorrectly applies CPU-to-GPU core ratio adjustments to dynamic allocation properties even when the source application is already GPU-enabled. This results in incorrect recommendations and potential inconsistencies between `spark.executor.instances` and `spark.dynamicAllocation.maxExecutors`.

## Observed Behavior

For a GPU application on g2-standard-48 (12 cores per executor) with:
- `spark.dynamicAllocation.maxExecutors=148`
- `spark.executor.instances=148`
- `spark.executor.cores=11`

The AutoTuner recommends:
- `spark.dynamicAllocation.maxExecutors=135` (incorrectly reduced via `floor(148 × 11/12)`)
- `spark.executor.instances=148` (unchanged, now inconsistent with maxExecutors)

## Root Cause

The `recommendDynamicAllocationConfigs` function applies a CPU-to-GPU core ratio adjustment formula:

```
GPU_value = max(1, floor(CPU_value × CPU_cores / GPU_cores))
```

This formula is designed to scale executor counts when converting a **CPU application** to run on GPU. However, it is being applied to **all applications**, including those that are already GPU-enabled. For GPU apps, the user's settings are already optimized for GPU execution, and this adjustment is both unnecessary and incorrect.
