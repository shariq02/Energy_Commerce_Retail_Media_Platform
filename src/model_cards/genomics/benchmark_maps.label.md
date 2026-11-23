# Model card: benchmark_maps.label

_model `gbt_lightgbm`, dataset `benchmark_maps`, frozen version 56, primary metric `pr_auc` (higher is better). Generated: 2026-11-23T13:07Z_

## Decision

- decision: `approved`
- restriction: `none`
- recommended by the rules: `approved`
- overridden by the owner: no
- decided by: owner
- rules applied: R10

## Purpose and task

- paradigm: supervised; task type: classification
- target: `target_label_integer`
- baseline fallback: `base_rate`

## Intended use

- scores variants on the MAPS benchmark set
- target `target_label_integer`, dataset `benchmark_maps`

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 12565 rows, columns 28, rows by partition {'test': 12565}
- held-out read: partition test, frozen_delta_version 56, rows 12565
- frame: 17950 rows, columns 28, rows by partition {'validation': 17950}

## Training

- model family: gbt_lightgbm; MLflow run `3445df88dc9b4f44aacaa45283c90030`
- parameters: {'row_fraction': 1.0, 'n_estimators': 300, 'learning_rate': 0.05, 'num_leaves': 31}
- library versions: lightgbm 4.7.0
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | gbt_lightgbm held-out | gbt_lightgbm validation | base_rate held-out |
|---|---|---|---|
| base_rate | 0.4851 | 0.2725 | 0.4851 |
| calibration_error | 0.1973 | 0.02445 | 0.2139 |
| log_loss | 0.6752 | 0.3973 | 0.7959 |
| pr_auc | 0.7629 | 0.7436 | 0.4851 |
| pr_auc_lift | 1.573 | 2.729 | 1 |
| primary_change_relative | -0.02597 |  | -0.7802 |
| skill_primary | 0.2778 |  |  |
| skill_primary_ci_high | 0.2878 |  |  |
| skill_primary_ci_low | 0.2667 |  |  |

## Flags

- none

## Conditions and known limits

- none

## Disclosures

- the held-out partition was read in at least 2 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `base_rate`: validation 0.2725, held-out 0.4851 on `pr_auc`
