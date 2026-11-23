# Model card: benchmark_causal_eqtl.label

_model `gbt_lightgbm`, dataset `benchmark_causal_eqtl`, frozen version 56, primary metric `pr_auc` (higher is better). Generated: 2026-11-23T13:07Z_

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

- separates causal from non-causal expression variants on a benchmark set
- target `target_label_integer`, dataset `benchmark_causal_eqtl`

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 8862 rows, columns 28, rows by partition {'test': 8862}
- held-out read: partition test, frozen_delta_version 56, rows 8862
- frame: 13501 rows, columns 28, rows by partition {'validation': 13501}

## Training

- model family: gbt_lightgbm; MLflow run `09c82c67350b41b5a3525c834bace082`
- parameters: {'row_fraction': 1.0, 'n_estimators': 300, 'learning_rate': 0.05, 'num_leaves': 31}
- library versions: lightgbm 4.7.0
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | gbt_lightgbm held-out | gbt_lightgbm validation | base_rate held-out |
|---|---|---|---|
| base_rate | 0.5141 | 0.4983 | 0.5141 |
| calibration_error | 0.07809 | 0.03998 | 0.01546 |
| log_loss | 0.6558 | 0.5316 | 0.6932 |
| pr_auc | 0.6954 | 0.8033 | 0.5141 |
| pr_auc_lift | 1.353 | 1.612 | 1 |
| primary_change_relative | 0.1344 |  | -0.03165 |
| skill_primary | 0.1813 |  |  |
| skill_primary_ci_high | 0.1934 |  |  |
| skill_primary_ci_low | 0.1694 |  |  |

## Flags

- none

## Conditions and known limits

- none

## Disclosures

- the held-out partition was read in at least 2 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `base_rate`: validation 0.4983, held-out 0.5141 on `pr_auc`
