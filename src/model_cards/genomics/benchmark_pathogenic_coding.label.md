# Model card: benchmark_pathogenic_coding.label

_model `gbt_lightgbm`, dataset `benchmark_pathogenic_coding`, frozen version 56, primary metric `pr_auc` (higher is better). Generated: 2026-11-23T13:07Z_

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

- separates pathogenic from benign coding variants on a benchmark set
- target `target_label_integer`, dataset `benchmark_pathogenic_coding`

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 1018 rows, columns 28, rows by partition {'test': 1018}
- held-out read: partition test, frozen_delta_version 56, rows 1018
- frame: 5719 rows, columns 28, rows by partition {'validation': 5719}

## Training

- model family: gbt_lightgbm; MLflow run `9c9e61685882438f98c13f79d4ad6230`
- parameters: {'row_fraction': 1.0, 'n_estimators': 300, 'learning_rate': 0.05, 'num_leaves': 31}
- library versions: lightgbm 4.7.0
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | gbt_lightgbm held-out | gbt_lightgbm validation | base_rate held-out |
|---|---|---|---|
| base_rate | 0.4528 | 0.5567 | 0.4528 |
| calibration_error | 1.52e-07 | 1.48e-07 | 0.1125 |
| log_loss | 1.52e-07 | 1.48e-07 | 0.7142 |
| pr_auc | 1 | 1 | 0.4528 |
| pr_auc_lift | 2.208 | 1.796 | 1 |
| primary_change_relative | 0 |  | 0.1866 |
| skill_primary | 0.5472 |  |  |
| skill_primary_ci_high | 0.5738 |  |  |
| skill_primary_ci_low | 0.5167 |  |  |

## Flags

- none

## Conditions and known limits

- none

## Disclosures

- the held-out partition was read in at least 2 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `base_rate`: validation 0.5567, held-out 0.4528 on `pr_auc`
