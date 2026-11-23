# Model card: benchmark_proteingym_clinical.label

_model `gbt_lightgbm`, dataset `benchmark_proteingym_clinical`, frozen version 56, primary metric `pr_auc` (higher is better). Generated: 2026-11-23T13:07Z_

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

- separates clinically pathogenic from benign missense variants
- target `target_label_integer`, dataset `benchmark_proteingym_clinical`

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 10036 rows, columns 27, rows by partition {'test': 10036}
- held-out read: partition test, frozen_delta_version 56, rows 10036, groups 381
- frame: 9343 rows, columns 27, rows by partition {'validation': 9343}

## Training

- model family: gbt_lightgbm; MLflow run `aee0eb6c9e9c439fa6edfd6feb39ad51`
- parameters: {'row_fraction': 1.0, 'n_estimators': 300, 'learning_rate': 0.05, 'num_leaves': 15, 'min_child_samples': 50}
- library versions: lightgbm 4.7.0
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | gbt_lightgbm held-out | gbt_lightgbm validation | base_rate held-out |
|---|---|---|---|
| base_rate | 0.5181 | 0.4891 | 0.5181 |
| calibration_error | 0.01261 | 0.0154 | 0.005308 |
| log_loss | 0.5979 | 0.5907 | 0.6925 |
| pr_auc | 0.7464 | 0.7355 | 0.5181 |
| pr_auc_lift | 1.441 | 1.504 | 1 |
| primary_change_relative | -0.01485 |  | -0.05929 |
| skill_primary | 0.2283 |  |  |
| skill_primary_ci_high | 0.2723 |  |  |
| skill_primary_ci_low | 0.1841 |  |  |

## Flags

- none

## Conditions and known limits

- none

## Disclosures

- the held-out partition was read in at least 2 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `base_rate`: validation 0.4891, held-out 0.5181 on `pr_auc`
