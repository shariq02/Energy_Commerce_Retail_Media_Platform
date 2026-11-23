# Model card: benchmark_gnomad_balanced.label

_model `gbt_sklearn`, dataset `benchmark_gnomad_balanced`, frozen version 56, primary metric `pr_auc` (higher is better). Generated: 2026-11-23T13:07Z_

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

- separates pathogenic from population variants on a balanced benchmark set
- target `target_label_integer`, dataset `benchmark_gnomad_balanced`

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 1781891 rows, columns 28, rows by partition {'test': 1781891}
- held-out read: partition test, frozen_delta_version 56, rows 1781891, groups 767574
- frame: 977804 rows, columns 28, rows by partition {'validation': 977804}

## Training

- model family: gbt_sklearn; MLflow run `f0427fd0113f46d4b2630f88e0735bd6`
- parameters: {'row_fraction': 0.55, 'n_estimators': 300, 'learning_rate': 0.05, 'num_leaves': 31}
- library versions: sklearn 1.7.2
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | gbt_sklearn held-out | gbt_sklearn validation | base_rate held-out |
|---|---|---|---|
| base_rate | 0.4992 | 0.5 | 0.4992 |
| calibration_error | 0.005469 | 0.006134 | 0.0007866 |
| log_loss | 0.6861 | 0.6851 | 0.6931 |
| pr_auc | 0.5585 | 0.5623 | 0.4992 |
| pr_auc_lift | 1.119 | 1.125 | 1 |
| primary_change_relative | 0.006874 |  | 0.001486 |
| skill_primary | 0.05924 |  |  |
| skill_primary_ci_high | 0.06559 |  |  |
| skill_primary_ci_low | 0.05212 |  |  |

## Flags

- none

## Conditions and known limits

- none

## Disclosures

- the held-out partition was read in at least 2 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `base_rate`: validation 0.5, held-out 0.4992 on `pr_auc`
