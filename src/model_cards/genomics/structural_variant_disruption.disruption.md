# Model card: structural_variant_disruption.disruption

_model `gbt_lightgbm`, dataset `structural_variant_disruption`, frozen version 51, primary metric `pr_auc` (higher is better). Generated: 2026-11-23T13:07Z_

## Decision

- decision: `approved`
- restriction: `none`
- recommended by the rules: `approved`
- overridden by the owner: no
- decided by: owner
- rules applied: R10

## Purpose and task

- paradigm: supervised; task type: classification
- target: `target_has_critical_gene_disruption`
- baseline fallback: `base_rate`

## Intended use

- estimates the probability that a structural variant disrupts a gene
- target `target_has_critical_gene_disruption`, dataset `structural_variant_disruption`

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 33068 rows, columns 29, rows by partition {'test': 33068}
- held-out read: partition test, frozen_delta_version 51, rows 33068, groups 33068
- frame: 33378 rows, columns 29, rows by partition {'validation': 33378}

## Training

- model family: gbt_lightgbm; MLflow run `7f400a5f2acf4062af651473446f8cfc`
- parameters: {'row_fraction': 1.0, 'n_estimators': 300, 'learning_rate': 0.05, 'num_leaves': 15, 'min_child_samples': 50}
- library versions: lightgbm 4.7.0
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | gbt_lightgbm held-out | gbt_lightgbm validation | base_rate held-out |
|---|---|---|---|
| base_rate | 0.8287 | 0.8285 | 0.8287 |
| calibration_error | 1.007e-07 | 9.936e-08 | 0.001544 |
| log_loss | 1.007e-07 | 9.936e-08 | 0.4579 |
| pr_auc | 1 | 1 | 0.8287 |
| pr_auc_lift | 1.207 | 1.207 | 1 |
| primary_change_relative | 0 |  | -0.0002496 |
| skill_primary | 0.1713 |  |  |
| skill_primary_ci_high | 0.1755 |  |  |
| skill_primary_ci_low | 0.1667 |  |  |

## Flags

- none

## Conditions and known limits

- none

## Disclosures

- the held-out partition was read in at least 2 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `base_rate`: validation 0.8285, held-out 0.8287 on `pr_auc`
