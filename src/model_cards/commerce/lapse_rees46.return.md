# Model card: lapse_rees46.return

_model `gbt_lightgbm`, dataset `lapse_rees46`, frozen version 8, primary metric `pr_auc` (higher is better). Generated: 2026-11-23T13:07Z_

## Decision

- decision: `approved`
- restriction: `none`
- recommended by the rules: `approved`
- overridden by the owner: no
- decided by: owner
- rules applied: R10

## Purpose and task

- paradigm: supervised; task type: classification
- target: `target_returned`
- baseline fallback: `base_rate`

## Intended use

- estimates the probability that a REES46 user returns
- target `target_returned`, dataset `lapse_rees46`

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 453593 rows, columns 12, rows by partition {'test': 453593}
- held-out read: partition test, frozen_delta_version 8, rows 453593, groups 453593
- frame: 453829 rows, columns 12, rows by partition {'validation': 453829}

## Training

- model family: gbt_lightgbm; MLflow run `daf1393e038c44ce93e9838892204afc`
- parameters: {'row_fraction': 1.0, 'n_estimators': 300, 'learning_rate': 0.05, 'num_leaves': 31}
- library versions: lightgbm 4.7.0
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | gbt_lightgbm held-out | gbt_lightgbm validation | base_rate held-out |
|---|---|---|---|
| base_rate | 0.463 | 0.4634 | 0.463 |
| calibration_error | 0.002309 | 0.00207 | 0.0001588 |
| log_loss | 0.59 | 0.5906 | 0.6904 |
| pr_auc | 0.7321 | 0.7319 | 0.463 |
| pr_auc_lift | 1.581 | 1.58 | 1 |
| primary_change_relative | -0.0002916 |  | 0.0008643 |
| skill_primary | 0.2691 |  |  |
| skill_primary_ci_high | 0.2711 |  |  |
| skill_primary_ci_low | 0.2632 |  |  |

## Flags

- none

## Conditions and known limits

- none

## Disclosures

- the held-out partition was read in at least 2 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `base_rate`: validation 0.4634, held-out 0.463 on `pr_auc`
