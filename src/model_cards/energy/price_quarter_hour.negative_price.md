# Model card: price_quarter_hour.negative_price

_model `gbt_sklearn`, dataset `price_quarter_hour`, frozen version 4, primary metric `pr_auc` (higher is better). Generated: 2026-11-23T13:07Z_

## Decision

- decision: `approved`
- restriction: `none`
- recommended by the rules: `approved`
- overridden by the owner: no
- decided by: owner
- rules applied: R10

## Purpose and task

- paradigm: supervised; task type: classification
- target: `target_is_negative_price`
- baseline fallback: `base_rate`

## Intended use

- estimates the probability that a quarter-hour has a negative electricity price
- target `target_is_negative_price`, dataset `price_quarter_hour`

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 30336 rows, columns 44, rows by partition {'test': 30336}
- held-out read: partition test, frozen_delta_version 4, rows 30336, time_min 2026-01-01 00:00:00, time_max 2026-11-12 00:00:00, groups 316
- frame: 17476 rows, columns 44, rows by partition {'validation': 17476}

## Training

- model family: gbt_sklearn; MLflow run `012db10426f746fba262716d90b7c6c8`
- parameters: {'row_fraction': 1.0, 'n_estimators': 300, 'learning_rate': 0.05, 'num_leaves': 15, 'min_child_samples': 50}
- library versions: sklearn 1.7.2
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | gbt_sklearn held-out | gbt_sklearn validation | base_rate held-out |
|---|---|---|---|
| base_rate | 0.06131 | 0.04252 | 0.06131 |
| calibration_error | 0.03283 | 0.02716 | 0.01381 |
| log_loss | 0.1415 | 0.11 | 0.232 |
| pr_auc | 0.699 | 0.6071 | 0.06131 |
| pr_auc_lift | 11.4 | 14.28 | 1 |
| primary_change_relative | -0.1515 |  | -0.4421 |
| skill_primary | 0.6377 |  |  |
| skill_primary_ci_high | 0.7577 |  |  |
| skill_primary_ci_low | 0.4413 |  |  |

## Flags

- none

## Conditions and known limits

- none

## Disclosures

- the held-out partition was read in at least 2 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `base_rate`: validation 0.04252, held-out 0.06131 on `pr_auc`
