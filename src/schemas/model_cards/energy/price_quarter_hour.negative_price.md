# Model card: price_quarter_hour.negative_price

_model `logistic`, dataset `price_quarter_hour`, frozen version 0, primary metric `pr_auc` (higher is better). Generated: 2026-10-04T18:03Z_

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

- frame: 23804 rows, columns 44, rows by partition {'test': 23804}
- held-out read: partition test, frozen_delta_version 0, rows 23804, time_min 2026-01-01 00:00:00, time_max 2026-09-05 00:00:00, groups 248
- frame: 17476 rows, columns 44, rows by partition {'validation': 17476}

## Training

- model family: logistic; MLflow run `cdad4ea678a1436b9c3f730f3a634d3a`
- parameters: {'row_fraction': 1.0, 'C': 0.1}
- library versions: sklearn 1.6.1
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | logistic held-out | logistic validation | base_rate held-out |
|---|---|---|---|
| base_rate | 0.07381 | 0.04252 | 0.07381 |
| calibration_error | 0.03368 | 0.01277 | 0.008823 |
| log_loss | 0.1287 | 0.08613 | 0.264 |
| pr_auc | 0.7205 | 0.6168 | 0.07381 |
| pr_auc_lift | 9.761 | 14.51 | 1 |
| primary_change_relative | -0.1681 |  | -0.7361 |
| skill_primary | 0.6467 |  |  |
| skill_primary_ci_high | 0.735 |  |  |
| skill_primary_ci_low | 0.4814 |  |  |

## Flags

- none

## Conditions and known limits

- none

## Disclosures

- the held-out partition was read in at least 2 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `base_rate`: validation 0.04252, held-out 0.07381 on `pr_auc`
