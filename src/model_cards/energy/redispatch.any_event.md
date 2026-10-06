# Model card: redispatch.any_event

_model `logistic`, dataset `redispatch`, frozen version 2, primary metric `pr_auc` (higher is better). Generated: 2026-10-04T18:03Z_

## Decision

- decision: `approved`
- restriction: `none`
- recommended by the rules: `approved`
- overridden by the owner: no
- decided by: owner
- rules applied: none

## Purpose and task

- paradigm: supervised; task type: classification
- target: `target_any_event`
- baseline fallback: `base_rate`

## Intended use

- estimates the probability that a redispatch event occurs
- target `target_any_event`, dataset `redispatch`

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 1464 rows, columns 32, rows by partition {'test': 1464}
- held-out read: partition test, frozen_delta_version 2, rows 1464, time_min 2020-01-01 00:00:00, time_max 2020-12-31 00:00:00, groups 366
- frame: 1452 rows, columns 32, rows by partition {'validation': 1452}

## Training

- model family: logistic; MLflow run `18a8365730ef481797230770cf89b5ec`
- parameters: {'row_fraction': 1.0, 'C': 1.0}
- library versions: sklearn 1.6.1
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | logistic held-out | logistic validation | base_rate held-out |
|---|---|---|---|
| base_rate | 0.7008 | 0.6749 | 0.7008 |
| calibration_error | 0.03913 | 0.04501 | 0.0589 |
| log_loss | 0.5004 | 0.5223 | 0.6179 |
| pr_auc | 0.8976 | 0.8828 | 0.7008 |
| pr_auc_lift | 1.281 | 1.308 | 1 |
| primary_change_relative | -0.01675 |  | -0.03836 |
| skill_primary | 0.1968 |  |  |
| skill_primary_ci_high | 0.2322 |  |  |
| skill_primary_ci_low | 0.1526 |  |  |

## Flags

- none

## Conditions and known limits

- none

## Disclosures

- none

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `base_rate`: validation 0.6749, held-out 0.7008 on `pr_auc`
