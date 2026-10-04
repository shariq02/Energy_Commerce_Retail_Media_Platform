# Model card: capacity_additions.additions

_model `poisson_gbt_sklearn`, dataset `capacity_additions`, frozen version 0, primary metric `mae` (lower is better). Generated: 2026-10-04T17:19Z_

## Decision

- decision: `approved_with_conditions`
- restriction: `none`
- recommended by the rules: `approved_with_conditions`
- overridden by the owner: no
- decided by: owner
- rules applied: R04

## Purpose and task

- paradigm: supervised; task type: regression_count
- target: `target_capacity_added_mw`
- baseline fallback: `seasonal_mean`

## Intended use

- estimates the capacity in MW added per period
- target `target_capacity_added_mw`, dataset `capacity_additions`

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 258 rows, columns 7, rows by partition {'test': 258}
- held-out read: partition test, frozen_delta_version 0, rows 258, time_min 2025-01-01 00:00:00, time_max 2026-09-01 00:00:00, groups 21
- frame: 189 rows, columns 7, rows by partition {'validation': 189}

## Training

- model family: poisson_gbt_sklearn; MLflow run `5c359f7fb3124683b842b09f602335f3`
- parameters: {'row_fraction': 1.0, 'n_estimators': 300, 'learning_rate': 0.05, 'num_leaves': 31}
- library versions: sklearn 1.6.1
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | poisson_gbt_sklearn held-out | poisson_gbt_sklearn validation | seasonal_mean held-out |
|---|---|---|---|
| mae | 31.66 | 19.83 | 36.13 |
| mean_error | -2.7 | 7.947 | -15.06 |
| mean_error_relative | -0.06167 | 0.3001 | -0.344 |
| primary_change_relative | 0.5966 |  | 0.7652 |
| rmse | 103.4 | 62.98 | 110.5 |
| skill_mae | 0.1237 | 0.03111 |  |
| skill_primary | 0.1237 |  |  |
| skill_primary_ci_high | 0.2649 |  |  |
| skill_primary_ci_low | 0.02823 |  |  |
| skill_rmse | 0.06411 | -0.1361 |  |

## Flags

- validation-to-test degradation above the threshold: primary metric +59.7% worse than on validation

## Conditions and known limits

- primary metric +59.7% worse than on validation; revalidate on newer data before relying on it

## Disclosures

- none

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `seasonal_mean`: validation 20.47, held-out 36.13 on `mae`
