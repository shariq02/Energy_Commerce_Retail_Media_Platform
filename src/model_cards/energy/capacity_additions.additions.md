# Model card: capacity_additions.additions

_model `poisson_gbt_sklearn`, dataset `capacity_additions`, frozen version 34, primary metric `mae` (lower is better). Generated: 2026-11-23T13:07Z_

## Decision

- decision: `approved_with_conditions`
- restriction: `none`
- recommended by the rules: `approved_with_conditions`
- overridden by the owner: no
- decided by: owner
- rules applied: R04, R10

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

- frame: 168 rows, columns 7, rows by partition {'test': 168}
- held-out read: partition test, frozen_delta_version 34, rows 168, time_min 2025-01-01 00:00:00, time_max 2026-09-01 00:00:00, groups 21
- frame: 100 rows, columns 7, rows by partition {'validation': 100}

## Training

- model family: poisson_gbt_sklearn; MLflow run `8e17abec9d32448dada33b2526fe61b5`
- parameters: {'row_fraction': 1.0, 'n_estimators': 300, 'learning_rate': 0.05, 'num_leaves': 31}
- library versions: sklearn 1.7.2
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | poisson_gbt_sklearn held-out | poisson_gbt_sklearn validation | seasonal_mean held-out |
|---|---|---|---|
| mae | 94.99 | 75.02 | 173.8 |
| mean_error | -15 | -2.05 | -134.2 |
| mean_error_relative | -0.05771 | -0.008471 | -0.5162 |
| primary_change_relative | 0.2661 |  | 0.1435 |
| rmse | 180.5 | 140.4 | 301.3 |
| skill_mae | 0.4535 | 0.5064 |  |
| skill_primary | 0.4535 |  |  |
| skill_primary_ci_high | 0.5369 |  |  |
| skill_primary_ci_low | 0.3075 |  |  |
| skill_rmse | 0.4007 | 0.5101 |  |

## Flags

- validation-to-test degradation above the threshold: primary metric +26.6% worse than on validation

## Conditions and known limits

- primary metric +26.6% worse than on validation; revalidate on newer data before relying on it

## Disclosures

- the held-out partition was read in at least 2 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `seasonal_mean`: validation 152, held-out 173.8 on `mae`
