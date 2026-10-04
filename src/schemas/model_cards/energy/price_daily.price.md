# Model card: price_daily.price

_model `quantile_gbt_lightgbm`, dataset `price_daily`, frozen version 0, primary metric `mae` (lower is better). Generated: 2026-10-04T18:03Z_

## Decision

- decision: `approved_with_conditions`
- restriction: `none`
- recommended by the rules: `approved_with_conditions`
- overridden by the owner: no
- decided by: owner
- rules applied: R04, R08, R10

## Purpose and task

- paradigm: supervised; task type: regression_price
- target: `target_price_eur_per_mwh`
- baseline fallback: `seasonal_naive`

## Intended use

- estimates the daily electricity price in EUR per MWh
- target `target_price_eur_per_mwh`, dataset `price_daily`

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 613 rows, columns 90, rows by partition {'test': 613}
- held-out read: partition test, frozen_delta_version 0, rows 613, time_min 2025-01-01 00:00:00, time_max 2026-09-05 00:00:00, groups 613
- frame: 364 rows, columns 90, rows by partition {'validation': 364}

## Training

- model family: quantile_gbt_lightgbm; MLflow run `db846a81684e45939e4584e2c5eb9c10`
- parameters: {'quantiles': [0.1, 0.5, 0.9], 'n_estimators': 300, 'learning_rate': 0.05, 'num_leaves': 31}
- library versions: lightgbm 4.7.0
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | quantile_gbt_lightgbm held-out | quantile_gbt_lightgbm validation | seasonal_naive held-out |
|---|---|---|---|
| interval_coverage_80 | 0.5677 | 0.5824 |  |
| mae | 15.95 | 11.81 | 28.62 |
| mean_error | -8.262 | -4.773 | -0.09514 |
| mean_error_relative | -0.08692 | -0.06076 | -0.001001 |
| pinball_q10 | 3.331 | 2.668 |  |
| pinball_q50 | 7.973 | 5.906 | 14.31 |
| pinball_q90 | 4.734 | 4.362 |  |
| primary_change_relative | 0.35 |  | 0.02071 |
| rmse | 21.22 | 19.71 | 38.47 |
| skill_mae | 0.4429 | 0.5788 |  |
| skill_pinball_q10 | 0.5514 | 0.5403 |  |
| skill_pinball_q50 | 0.6026 | 0.595 |  |
| skill_pinball_q90 | 0.6839 | 0.7404 |  |
| skill_primary | 0.4429 |  |  |
| skill_primary_ci_high | 0.5128 |  |  |
| skill_primary_ci_low | 0.3526 |  |  |
| skill_rmse | 0.4484 | 0.5407 |  |

## Flags

- validation-to-test degradation above the threshold: primary metric +35.0% worse than on validation

## Conditions and known limits

- primary metric +35.0% worse than on validation; revalidate on newer data before relying on it
- 80% interval covers 56.8%, target 80% plus or minus 10%

## Disclosures

- the held-out partition was read in at least 2 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `seasonal_naive`: validation 28.04, held-out 28.62 on `mae`
