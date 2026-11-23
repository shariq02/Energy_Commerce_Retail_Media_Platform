# Model card: price_daily.price

_model `quantile_gbt_sklearn`, dataset `price_daily`, frozen version 14, primary metric `mae` (lower is better). Generated: 2026-11-23T13:07Z_

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

- frame: 681 rows, columns 100, rows by partition {'test': 681}
- held-out read: partition test, frozen_delta_version 14, rows 681, time_min 2025-01-01 00:00:00, time_max 2026-11-12 00:00:00, groups 681
- frame: 364 rows, columns 100, rows by partition {'validation': 364}

## Training

- model family: quantile_gbt_sklearn; MLflow run `7b8ca84dfa7d4ab2b5863d06b632e1fc`
- parameters: {'quantiles': [0.1, 0.5, 0.9], 'n_estimators': 300, 'learning_rate': 0.05, 'num_leaves': 31}
- library versions: not recorded
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | quantile_gbt_sklearn held-out | quantile_gbt_sklearn validation | seasonal_naive held-out |
|---|---|---|---|
| interval_coverage_80 | 0.6549 | 0.6346 |  |
| mae | 19.3 | 12.33 | 32.74 |
| mean_error | -12.37 | -4.776 | -1.893 |
| mean_error_relative | -0.1194 | -0.0608 | -0.01827 |
| pinball_q10 | 3.673 | 2.687 |  |
| pinball_q50 | 9.649 | 6.165 | 16.37 |
| pinball_q90 | 5.14 | 4.514 |  |
| primary_change_relative | 0.5651 |  | 0.1675 |
| rmse | 27.11 | 21.13 | 45.64 |
| skill_mae | 0.4106 | 0.5603 |  |
| skill_pinball_q10 | 0.5524 | 0.5369 |  |
| skill_pinball_q50 | 0.593 | 0.5773 |  |
| skill_pinball_q90 | 0.6904 | 0.7313 |  |
| skill_primary | 0.4106 |  |  |
| skill_primary_ci_high | 0.4949 |  |  |
| skill_primary_ci_low | 0.324 |  |  |
| skill_rmse | 0.4059 | 0.5074 |  |

## Flags

- validation-to-test degradation above the threshold: primary metric +56.5% worse than on validation

## Conditions and known limits

- primary metric +56.5% worse than on validation; revalidate on newer data before relying on it
- 80% interval covers 65.5%, target 80% plus or minus 10%

## Disclosures

- the held-out partition was read in at least 2 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `seasonal_naive`: validation 28.04, held-out 32.74 on `mae`
