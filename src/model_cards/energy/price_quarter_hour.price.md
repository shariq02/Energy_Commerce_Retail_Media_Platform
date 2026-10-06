# Model card: price_quarter_hour.price

_model `quantile_gbt_lightgbm`, dataset `price_quarter_hour`, frozen version 0, primary metric `mae` (lower is better). Generated: 2026-10-04T18:03Z_

## Decision

- decision: `approved_with_conditions`
- restriction: `none`
- recommended by the rules: `approved_with_conditions`
- overridden by the owner: no
- decided by: owner
- rules applied: R02, R04, R08, R10

## Purpose and task

- paradigm: supervised; task type: regression_price
- target: `target_price_eur_per_mwh`
- baseline fallback: `persistence`

## Intended use

- estimates the quarter-hour electricity price in EUR per MWh
- target `target_price_eur_per_mwh`, dataset `price_quarter_hour`

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 23804 rows, columns 45, rows by partition {'test': 23804}
- held-out read: partition test, frozen_delta_version 0, rows 23804, time_min 2026-01-01 00:00:00, time_max 2026-09-05 00:00:00, groups 248
- frame: 17476 rows, columns 45, rows by partition {'validation': 17476}

## Training

- model family: quantile_gbt_lightgbm; MLflow run `cb36ec00aad54e7c8bf831ffed77c6d5`
- parameters: {'quantiles': [0.1, 0.5, 0.9], 'n_estimators': 300, 'learning_rate': 0.05, 'num_leaves': 31}
- library versions: lightgbm 4.7.0
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | quantile_gbt_lightgbm held-out | quantile_gbt_lightgbm validation | persistence held-out |
|---|---|---|---|
| interval_coverage_80 | 0.494 | 0.6224 |  |
| mae | 29.45 | 17.74 | 37.3 |
| mean_error | -13.27 | -3.306 | 0.1774 |
| mean_error_relative | -0.1252 | -0.03742 | 0.001674 |
| pinball_q10 | 5.905 | 4.085 |  |
| pinball_q50 | 14.72 | 8.87 | 18.65 |
| pinball_q90 | 9.628 | 5.362 |  |
| primary_change_relative | 0.6599 |  | 0.1254 |
| rmse | 42.29 | 27.92 | 57.28 |
| skill_mae | 0.2107 | 0.4648 |  |
| skill_pinball_q10 | 0.4676 | 0.4985 |  |
| skill_pinball_q50 | 0.4039 | 0.4391 |  |
| skill_pinball_q90 | 0.2121 | 0.4344 |  |
| skill_primary | 0.2107 |  |  |
| skill_primary_ci_high | 0.36 |  |  |
| skill_primary_ci_low | -0.02785 |  |  |
| skill_rmse | 0.2616 | 0.4594 |  |

## Flags

- validation-to-test degradation above the threshold: primary metric +66.0% worse than on validation
- skill interval includes zero: interval -0.02785 to 0.36

## Conditions and known limits

- skill interval -0.02785 to 0.36 includes zero
- primary metric +66.0% worse than on validation; revalidate on newer data before relying on it
- 80% interval covers 49.4%, target 80% plus or minus 10%

## Disclosures

- the held-out partition was read in at least 2 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `persistence`: validation 33.15, held-out 37.3 on `mae`
