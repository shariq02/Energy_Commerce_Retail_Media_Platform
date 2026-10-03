# Model card: price_quarter_hour.price

_model `quantile_gbt_lightgbm`, dataset `price_quarter_hour`, frozen version 0, primary metric `mae` (lower is better). Generated: 2026-10-03T23:11Z_

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

- estimates `target_price_eur_per_mwh` for the entities and period of dataset `price_quarter_hour`
- comparison and analysis with the stated conditions below

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
- library versions: cloudpickle 3.0.0, lifelines 0.30.3, lightgbm 4.7.0, psutil 5.9.0, sklearn 1.6.1, sksurv 0.25.0, torch 2.14.1+cpu, xgboost 3.4.1
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | quantile_gbt_lightgbm held-out | quantile_gbt_lightgbm validation | persistence held-out |
|---|---|---|---|
| bootstrap_blocks | 9 |  |  |
| bootstrap_resamples_valid | 200 |  |  |
| bootstrap_rows_used | 2.38e+04 |  |  |
| interval_coverage_80 | 0.494 | 0.6224 |  |
| mae | 29.45 | 17.74 | 37.3 |
| mean_error | -13.27 | -3.306 | 0.1774 |
| mean_error_relative | -0.1252 | -0.03742 | 0.001674 |
| pinball_q10 | 5.905 | 4.085 |  |
| pinball_q50 | 14.72 | 8.87 | 18.65 |
| pinball_q90 | 9.628 | 5.362 |  |
| pred_finite_share | 1 |  | 1 |
| pred_outside_range_share | 0 |  | 0 |
| pred_std | 49.02 |  | 63.29 |
| pred_std_ratio | 0.7712 | 0.8947 | 0.9957 |
| primary_change_relative | 0.6599 |  | 0.1254 |
| reproduced_validation_value | 17.74 |  | 33.15 |
| reproduction_gap_relative | 0 |  | 0 |
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

- held-out partition read 2 times (re-runs); scores unchanged

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `persistence`: validation 33.15, held-out 37.3 on `mae`
