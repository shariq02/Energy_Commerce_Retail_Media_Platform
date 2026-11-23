# Model card: price_quarter_hour.price

_model `quantile_gbt_sklearn`, dataset `price_quarter_hour`, frozen version 4, primary metric `mae` (lower is better). Generated: 2026-11-23T13:07Z_

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

- frame: 30336 rows, columns 45, rows by partition {'test': 30336}
- held-out read: partition test, frozen_delta_version 4, rows 30336, time_min 2026-01-01 00:00:00, time_max 2026-11-12 00:00:00, groups 316
- frame: 17476 rows, columns 45, rows by partition {'validation': 17476}

## Training

- model family: quantile_gbt_sklearn; MLflow run `25cb89d283824549ba16ddd1bd5dc18c`
- parameters: {'quantiles': [0.1, 0.5, 0.9], 'n_estimators': 300, 'learning_rate': 0.05, 'num_leaves': 31}
- library versions: not recorded
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | quantile_gbt_sklearn held-out | quantile_gbt_sklearn validation | persistence held-out |
|---|---|---|---|
| interval_coverage_80 | 0.405 | 0.593 |  |
| mae | 38.89 | 18.43 | 42.93 |
| mean_error | -25.06 | -1.202 | -1.078 |
| mean_error_relative | -0.2053 | -0.01361 | -0.008831 |
| pinball_q10 | 7.057 | 4.627 |  |
| pinball_q50 | 19.45 | 9.217 | 21.47 |
| pinball_q90 | 17.73 | 5.535 |  |
| primary_change_relative | 1.11 |  | 0.2952 |
| rmse | 55.86 | 29.48 | 65.12 |
| skill_mae | 0.09407 | 0.4439 |  |
| skill_pinball_q10 | 0.4349 | 0.4425 |  |
| skill_pinball_q50 | 0.3566 | 0.4251 |  |
| skill_pinball_q90 | 0.1262 | 0.4211 |  |
| skill_primary | 0.09407 |  |  |
| skill_primary_ci_high | 0.2341 |  |  |
| skill_primary_ci_low | -0.1238 |  |  |
| skill_rmse | 0.1422 | 0.4292 |  |

## Flags

- validation-to-test degradation above the threshold: primary metric +111.0% worse than on validation
- skill interval includes zero: interval -0.1238 to 0.2341

## Conditions and known limits

- skill interval -0.1238 to 0.2341 includes zero
- primary metric +111.0% worse than on validation; revalidate on newer data before relying on it
- 80% interval covers 40.5%, target 80% plus or minus 10%

## Disclosures

- the held-out partition was read in at least 2 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `persistence`: validation 33.15, held-out 42.93 on `mae`
