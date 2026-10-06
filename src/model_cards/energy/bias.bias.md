# Model card: bias.bias

_model `gbt_lightgbm`, dataset `bias`, frozen version 0, primary metric `mae` (lower is better). Generated: 2026-10-04T18:03Z_

## Decision

- decision: `approved_with_conditions`
- restriction: `none`
- recommended by the rules: `approved_with_conditions`
- overridden by the owner: no
- decided by: owner
- rules applied: R07, R11

## Purpose and task

- paradigm: supervised; task type: regression
- target: `target_bias_mwh`
- baseline fallback: `zero_correction`

## Intended use

- estimates the bias of the published generation forecast in MWh, per forecast scope
- target `target_bias_mwh`, dataset `bias`

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 3049 rows, columns 91, rows by partition {'test': 3049}
- held-out read: partition test, frozen_delta_version 0, rows 3049, time_min 2025-01-01 00:00:00, time_max 2026-09-03 00:00:00, groups 611
- frame: 1819 rows, columns 91, rows by partition {'validation': 1819}

## Training

- model family: gbt_lightgbm; MLflow run `c41cb25a0b3343969578f27cc0b55883`
- parameters: {'row_fraction': 1.0, 'n_estimators': 300, 'learning_rate': 0.05, 'num_leaves': 31}
- library versions: lightgbm 4.7.0
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | gbt_lightgbm held-out | gbt_lightgbm validation | zero_correction held-out |
|---|---|---|---|
| mae | 1.625e+04 | 1.533e+04 | 1.025e+05 |
| mean_error | -1827 | -725.7 | 8.691e+04 |
| mean_error_relative | -0.01782 | -0.007749 | 0.848 |
| primary_change_relative | 0.05955 |  | 0.09444 |
| rmse | 2.379e+04 | 2.288e+04 | 2.105e+05 |
| skill_mae | 0.8415 | 0.8363 |  |
| skill_primary | 0.8415 |  |  |
| skill_primary_ci_high | 0.8523 |  |  |
| skill_primary_ci_low | 0.8292 |  |  |
| skill_rmse | 0.8869 | 0.8835 |  |

## Segments

| segment | skill | decision | detail |
|---|---|---|---|
| offshore_wind | -0.1053 | not_approved | negative skill |
| onshore_wind | 0.03132 | approved_with_conditions | no segment interval |
| other | 0.9718 | approved_with_conditions | no segment interval |
| photovoltaic | 0.02954 | approved_with_conditions | no segment interval |
| wind_and_photovoltaic | 0.03343 | approved_with_conditions | no segment interval |

## Flags

- none

## Conditions and known limits

- segments not approved: offshore_wind
- segments with positive skill but no uncertainty interval, conditional until an interval is recorded: onshore_wind (0.0313), other (0.972), photovoltaic (0.0295), wind_and_photovoltaic (0.0334)

## Disclosures

- none

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `zero_correction`: validation 9.365e+04, held-out 1.025e+05 on `mae`
