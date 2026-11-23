# Model card: bias.bias

_model `gbt_lightgbm`, dataset `bias`, frozen version 14, primary metric `mae` (lower is better). Generated: 2026-11-23T13:07Z_

## Decision

- decision: `approved_with_conditions`
- restriction: `none`
- recommended by the rules: `approved_with_conditions`
- overridden by the owner: no
- decided by: owner
- rules applied: R07, R10, R11

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

- frame: 3379 rows, columns 100, rows by partition {'test': 3379}
- held-out read: partition test, frozen_delta_version 14, rows 3379, time_min 2025-01-01 00:00:00, time_max 2026-11-10 00:00:00, groups 679
- frame: 1819 rows, columns 100, rows by partition {'validation': 1819}

## Training

- model family: gbt_lightgbm; MLflow run `b75aa35aa30c4aed8ce5f0ef3e7d6fe3`
- parameters: {'row_fraction': 1.0, 'n_estimators': 300, 'learning_rate': 0.05, 'num_leaves': 31}
- library versions: lightgbm 4.7.0
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | gbt_lightgbm held-out | gbt_lightgbm validation | zero_correction held-out |
|---|---|---|---|
| mae | 1.657e+04 | 1.534e+04 | 1.033e+05 |
| mean_error | -1146 | -235.3 | 8.743e+04 |
| mean_error_relative | -0.01109 | -0.002513 | 0.8461 |
| primary_change_relative | 0.0803 |  | 0.1034 |
| rmse | 2.422e+04 | 2.292e+04 | 2.122e+05 |
| skill_mae | 0.8396 | 0.8362 |  |
| skill_primary | 0.8396 |  |  |
| skill_primary_ci_high | 0.852 |  |  |
| skill_primary_ci_low | 0.8269 |  |  |
| skill_rmse | 0.8859 | 0.8833 |  |

## Segments

| segment | skill | decision | detail |
|---|---|---|---|
| offshore_wind | -0.03177 | not_approved | negative skill |
| onshore_wind | 0.02731 | approved_with_conditions | no segment interval |
| other | 0.9705 | approved_with_conditions | no segment interval |
| photovoltaic | 0.03703 | approved_with_conditions | no segment interval |
| wind_and_photovoltaic | 0.02561 | approved_with_conditions | no segment interval |

## Flags

- none

## Conditions and known limits

- segments not approved: offshore_wind
- segments with positive skill but no uncertainty interval, conditional until an interval is recorded: onshore_wind (0.0273), other (0.97), photovoltaic (0.037), wind_and_photovoltaic (0.0256)

## Disclosures

- the held-out partition was read in at least 2 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `zero_correction`: validation 9.365e+04, held-out 1.033e+05 on `mae`
