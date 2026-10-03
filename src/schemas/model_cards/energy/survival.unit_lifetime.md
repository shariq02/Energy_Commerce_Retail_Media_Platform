# Model card: survival.unit_lifetime

_model `survival_forest`, dataset `survival`, frozen version 0, primary metric `concordance` (higher is better). Generated: 2026-10-03T23:11Z_

## Decision

- decision: `approved`
- restriction: `none`
- recommended by the rules: `approved`
- overridden by the owner: no
- decided by: owner
- rules applied: R10

## Purpose and task

- paradigm: survival; task type: survival
- target: `target_duration_years`
- baseline fallback: `kaplan_meier`

## Intended use

- estimates `target_duration_years` for the entities and period of dataset `survival`
- comparison and analysis with the stated conditions below

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 5244 rows, columns 21, rows by partition {'test': 5244}
- held-out read: partition test, frozen_delta_version 0, rows 5244, groups 5244
- frame: 5243 rows, columns 21, rows by partition {'validation': 5243}
- events in the frozen data, train: 1895 of 24469 rows
- events in the frozen data, validation: 406 of 5243 rows
- events in the held-out partition: not recorded

## Training

- model family: survival_forest; MLflow run `04ffcdafa746415b93443fa14253e54b`
- parameters: {'entry': 'ignored', 'row_fraction': 1.0}
- library versions: cloudpickle 3.0.0, lifelines 0.30.3, lightgbm 4.7.0, psutil 5.9.0, sklearn 1.6.1, sksurv 0.25.0, torch 2.14.1+cpu, xgboost 3.4.1
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | survival_forest held-out | survival_forest validation | kaplan_meier held-out |
|---|---|---|---|
| bootstrap_blocks | 5244 |  |  |
| bootstrap_resamples_valid | 200 |  |  |
| bootstrap_rows_used | 5244 |  |  |
| brier_1y | 0.0008775 | 0.0007608 | 0.001169 |
| brier_3y | 0.001194 | 0.001825 | 0.001779 |
| brier_5y | 0.0009867 | 0.001729 | 0.001779 |
| calibration_gap_1y | 0.000237 | 0.0007636 | 3.065e-05 |
| calibration_gap_3y | 0.002679 | 0.002123 | 0.0003227 |
| calibration_gap_5y | 0.00359 | 0.002643 | 4.052e-05 |
| concordance | 0.9626 | 0.9642 | 0.5031 |
| pred_finite_share | 1 |  | 1 |
| pred_std | 517.1 |  | 0.0003374 |
| primary_change_relative | 0.001632 |  | 0.001769 |
| reproduced_validation_value | 0.9642 |  | 0.504 |
| reproduction_gap_relative | 0 |  | 0 |
| skill_primary | 0.4595 |  |  |
| skill_primary_ci_high | 0.4673 |  |  |
| skill_primary_ci_low | 0.4516 |  |  |

## Flags

- none

## Conditions and known limits

- none

## Disclosures

- The number of events in the held-out partition was not recorded; the concordance and Brier scores rest on an unknown number of events.
- held-out partition read 4 times (re-runs); scores unchanged

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `kaplan_meier`: validation 0.504, held-out 0.5031 on `concordance`
