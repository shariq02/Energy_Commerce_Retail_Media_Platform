# Model card: honda_anomaly.anomaly

_model `forecast_residual_sklearn`, dataset `honda_anomaly`, frozen version 0, primary metric `pr_auc` (higher is better). Generated: 2026-10-04T18:03Z_

## Decision

- decision: `approved_with_conditions`
- restriction: `offline_only`
- recommended by the rules: `approved_with_conditions`
- overridden by the owner: no
- decided by: owner
- rules applied: R09

## Purpose and task

- paradigm: unsupervised; task type: anomaly
- target: `injected_anomaly`
- baseline fallback: `seasonal_zscore`

## Intended use

- scores Honda site energy readings for anomalies; the evidence rests on injected anomalies
- target `injected_anomaly`, dataset `honda_anomaly`
- restricted to offline only (see the conditions below)

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 26277 rows, columns 20, rows by partition {'test': 26277}
- held-out read: partition test, frozen_delta_version 0, rows 26277, time_min 2022-12-31 23:00:00, time_max 2023-12-31 21:00:00, groups 8759

## Training

- model family: forecast_residual_sklearn; MLflow run `41c192648f6441488d6897f624d75b48`
- parameters: {'inject_rate': 0.02, 'flag_quantile': 0.99, 'injected_rows': 537, 'row_fraction': 1.0}
- library versions: sklearn 1.6.1
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | forecast_residual_sklearn held-out | forecast_residual_sklearn validation | seasonal_zscore held-out |
|---|---|---|---|
| flag_rate | 0.02984 | 0.03156 | 0.03124 |
| injected_rows | 575 |  | 575 |
| pr_auc | 0.5587 | 0.6517 | 0.357 |
| precision | 0.3251 | 0.3112 | 0.266 |
| primary_change_relative | 0.1428 |  | 0.1923 |
| recall | 0.6435 | 0.6909 | 0.5061 |
| skill_primary | 0.2016 |  |  |
| skill_primary_ci_high | 0.3074 |  |  |
| skill_primary_ci_low | 0.08569 |  |  |
| threshold | 4.323 | 4.323 | 2.546 |

## Flags

- none

## Conditions and known limits

- offline evidence only; operational feasibility is not shown

## Disclosures

- none

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `seasonal_zscore`: validation 0.442, held-out 0.357 on `pr_auc`
