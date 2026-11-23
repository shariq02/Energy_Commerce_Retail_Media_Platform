# Model card: session_purchase_ga4.purchase

_model `gbt_sklearn`, dataset `session_purchase_ga4`, frozen version 15, primary metric `pr_auc` (higher is better). Generated: 2026-11-23T13:07Z_

## Decision

- decision: `approved`
- restriction: `none`
- recommended by the rules: `approved`
- overridden by the owner: no
- decided by: owner
- rules applied: R10

## Purpose and task

- paradigm: supervised; task type: classification
- target: `target_purchase_after_prefix`
- baseline fallback: `base_rate`

## Intended use

- estimates the probability that a GA4 session ends in a purchase, from the first events of the session
- target `target_purchase_after_prefix`, dataset `session_purchase_ga4`

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 2724 rows, columns 21, rows by partition {'test': 2724}
- held-out read: partition test, frozen_delta_version 15, rows 2724, groups 2180
- frame: 2609 rows, columns 21, rows by partition {'validation': 2609}

## Training

- model family: gbt_sklearn; MLflow run `7bd160bd287f48429b95e51816080d80`
- parameters: {'row_fraction': 1.0, 'n_estimators': 300, 'learning_rate': 0.05, 'num_leaves': 15, 'min_child_samples': 50}
- library versions: sklearn 1.7.2
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | gbt_sklearn held-out | gbt_sklearn validation | base_rate held-out |
|---|---|---|---|
| base_rate | 0.2551 | 0.2376 | 0.2551 |
| calibration_error | 0.02338 | 0.02134 | 0.01122 |
| log_loss | 0.5052 | 0.5016 | 0.5682 |
| pr_auc | 0.4607 | 0.3944 | 0.2551 |
| pr_auc_lift | 1.806 | 1.66 | 1 |
| primary_change_relative | -0.1681 |  | -0.07364 |
| skill_primary | 0.2056 |  |  |
| skill_primary_ci_high | 0.2364 |  |  |
| skill_primary_ci_low | 0.1742 |  |  |

## Flags

- none

## Conditions and known limits

- none

## Disclosures

- the held-out partition was read in at least 2 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `base_rate`: validation 0.2376, held-out 0.2551 on `pr_auc`
