# Model card: weak_supervision.labels

_model `label_model`, dataset `weak_supervision`, frozen version 2, primary metric `coverage` (higher is better). Generated: 2026-10-03T23:11Z_

## Decision

- decision: `approved_with_conditions`
- restriction: `diagnostic_only`
- recommended by the rules: `approved_with_conditions`
- overridden by the owner: no
- decided by: owner
- rules applied: R02, R06

## Purpose and task

- paradigm: weak_supervision; task type: label_model
- target: `lf_label`
- baseline fallback: `majority_vote`

## Intended use

- estimates `lf_label` for the entities and period of dataset `weak_supervision`
- comparison and analysis with the stated conditions below

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 173756 rows, columns 6, rows by partition {'train': 121682, 'test': 26046, 'validation': 26028}
- held-out read: partition test, frozen_delta_version 2, rows 26046, groups 25650

## Training

- model family: label_model; MLflow run `285b09e3bb8f44fa966137b42024a406`
- parameters: {'em_iterations': 50, 'note': 'two correlated functions; accuracies not identifiable', 'applies_to': 'entity types with two or more functions'}
- library versions: cloudpickle 3.0.0, lifelines 0.30.3, lightgbm 4.7.0, psutil 5.9.0, sklearn 1.6.1, sksurv 0.25.0, torch 2.14.1+cpu, xgboost 3.4.1
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | label_model held-out | label_model validation | majority_vote held-out |
|---|---|---|---|
| bootstrap_blocks | 2.565e+04 |  |  |
| bootstrap_resamples_valid | 200 |  |  |
| bootstrap_rows_used | 2.585e+04 |  |  |
| coverage | 1 | 1 | 1 |
| primary_change_relative | 0 |  | 0 |
| reproduced_validation_value | 1 |  | 1 |
| reproduction_gap_relative | 0 |  | 0 |
| skill_primary | 0 |  |  |
| skill_primary_ci_high | 0 |  |  |
| skill_primary_ci_low | 0 |  |  |

## Flags

- skill interval includes zero: interval 0 to 0
- no skill over the baseline: skill 0

## Conditions and known limits

- skill interval 0 to 0 includes zero
- no ground truth: diagnostic use only

## Disclosures

- none

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `majority_vote`: validation 1, held-out 1 on `coverage`
