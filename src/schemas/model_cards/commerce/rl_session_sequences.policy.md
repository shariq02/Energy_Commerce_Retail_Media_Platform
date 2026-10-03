# Model card: rl_session_sequences.policy

_model `sequence_gru`, dataset `rl_session_sequences`, frozen version 0, primary metric `action_agreement` (higher is better). Generated: 2026-10-03T23:11Z_

## Decision

- decision: `approved_with_conditions`
- restriction: `offline_only`
- recommended by the rules: `approved_with_conditions`
- overridden by the owner: no
- decided by: owner
- rules applied: R02, R09

## Purpose and task

- paradigm: offline_rl; task type: policy
- target: `target_action_event_type`
- baseline fallback: `most_frequent_event`

## Intended use

- estimates `target_action_event_type` for the entities and period of dataset `rl_session_sequences`
- comparison and analysis with the stated conditions below

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 1053871 rows, columns 12, rows by partition {'test': 1053871}
- held-out read: partition test, frozen_delta_version 0, rows 1053871, groups 56030
- frame: 1014461 rows, columns 12, rows by partition {'validation': 1014461}

## Training

- model family: sequence_gru; MLflow run `17c2f675e55f4197a5f33a77e81c7974`
- parameters: {'row_fraction': 1.0, 'history': 10, 'epochs': 2}
- library versions: cloudpickle 3.0.0, lifelines 0.30.3, lightgbm 4.7.0, psutil 5.9.0, sklearn 1.6.1, sksurv 0.25.0, torch 2.14.1+cpu, xgboost 3.4.1
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | sequence_gru held-out | sequence_gru validation | most_frequent_event held-out |
|---|---|---|---|
| action_agreement | 0.9439 | 0.9445 | 0.9433 |
| bootstrap_blocks | 5332 |  |  |
| bootstrap_resamples_valid | 200 |  |  |
| bootstrap_rows_used | 1.049e+05 |  |  |
| primary_change_relative | 0.0006808 |  | 0.0009683 |
| reproduced_validation_value | 0.9445 |  | 0.9442 |
| reproduction_gap_relative | 0 |  | 0 |
| reward_weighted_agreement | 0.3527 | 0.3337 | 0 |
| reward_when_agree | 2.019 | 1.764 | 0 |
| reward_when_disagree | 62.3 | 59.97 | 95.22 |
| skill_primary | 0.0006063 |  |  |
| skill_primary_ci_high | 0.003587 |  |  |
| skill_primary_ci_low | -0.0005137 |  |  |

## Flags

- skill interval includes zero: interval -0.0005137 to 0.003587

## Conditions and known limits

- skill interval -0.0005137 to 0.003587 includes zero
- offline evidence only; operational feasibility is not shown

## Disclosures

- none

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `most_frequent_event`: validation 0.9442, held-out 0.9433 on `action_agreement`
