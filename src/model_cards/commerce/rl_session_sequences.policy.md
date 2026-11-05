# Model card: rl_session_sequences.policy

_model `sequence_gru`, dataset `rl_session_sequences`, frozen version 9, primary metric `action_agreement` (higher is better). Generated: 2026-10-26T17:52Z_

## Decision

- decision: `approved_with_conditions`
- restriction: `offline_only`
- recommended by the rules: `approved_with_conditions`
- overridden by the owner: no
- decided by: owner
- rules applied: R02, R09, R10

## Purpose and task

- paradigm: offline_rl; task type: policy
- target: `target_action_event_type`
- baseline fallback: `most_frequent_event`

## Intended use

- proposes the next event type of a session; evaluated on logged data only
- target `target_action_event_type`, dataset `rl_session_sequences`
- restricted to offline only (see the conditions below)

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 1053871 rows, columns 12, rows by partition {'test': 1053871}
- held-out read: partition test, frozen_delta_version 9, rows 1053871, groups 56030
- frame: 1014461 rows, columns 12, rows by partition {'validation': 1014461}

## Training

- model family: sequence_gru; MLflow run `acbac24e0b264663a0762dca8fa8b91b`
- parameters: {'row_fraction': 1.0, 'history': 10, 'epochs': 2}
- library versions: torch 2.9.0+cu129
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | sequence_gru held-out | sequence_gru validation | most_frequent_event held-out |
|---|---|---|---|
| action_agreement | 0.9439 | 0.9445 | 0.9433 |
| primary_change_relative | 0.0006818 |  | 0.0009683 |
| reward_weighted_agreement | 0.3527 | 0.3337 | 0 |
| reward_when_agree | 2.019 | 1.764 | 0 |
| reward_when_disagree | 62.3 | 59.97 | 95.22 |
| skill_primary | 0.0006054 |  |  |
| skill_primary_ci_high | 0.003587 |  |  |
| skill_primary_ci_low | -0.0005137 |  |  |

## Flags

- skill interval includes zero: interval -0.0005137 to 0.003587

## Conditions and known limits

- skill interval -0.0005137 to 0.003587 includes zero
- offline evidence only; operational feasibility is not shown

## Disclosures

- the held-out partition was read in at least 2 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `most_frequent_event`: validation 0.9442, held-out 0.9433 on `action_agreement`
