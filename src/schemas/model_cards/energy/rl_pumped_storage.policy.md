# Model card: rl_pumped_storage.policy

_model `conservative_q`, dataset `rl_pumped_storage`, frozen version 2, primary metric `reward_timing` (higher is better). Generated: 2026-10-04T17:19Z_

## Decision

- decision: `approved_with_conditions`
- restriction: `offline_only`
- recommended by the rules: `approved_with_conditions`
- overridden by the owner: no
- decided by: owner
- rules applied: R09

## Purpose and task

- paradigm: offline_rl; task type: policy
- target: `action_net_mwh`
- baseline fallback: `rule_policy`

## Intended use

- proposes the net pumped-storage action in MWh; evaluated on logged data only
- target `action_net_mwh`, dataset `rl_pumped_storage`
- restricted to offline only (see the conditions below)

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 23693 rows, columns 11, rows by partition {'test': 23693}
- held-out read: partition test, frozen_delta_version 2, rows 23693, groups 247
- frame: 17476 rows, columns 11, rows by partition {'validation': 17476}

## Training

- model family: conservative_q; MLflow run `df01f87aeada4151a852f7ad9ccc99db`
- parameters: {'row_fraction': 1.0, 'reward': 'price x net energy, price-taker', 'alpha': 1.0, 'bins': 9, 'discount': 0.0}
- library versions: torch 2.14.1+cpu
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | conservative_q held-out | conservative_q validation | rule_policy held-out |
|---|---|---|---|
| action_mae | 557.7 | 527.7 | 557.3 |
| episode_net_abs_logged | 1.26e+04 | 1.127e+04 | 1.26e+04 |
| episode_net_abs_policy | 4.42e+04 | 4.036e+04 | 2.673e+04 |
| primary_change_relative | -0.7866 |  | -2.179 |
| reward_gain | 5.327e+04 | 4.15e+04 | 1.373e+04 |
| reward_logged | 3.081e+04 | 1.483e+04 | 3.081e+04 |
| reward_policy | 8.407e+04 | 5.633e+04 | 4.453e+04 |
| reward_timing | 4.438e+04 | 2.484e+04 | 4.159e+04 |
| reward_timing_gain | 1.357e+04 | 1.001e+04 | 1.078e+04 |
| sign_agreement | 0.7711 | 0.7195 | 0.7744 |
| skill_primary | 2793 |  |  |
| skill_primary_ci_high | 4476 |  |  |
| skill_primary_ci_low | 934.8 |  |  |

## Flags

- none

## Conditions and known limits

- offline evidence only; operational feasibility is not shown
- net energy per episode 4.42e+04 for the policy against 1.26e+04 in the logged data

## Disclosures

- none

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `rule_policy`: validation 1.308e+04, held-out 4.159e+04 on `reward_timing`
