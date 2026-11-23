# Model card: rl_pumped_storage.policy

_model `conservative_q`, dataset `rl_pumped_storage`, frozen version 12, primary metric `reward_timing` (higher is better). Generated: 2026-11-23T13:07Z_

## Decision

- decision: `approved_with_conditions`
- restriction: `offline_only`
- recommended by the rules: `approved_with_conditions`
- overridden by the owner: no
- decided by: owner
- rules applied: R09, R10

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

- frame: 30086 rows, columns 11, rows by partition {'test': 30086}
- held-out read: partition test, frozen_delta_version 12, rows 30086, groups 314
- frame: 17476 rows, columns 11, rows by partition {'validation': 17476}

## Training

- model family: conservative_q; MLflow run `e4b45f2ab6cf40399eac15af5639b75a`
- parameters: {'row_fraction': 1.0, 'reward': 'price x net energy, price-taker', 'alpha': 1.0, 'bins': 9, 'discount': 0.0}
- library versions: torch 2.14.1+cpu
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | conservative_q held-out | conservative_q validation | rule_policy held-out |
|---|---|---|---|
| action_mae | 607 | 563.3 | 579.7 |
| episode_net_abs_logged | 1.302e+04 | 1.127e+04 | 1.302e+04 |
| episode_net_abs_policy | 4.993e+04 | 4.424e+04 | 3.064e+04 |
| primary_change_relative | -0.7263 |  | -3.152 |
| reward_gain | 7.139e+04 | 4.538e+04 | 2.503e+04 |
| reward_logged | 3.008e+04 | 1.483e+04 | 3.008e+04 |
| reward_policy | 1.015e+05 | 6.021e+04 | 5.51e+04 |
| reward_timing | 5.264e+04 | 3.049e+04 | 4.906e+04 |
| reward_timing_gain | 2.256e+04 | 1.566e+04 | 1.899e+04 |
| sign_agreement | 0.7317 | 0.679 | 0.7393 |
| skill_primary | 3577 |  |  |
| skill_primary_ci_high | 4859 |  |  |
| skill_primary_ci_low | 2387 |  |  |

## Flags

- none

## Conditions and known limits

- offline evidence only; operational feasibility is not shown
- net energy per episode 4.993e+04 for the policy against 1.302e+04 in the logged data

## Disclosures

- the held-out partition was read in at least 2 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `rule_policy`: validation 1.182e+04, held-out 4.906e+04 on `reward_timing`
