# Model card: ccpp.output

_model `tabicl_v2_in_context`, dataset `ccpp`, frozen version 0, primary metric `mae` (lower is better). Generated: 2026-10-26T17:52Z_

## Decision

- decision: `approved_with_conditions`
- restriction: `none`
- recommended by the rules: `approved_with_conditions`
- overridden by the owner: no
- decided by: owner
- rules applied: R10, R13

## Purpose and task

- paradigm: supervised; task type: regression
- target: `target_net_output_mw`
- baseline fallback: `train_mean`

## Intended use

- estimates the net electrical output in MW of a combined cycle power plant from its operating conditions
- target `target_net_output_mw`, dataset `ccpp`

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 1447 rows, columns 8, rows by partition {'test': 1447}
- held-out read: partition test, frozen_delta_version 0, rows 1447, groups 1447
- frame: 1369 rows, columns 8, rows by partition {'validation': 1369}

## Training

- model family: tabicl_v2_in_context; MLflow run `a1d3a2b1b3f9434788b65c8788ca2a04`
- parameters: {'row_fraction': 1.0, 'model_key': 'tabicl_v2', 'repo_id': 'jingang/TabICL', 'revision': '4dcd344ece2c00be9e831fdd35bed57b5ad83e19', 'licence': 'bsd-3-clause', 'mode': 'in_context', 'context_rows': 6711, 'train_rows': 6711, 'features': 4, 'seed': 42}
- library versions: torch 2.9.0+cu129, tabicl 2.2.0
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | tabicl_v2_in_context held-out | tabicl_v2_in_context validation | train_mean held-out |
|---|---|---|---|
| mae | 1.882 | 1.927 | 15.03 |
| mean_error | -0.05196 | -0.08142 | 0.01432 |
| mean_error_relative | -0.0001144 | -0.000179 | 3.152e-05 |
| primary_change_relative | -0.02328 |  | 0.01593 |
| rmse | 2.804 | 2.989 | 17.28 |
| skill_mae | 0.8748 | 0.8698 |  |
| skill_primary | 0.8748 |  |  |
| skill_primary_ci_high | 0.8832 |  |  |
| skill_primary_ci_low | 0.8659 |  |  |
| skill_rmse | 0.8378 | 0.8243 |  |

## Flags

- stored model does not reproduce its validation result: relative gap 0.000317

## Conditions and known limits

- contamination flag unresolved: the pretraining data of the model was not shown to be free of the evaluated data; revision 4dcd344ece2c; pretraining data per register: synthetic tables from the TabICL prior; read the model card; task data names found: none

## Disclosures

- the held-out partition was read in at least 5 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged
- foundation model jingang/TabICL at revision 4dcd344ece2c, licence bsd-3-clause, ran in context

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `train_mean`: validation 14.79, held-out 15.03 on `mae`
