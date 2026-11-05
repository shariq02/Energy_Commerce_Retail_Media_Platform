# Model card: capacity_additions.additions

_model `tabpfn_25_in_context`, dataset `capacity_additions`, frozen version 0, primary metric `mae` (lower is better). Generated: 2026-10-26T17:52Z_

## Decision

- decision: `approved_with_conditions`
- restriction: `none`
- recommended by the rules: `approved_with_conditions`
- overridden by the owner: no
- decided by: owner
- rules applied: R04, R10, R13

## Purpose and task

- paradigm: supervised; task type: regression_count
- target: `target_capacity_added_mw`
- baseline fallback: `seasonal_mean`

## Intended use

- estimates the capacity in MW added per period
- target `target_capacity_added_mw`, dataset `capacity_additions`

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 258 rows, columns 7, rows by partition {'test': 258}
- held-out read: partition test, frozen_delta_version 0, rows 258, time_min 2025-01-01 00:00:00, time_max 2026-09-01 00:00:00, groups 21
- frame: 189 rows, columns 7, rows by partition {'validation': 189}

## Training

- model family: tabpfn_25_in_context; MLflow run `e44820d0f73f44bba7299d2d2dc27d39`
- parameters: {'row_fraction': 1.0, 'model_key': 'tabpfn_25', 'repo_id': 'Prior-Labs/tabpfn_2_5', 'revision': '6c45f3a6d0d07c6c5f62572e04a0c2929de91b8b', 'licence': 'tabpfn-2.5-non-commercial', 'mode': 'in_context', 'context_rows': 972, 'train_rows': 972, 'features': 3, 'seed': 42}
- library versions: torch 2.9.0+cu129, tabpfn 9.2.0
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | tabpfn_25_in_context held-out | tabpfn_25_in_context validation | seasonal_mean held-out |
|---|---|---|---|
| mae | 31.64 | 14.1 | 36.13 |
| mean_error | -25.28 | -10.47 | -15.06 |
| mean_error_relative | -0.5774 | -0.3953 | -0.344 |
| primary_change_relative | 1.245 |  | 0.7652 |
| rmse | 114.6 | 48.52 | 110.5 |
| skill_mae | 0.1243 | 0.3113 |  |
| skill_primary | 0.1243 |  |  |
| skill_primary_ci_high | 0.1953 |  |  |
| skill_primary_ci_low | 0.0513 |  |  |
| skill_rmse | -0.03724 | 0.1248 |  |

## Flags

- validation-to-test degradation above the threshold: primary metric +124.5% worse than on validation
- stored model does not reproduce its validation result: relative gap 0.00031

## Conditions and known limits

- primary metric +124.5% worse than on validation; revalidate on newer data before relying on it
- contamination flag unresolved: the pretraining data of the model was not shown to be free of the evaluated data; revision 6c45f3a6d0d0; pretraining data per register: synthetic tables from the TabPFN prior; read the model card; task data names found: none

## Disclosures

- the held-out partition was read in at least 5 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged
- foundation model Prior-Labs/tabpfn_2_5 at revision 6c45f3a6d0d0, licence tabpfn-2.5-non-commercial, ran in context

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `seasonal_mean`: validation 20.47, held-out 36.13 on `mae`
