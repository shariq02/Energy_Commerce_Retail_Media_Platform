# Model card: honda_forecast.increment

_model `chronos2_lora`, dataset `honda_forecast`, frozen version 0, primary metric `mae` (lower is better). Generated: 2026-10-26T17:52Z_

## Decision

- decision: `approved_with_conditions`
- restriction: `none`
- recommended by the rules: `approved_with_conditions`
- overridden by the owner: no
- decided by: owner
- rules applied: R10, R11, R13

## Purpose and task

- paradigm: supervised; task type: regression
- target: `target_increment`
- baseline fallback: `persistence`

## Intended use

- estimates the next energy increment of a Honda site channel
- target `target_increment`, dataset `honda_forecast`

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 131394 rows, columns 22, rows by partition {'test': 131394}
- held-out read: partition test, frozen_delta_version 0, rows 131394, time_min 2023-01-01 00:00:00, time_max 2023-12-31 00:00:00, groups 365
- frame: 131040 rows, columns 22, rows by partition {'validation': 131040}

## Training

- model family: chronos2_lora; MLflow run `600096b94e724f10b5f0e2cd15b76b07`
- parameters: {'row_fraction': 1.0, 'lora_learning_rate': 1e-05, 'lora_num_steps': 500, 'lora_batch_size': 32, 'lora_context_length': 512, 'lora_max_series': 200, 'model_key': 'chronos2', 'repo_id': 'amazon/chronos-2', 'revision': '29ec3766d36d6f73f0696f85560a422f50e8498c', 'licence': 'apache-2.0', 'adapter_dir': '/Volumes/energy_commerce_retail_media/energy_ml_models/foundation_models/adapters/honda_forecast.increment/model-20261024T112829Z', 'mode': 'lora', 'context': 672, 'gap_days': 1, 'seed': 42}
- library versions: torch 2.9.0+cu129, chronos 2.3.2
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | chronos2_lora held-out | chronos2_lora validation | persistence held-out |
|---|---|---|---|
| mae | 18.13 | 16.15 | 23.28 |
| mean_error | -0.9812 | -1.637 | 0.02136 |
| mean_error_relative | -0.02585 | -0.04175 | 0.0005626 |
| primary_change_relative | 0.1227 |  | 0.1468 |
| rmse | 37.48 | 35.44 | 49.99 |
| skill_mae | 0.2214 | 0.2047 |  |
| skill_primary | 0.2214 |  |  |
| skill_primary_ci_high | 0.2784 |  |  |
| skill_primary_ci_low | 0.1768 |  |  |
| skill_rmse | 0.2503 | 0.2299 |  |

## Segments

| segment | skill | decision | detail |
|---|---|---|---|
| combined_heat_and_power | 0.1858 | approved_with_conditions | no segment interval |
| solar_photovoltaic | 0.1163 | approved_with_conditions | no segment interval |
| total | 0.2711 | approved_with_conditions | no segment interval |

## Flags

- none

## Conditions and known limits

- contamination flag unresolved: the pretraining data of the model was not shown to be free of the evaluated data; revision 29ec3766d36d; pretraining data per register: named on the model card; read before the contamination check; task data names found: none
- segments with positive skill but no uncertainty interval, conditional until an interval is recorded: combined_heat_and_power (0.186), solar_photovoltaic (0.116), total (0.271)

## Disclosures

- the held-out partition was read in at least 4 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged
- foundation model amazon/chronos-2 at revision 29ec3766d36d, licence apache-2.0, ran lora

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `persistence`: validation 20.3, held-out 23.28 on `mae`
