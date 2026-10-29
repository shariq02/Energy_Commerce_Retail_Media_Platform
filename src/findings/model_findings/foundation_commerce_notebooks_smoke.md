# FOUNDATION COMMERCE NOTEBOOK FINDINGS (SMOKE RUN)

_Merged by the findings gate from the files of the model notebooks; a section is replaced when its notebook runs again. Generated: 2026-10-25T13:29Z_

<!-- section: foundation_commerce__03_foundation_tab_commerce -->
## Foundation Tabular Commerce

### summary

| task | foundation candidates | ok | failed or skipped |
|---|---|---|---|
| session_purchase_ga4.purchase | 3 | 3 | 0 |
| session_purchase_rees46.purchase | 3 | 3 | 0 |
| lapse_ga4.return | 3 | 3 | 0 |
| lapse_rees46.return | 3 | 3 | 0 |

### models

| model | repository | revision | licence | restriction |
|---|---|---|---|---|
| chronos2 | amazon/chronos-2 | 29ec3766d36d | apache-2.0 | none |
| energy_tspulse | EnergyFM/energy-tspulse | 86442e1851fb | apache-2.0 | none |
| energy_ttm | EnergyFM/energy-ttm | 63acc54104d4 | apache-2.0 | none |
| moment | AutonLab/MOMENT-1-large | ca58581bc7be | mit | read the model card |
| survpfn_code | genepi-freiburg/SurvPFN | 4f26fe31efd8 | mit | none |
| survpfn_weights | samuelboehm/SurvPFN | 6225cc4c8ccb | mit | read the model card |
| tabicl_v2 | jingang/TabICL | 4dcd344ece2c | bsd-3-clause | none |
| tabpfn_25 | Prior-Labs/tabpfn_2_5 | 6c45f3a6d0d0 | tabpfn-2.5-non-commercial | non-commercial use only; gated (accept the terms, set HF_TOKEN) |
| tabpfn_v2_clf | Prior-Labs/TabPFN-v2-clf | f851f2a3c941 | priorlabs-1-1 | Apache 2.0 with an attribution requirement |
| tabpfn_v2_reg | Prior-Labs/TabPFN-v2-reg | 4972a65a1b30 | priorlabs-1-1 | Apache 2.0 with an attribution requirement |
| timesfm25 | google/timesfm-2.5-200m-pytorch | 1d952420fba8 | apache-2.0 | none |
| toto2 | Datadog/Toto-2.0-1B | 1604e1a52428 | apache-2.0 | none |
| ttm_r2 | ibm-granite/granite-timeseries-ttm-r2 | bcfc4a497430 | apache-2.0 | none |

### flags

_no automatic flags raised_

### session_purchase_ga4.purchase

_dataset `session_purchase_ga4`, frozen version 0, primary metric `pr_auc` (higher is better)_

#### data seen by the models

- frame 1: 6609 rows x 21 columns, sample fraction 1.0, rows by partition {'train': 4000, 'validation': 2609}, highest null rates {'fold_id': 0.3948}
- target, train: rows 4000, positive_rate 0.2435
- target, validation: rows 2609, positive_rate 0.2376

#### candidates

| model | family | stage | status | artifact | rows | pr_auc | detail |
|---|---|---|---|---|---|---|---|
| base_rate | baseline | baseline | ok | ok | 2609 | 0.2376 |  |
| tabicl_v2_in_context | foundation_tab | candidate | ok | ok | 2609 | 0.3868 |  |
| tabpfn_25_in_context | foundation_tab | candidate | ok | ok | 2609 | 0.3865 |  |
| tabpfn_v2_in_context | foundation_tab | candidate | ok | ok | 2609 | 0.3817 |  |

#### metrics (validation partition)

| metric | base_rate | tabicl_v2_in_context | tabpfn_25_in_context | tabpfn_v2_in_context |
|---|---|---|---|---|
| pr_auc | 0.2376 | 0.3868 | 0.3865 | 0.3817 |
| log_loss | 0.5484 | 0.504 | 0.5027 | 0.5064 |
| calibration_error | 0.005861 | 0.02616 | 0.0168 | 0.01626 |
| base_rate | 0.2376 | 0.2376 | 0.2376 | 0.2376 |
| pred_mean | 0.2435 | 0.2285 | 0.2273 | 0.2323 |
| pred_positive_share | 0 | 0.03641 | 0.03488 | 0.03795 |
| pr_auc_lift | 1 | 1.628 | 1.626 | 1.606 |

### session_purchase_rees46.purchase

_dataset `session_purchase_rees46`, frozen version 1, primary metric `pr_auc` (higher is better)_

#### data seen by the models

- frame 1: 8000 rows x 30 columns, sample fraction 1.0, rows by partition {'train': 4000, 'validation': 4000}, highest null rates {'fold_id': 0.5}
- target, train: rows 4000, positive_rate 0.0815
- target, validation: rows 4000, positive_rate 0.0772

#### candidates

| model | family | stage | status | artifact | rows | pr_auc | detail |
|---|---|---|---|---|---|---|---|
| base_rate | baseline | baseline | ok | ok | 4000 | 0.07725 |  |
| tabicl_v2_in_context | foundation_tab | candidate | ok | ok | 4000 | 0.439 |  |
| tabpfn_25_in_context | foundation_tab | candidate | ok | ok | 4000 | 0.4405 |  |
| tabpfn_v2_in_context | foundation_tab | candidate | ok | ok | 4000 | 0.4423 |  |

#### metrics (validation partition)

| metric | base_rate | tabicl_v2_in_context | tabpfn_25_in_context | tabpfn_v2_in_context |
|---|---|---|---|---|
| pr_auc | 0.07725 | 0.439 | 0.4405 | 0.4423 |
| log_loss | 0.2721 | 0.2061 | 0.2063 | 0.2083 |
| calibration_error | 0.00425 | 0.01612 | 0.01928 | 0.01947 |
| base_rate | 0.07725 | 0.07725 | 0.07725 | 0.07725 |
| pred_mean | 0.0815 | 0.09224 | 0.09598 | 0.09363 |
| pred_positive_share | 0 | 0.05025 | 0.0525 | 0.04 |
| pr_auc_lift | 1 | 5.683 | 5.702 | 5.725 |

### lapse_ga4.return

_dataset `lapse_ga4`, frozen version 0, primary metric `pr_auc` (higher is better)_

#### data seen by the models

- frame 1: 8000 rows x 10 columns, sample fraction 1.0, rows by partition {'train': 4000, 'validation': 4000}, highest null rates {'fold_id': 0.5}
- target, train: rows 4000, positive_rate 0.1013
- target, validation: rows 4000, positive_rate 0.0742

#### candidates

| model | family | stage | status | artifact | rows | pr_auc | detail |
|---|---|---|---|---|---|---|---|
| base_rate | baseline | baseline | ok | ok | 4000 | 0.07425 |  |
| tabicl_v2_in_context | foundation_tab | candidate | ok | ok | 4000 | 0.2027 |  |
| tabpfn_25_in_context | foundation_tab | candidate | ok | ok | 4000 | 0.1944 |  |
| tabpfn_v2_in_context | foundation_tab | candidate | ok | ok | 4000 | 0.219 |  |

#### metrics (validation partition)

| metric | base_rate | tabicl_v2_in_context | tabpfn_25_in_context | tabpfn_v2_in_context |
|---|---|---|---|---|
| pr_auc | 0.07425 | 0.2027 | 0.1944 | 0.219 |
| log_loss | 0.2689 | 0.2585 | 0.2604 | 0.2391 |
| calibration_error | 0.027 | 0.04344 | 0.03729 | 0.006702 |
| base_rate | 0.07425 | 0.07425 | 0.07425 | 0.07425 |
| pred_mean | 0.1013 | 0.115 | 0.1086 | 0.07889 |
| pred_positive_share | 0 | 0.00425 | 0.00425 | 0.00475 |
| pr_auc_lift | 1 | 2.73 | 2.619 | 2.95 |

### lapse_rees46.return

_dataset `lapse_rees46`, frozen version 0, primary metric `pr_auc` (higher is better)_

#### data seen by the models

- frame 1: 8000 rows x 12 columns, sample fraction 1.0, rows by partition {'train': 4000, 'validation': 4000}, highest null rates {'fold_id': 0.5}
- target, train: rows 4000, positive_rate 0.4735
- target, validation: rows 4000, positive_rate 0.4657

#### candidates

| model | family | stage | status | artifact | rows | pr_auc | detail |
|---|---|---|---|---|---|---|---|
| base_rate | baseline | baseline | ok | ok | 4000 | 0.4657 |  |
| tabicl_v2_in_context | foundation_tab | candidate | ok | ok | 4000 | 0.7311 |  |
| tabpfn_25_in_context | foundation_tab | candidate | ok | ok | 4000 | 0.7305 |  |
| tabpfn_v2_in_context | foundation_tab | candidate | ok | ok | 4000 | 0.7291 |  |

#### metrics (validation partition)

| metric | base_rate | tabicl_v2_in_context | tabpfn_25_in_context | tabpfn_v2_in_context |
|---|---|---|---|---|
| pr_auc | 0.4657 | 0.7311 | 0.7305 | 0.7291 |
| log_loss | 0.6909 | 0.5947 | 0.5947 | 0.5949 |
| calibration_error | 0.00775 | 0.02689 | 0.02368 | 0.01387 |
| base_rate | 0.4657 | 0.4657 | 0.4657 | 0.4657 |
| pred_mean | 0.4735 | 0.4639 | 0.4669 | 0.4695 |
| pred_positive_share | 0 | 0.3905 | 0.3925 | 0.3882 |
| pr_auc_lift | 1 | 1.57 | 1.568 | 1.566 |

<!-- section: foundation_commerce__07_foundation_sequence_commerce -->
## Foundation Sequence Commerce

### summary

| task | foundation candidates | ok | failed or skipped |
|---|---|---|---|
| next_item_ga4.next_item | 1 | 1 | 0 |
| next_item_rees46.next_item | 1 | 1 | 0 |
| rl_session_sequences.policy | 1 | 1 | 0 |

### models

| model | repository | revision | licence | restriction |
|---|---|---|---|---|
| chronos2 | amazon/chronos-2 | 29ec3766d36d | apache-2.0 | none |
| energy_tspulse | EnergyFM/energy-tspulse | 86442e1851fb | apache-2.0 | none |
| energy_ttm | EnergyFM/energy-ttm | 63acc54104d4 | apache-2.0 | none |
| moment | AutonLab/MOMENT-1-large | ca58581bc7be | mit | read the model card |
| survpfn_code | genepi-freiburg/SurvPFN | 4f26fe31efd8 | mit | none |
| survpfn_weights | samuelboehm/SurvPFN | 6225cc4c8ccb | mit | read the model card |
| tabicl_v2 | jingang/TabICL | 4dcd344ece2c | bsd-3-clause | none |
| tabpfn_25 | Prior-Labs/tabpfn_2_5 | 6c45f3a6d0d0 | tabpfn-2.5-non-commercial | non-commercial use only; gated (accept the terms, set HF_TOKEN) |
| tabpfn_v2_clf | Prior-Labs/TabPFN-v2-clf | f851f2a3c941 | priorlabs-1-1 | Apache 2.0 with an attribution requirement |
| tabpfn_v2_reg | Prior-Labs/TabPFN-v2-reg | 4972a65a1b30 | priorlabs-1-1 | Apache 2.0 with an attribution requirement |
| timesfm25 | google/timesfm-2.5-200m-pytorch | 1d952420fba8 | apache-2.0 | none |
| toto2 | Datadog/Toto-2.0-1B | 1604e1a52428 | apache-2.0 | none |
| ttm_r2 | ibm-granite/granite-timeseries-ttm-r2 | bcfc4a497430 | apache-2.0 | none |

### flags

| task | model | flag | detail |
|---|---|---|---|
| next_item_ga4.next_item | popularity | no candidate beats the baseline | ndcg_at_10 |
| next_item_rees46.next_item | popularity | no candidate beats the baseline | ndcg_at_10 |
| rl_session_sequences.policy | most_frequent_event | no candidate beats the baseline | action_agreement |

### next_item_ga4.next_item

_dataset `next_item_ga4`, frozen version 8, primary metric `ndcg_at_10` (higher is better)_

#### data seen by the models

- frame 1: 8000 rows x 5 columns, sample fraction 1.0, rows by partition {'train': 4000, 'validation': 4000}, highest null rates none

#### candidates

| model | family | stage | status | artifact | rows | ndcg_at_10 | detail |
|---|---|---|---|---|---|---|---|
| popularity | baseline | baseline | ok | ok | 4000 | 0.07476 |  |
| sasrec_sequence | sequence | candidate | ok | ok | 4000 | 0.005243 |  |

#### metrics (validation partition)

| metric | popularity | sasrec_sequence |
|---|---|---|
| recall_at_5 | 0.08625 | 0.0055 |
| ndcg_at_5 | 0.05387 | 0.002681 |
| recall_at_10 | 0.1515 | 0.0135 |
| ndcg_at_10 | 0.07476 | 0.005243 |
| recall_at_20 | 0.2682 | 0.03325 |
| ndcg_at_20 | 0.1039 | 0.01016 |
| mrr | 0.07108 | 0.01076 |
| train_recall_at_10 | 0.1655 | 0.016 |
| train_ndcg_at_10 | 0.08284 | 0.006739 |
| train_mrr | 0.07729 | 0.01222 |

### next_item_rees46.next_item

_dataset `next_item_rees46`, frozen version 14, primary metric `ndcg_at_10` (higher is better)_

#### data seen by the models

- frame 1: 790 rows x 5 columns, sample fraction 1.0, rows by partition {'train': 398, 'validation': 392}, highest null rates none

#### candidates

| model | family | stage | status | artifact | rows | ndcg_at_10 | detail |
|---|---|---|---|---|---|---|---|
| popularity | baseline | baseline | ok | ok | 392 | 0.005883 |  |
| sasrec_sequence | sequence | candidate | ok | ok | 392 | 0.0007679 |  |

#### metrics (validation partition)

| metric | popularity | sasrec_sequence |
|---|---|---|
| recall_at_5 | 0.007653 | 0 |
| ndcg_at_5 | 0.004206 | 0 |
| recall_at_10 | 0.01276 | 0.002551 |
| ndcg_at_10 | 0.005883 | 0.0007679 |
| recall_at_20 | 0.02551 | 0.005102 |
| ndcg_at_20 | 0.008861 | 0.001457 |
| mrr | 0.005202 | 0.001399 |
| train_recall_at_10 | 0.07286 | 0.01005 |
| train_ndcg_at_10 | 0.03775 | 0.004258 |
| train_mrr | 0.038 | 0.01096 |

### rl_session_sequences.policy

_dataset `rl_session_sequences`, frozen version 9, primary metric `action_agreement` (higher is better)_

#### data seen by the models

- frame 1: 744 rows x 10 columns, sample fraction 1.0, rows by partition {'train': 395, 'validation': 349}, highest null rates {'seq_timestamp_tie': 0.1882}

#### candidates

| model | family | stage | status | artifact | rows | action_agreement | detail |
|---|---|---|---|---|---|---|---|
| most_frequent_event | baseline | baseline | ok | ok | 349 | 0.9484 |  |
| decision_transformer | offline_rl | candidate | ok | ok | 349 | 0.937 |  |

#### metrics (validation partition)

| metric | most_frequent_event | decision_transformer |
|---|---|---|
| action_agreement | 0.9484 | 0.937 |
| reward_when_agree | 0 | 0 |
| reward_when_disagree | 65.14 | 53.3 |
| reward_weighted_agreement | 0 | 0 |
| train_action_agreement | 0.9747 | 0.9747 |
