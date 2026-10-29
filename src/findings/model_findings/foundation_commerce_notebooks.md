# FOUNDATION COMMERCE NOTEBOOK FINDINGS

_Merged by the findings gate from the files of the model notebooks; a section is replaced when its notebook runs again. Generated: 2026-10-25T13:29Z_

<!-- section: foundation_commerce__03_foundation_tab_commerce -->
## Foundation Tabular Commerce

### summary

| task | foundation candidates | ok | failed or skipped |
|---|---|---|---|
| session_purchase_ga4.purchase | 3 | 3 | 0 |
| session_purchase_rees46.purchase | 3 | 1 | 2 |
| lapse_ga4.return | 3 | 3 | 0 |
| lapse_rees46.return | 3 | 2 | 1 |

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
| session_purchase_rees46.purchase | tabicl_v2_in_context | failed | TimeoutError: tabicl_v2_in_context session_purchase_rees46.purchase: timeout after 1800 s |
| session_purchase_rees46.purchase | tabpfn_25_in_context | failed | TimeoutError: tabpfn_25_in_context session_purchase_rees46.purchase: timeout after 1800 s |
| lapse_ga4.return | tabpfn_v2_in_context | a single class is predicted | share of positive predictions = 0 |
| lapse_rees46.return | tabicl_v2_in_context | failed | TimeoutError: tabicl_v2_in_context lapse_rees46.return: timeout after 1800 s |

### session_purchase_ga4.purchase

_dataset `session_purchase_ga4`, frozen version 0, primary metric `pr_auc` (higher is better)_

#### data seen by the models

- frame 1: 14494 rows x 21 columns, sample fraction 1.0, rows by partition {'train': 11885, 'validation': 2609}, highest null rates {'fold_id': 0.18}
- target, train: rows 1.188e+04, positive_rate 0.2439
- target, validation: rows 2609, positive_rate 0.2376

#### candidates

| model | family | stage | status | artifact | rows | pr_auc | detail |
|---|---|---|---|---|---|---|---|
| base_rate | baseline | baseline | ok | ok | 2609 | 0.2376 |  |
| tabicl_v2_in_context | foundation_tab | candidate | ok | ok | 2609 | 0.4042 |  |
| tabpfn_25_in_context | foundation_tab | candidate | ok | ok | 2609 | 0.4065 |  |
| tabpfn_v2_in_context | foundation_tab | candidate | ok | ok | 2609 | 0.384 |  |

#### metrics (validation partition)

| metric | base_rate | tabicl_v2_in_context | tabpfn_25_in_context | tabpfn_v2_in_context |
|---|---|---|---|---|
| pr_auc | 0.2376 | 0.4042 | 0.4065 | 0.384 |
| log_loss | 0.5485 | 0.4972 | 0.4965 | 0.5046 |
| calibration_error | 0.006282 | 0.01893 | 0.01867 | 0.02011 |
| base_rate | 0.2376 | 0.2376 | 0.2376 | 0.2376 |
| pred_mean | 0.2439 | 0.2361 | 0.2328 | 0.2391 |
| pred_positive_share | 0 | 0.06209 | 0.05558 | 0.05749 |
| pr_auc_lift | 1 | 1.701 | 1.711 | 1.616 |

### session_purchase_rees46.purchase

_dataset `session_purchase_rees46`, frozen version 1, primary metric `pr_auc` (higher is better)_

#### data seen by the models

- frame 1: 5386716 rows x 30 columns, sample fraction 1.0, rows by partition {'train': 4077311, 'validation': 1309405}, highest null rates {'fold_id': 0.2431}
- target, train: rows 4.077e+06, positive_rate 0.0832
- target, validation: rows 1.309e+06, positive_rate 0.0813

#### candidates

| model | family | stage | status | artifact | rows | pr_auc | detail |
|---|---|---|---|---|---|---|---|
| base_rate | baseline | baseline | ok | ok | 785035 | 0.08131 |  |
| tabicl_v2_in_context | foundation_tab | candidate | failed | none |  |  | TimeoutError: tabicl_v2_in_context session_purchase_rees46.purchase: timeout after 1800 s |
| tabpfn_25_in_context | foundation_tab | candidate | failed | none |  |  | TimeoutError: tabpfn_25_in_context session_purchase_rees46.purchase: timeout after 1800 s |
| tabpfn_v2_in_context | foundation_tab | candidate | ok | ok | 1309405 | 0.4245 |  |

#### metrics (validation partition)

| metric | base_rate | tabpfn_v2_in_context |
|---|---|---|
| pr_auc | 0.08131 | 0.4245 |
| log_loss | 0.282 | 0.2187 |
| calibration_error | 0.001849 | 0.01714 |
| base_rate | 0.08131 | 0.08128 |
| pred_mean | 0.08316 | 0.07718 |
| pred_positive_share | 0 | 0.04201 |
| pr_auc_lift | 1 | 5.223 |

### lapse_ga4.return

_dataset `lapse_ga4`, frozen version 0, primary metric `pr_auc` (higher is better)_

#### data seen by the models

- frame 1: 26721 rows x 10 columns, sample fraction 1.0, rows by partition {'train': 20197, 'validation': 6524}, highest null rates {'fold_id': 0.2442}
- target, train: rows 2.02e+04, positive_rate 0.0989
- target, validation: rows 6524, positive_rate 0.0786

#### candidates

| model | family | stage | status | artifact | rows | pr_auc | detail |
|---|---|---|---|---|---|---|---|
| base_rate | baseline | baseline | ok | ok | 6524 | 0.07863 |  |
| tabicl_v2_in_context | foundation_tab | candidate | ok | ok | 6524 | 0.2136 |  |
| tabpfn_25_in_context | foundation_tab | candidate | ok | ok | 6524 | 0.1886 |  |
| tabpfn_v2_in_context | foundation_tab | candidate | ok | ok | 6524 | 0.2205 |  |

#### metrics (validation partition)

| metric | base_rate | tabicl_v2_in_context | tabpfn_25_in_context | tabpfn_v2_in_context |
|---|---|---|---|---|
| pr_auc | 0.07863 | 0.2136 | 0.1886 | 0.2205 |
| log_loss | 0.2779 | 0.2591 | 0.2767 | 0.2508 |
| calibration_error | 0.02024 | 0.009877 | 0.04357 | 0.01076 |
| base_rate | 0.07863 | 0.07863 | 0.07863 | 0.07863 |
| pred_mean | 0.09888 | 0.08299 | 0.1163 | 0.06803 |
| pred_positive_share | 0 | 0.001993 | 0.001533 | 0 |
| pr_auc_lift | 1 | 2.716 | 2.398 | 2.804 |

### lapse_rees46.return

_dataset `lapse_rees46`, frozen version 0, primary metric `pr_auc` (higher is better)_

#### data seen by the models

- frame 1: 2568696 rows x 12 columns, sample fraction 1.0, rows by partition {'train': 2114867, 'validation': 453829}, highest null rates {'fold_id': 0.1767}
- target, train: rows 2.115e+06, positive_rate 0.4631
- target, validation: rows 4.538e+05, positive_rate 0.4634

#### candidates

| model | family | stage | status | artifact | rows | pr_auc | detail |
|---|---|---|---|---|---|---|---|
| base_rate | baseline | baseline | ok | ok | 453829 | 0.4634 |  |
| tabicl_v2_in_context | foundation_tab | candidate | failed | none |  |  | TimeoutError: tabicl_v2_in_context lapse_rees46.return: timeout after 1800 s |
| tabpfn_25_in_context | foundation_tab | candidate | ok | ok | 453829 | 0.7283 |  |
| tabpfn_v2_in_context | foundation_tab | candidate | ok | ok | 453829 | 0.7246 |  |

#### metrics (validation partition)

| metric | base_rate | tabpfn_25_in_context | tabpfn_v2_in_context |
|---|---|---|---|
| pr_auc | 0.4634 | 0.7283 | 0.7246 |
| log_loss | 0.6905 | 0.5947 | 0.5966 |
| calibration_error | 0.0002417 | 0.01843 | 0.008723 |
| base_rate | 0.4634 | 0.4634 | 0.4634 |
| pred_mean | 0.4631 | 0.4659 | 0.4608 |
| pred_positive_share | 0 | 0.3833 | 0.3657 |
| pr_auc_lift | 1 | 1.572 | 1.564 |

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
| rl_session_sequences.policy | most_frequent_event | no candidate beats the baseline | action_agreement |

### next_item_ga4.next_item

_dataset `next_item_ga4`, frozen version 8, primary metric `ndcg_at_10` (higher is better)_

#### data seen by the models

- frame 1: 3191185 rows x 5 columns, sample fraction 1.0, rows by partition {'train': 2603292, 'validation': 587893}, highest null rates none

#### candidates

| model | family | stage | status | artifact | rows | ndcg_at_10 | detail |
|---|---|---|---|---|---|---|---|
| popularity | baseline | baseline | ok | ok | 297217 | 0.07577 |  |
| sasrec_sequence | sequence | candidate | ok | ok | 297217 | 0.2381 |  |

#### metrics (validation partition)

| metric | popularity | sasrec_sequence |
|---|---|---|
| recall_at_5 | 0.08949 | 0.278 |
| ndcg_at_5 | 0.05476 | 0.1444 |
| recall_at_10 | 0.1553 | 0.571 |
| ndcg_at_10 | 0.07577 | 0.2381 |
| recall_at_20 | 0.2679 | 0.7617 |
| ndcg_at_20 | 0.1039 | 0.2872 |
| mrr | 0.07111 | 0.1578 |
| train_recall_at_10 | 0.1466 | 0.5824 |
| train_ndcg_at_10 | 0.07121 | 0.2425 |
| train_mrr | 0.06755 | 0.1598 |

### next_item_rees46.next_item

_dataset `next_item_rees46`, frozen version 14, primary metric `ndcg_at_10` (higher is better)_

#### data seen by the models

- frame 1: 2673007 rows x 5 columns, sample fraction 1.0, rows by partition {'train': 1984544, 'validation': 688463}, highest null rates none

#### candidates

| model | family | stage | status | artifact | rows | ndcg_at_10 | detail |
|---|---|---|---|---|---|---|---|
| popularity | baseline | baseline | ok | ok | 302614 | 0.01624 |  |
| sasrec_sequence | sequence | candidate | ok | ok | 302614 | 0.0167 |  |

#### metrics (validation partition)

| metric | popularity | sasrec_sequence |
|---|---|---|
| recall_at_5 | 0.01912 | 0.02243 |
| ndcg_at_5 | 0.01182 | 0.012 |
| recall_at_10 | 0.03291 | 0.03704 |
| ndcg_at_10 | 0.01624 | 0.0167 |
| recall_at_20 | 0.05209 | 0.05629 |
| ndcg_at_20 | 0.02106 | 0.02153 |
| mrr | 0.01491 | 0.01423 |
| train_recall_at_10 | 0.03574 | 0.04255 |
| train_ndcg_at_10 | 0.01763 | 0.01926 |
| train_mrr | 0.01662 | 0.0166 |

### rl_session_sequences.policy

_dataset `rl_session_sequences`, frozen version 9, primary metric `action_agreement` (higher is better)_

#### data seen by the models

- frame 1: 3980795 rows x 10 columns, sample fraction 1.0, rows by partition {'train': 2966334, 'validation': 1014461}, highest null rates {'seq_timestamp_tie': 0.2141}

#### candidates

| model | family | stage | status | artifact | rows | action_agreement | detail |
|---|---|---|---|---|---|---|---|
| most_frequent_event | baseline | baseline | ok | ok | 1014461 | 0.9442 |  |
| decision_transformer | offline_rl | candidate | ok | ok | 1014461 | 0.9442 |  |

#### metrics (validation partition)

| metric | most_frequent_event | decision_transformer |
|---|---|---|
| action_agreement | 0.9442 | 0.9442 |
| reward_when_agree | 0 | 0 |
| reward_when_disagree | 89.45 | 89.45 |
| reward_weighted_agreement | 0 | 0 |
| train_action_agreement | 0.9611 | 0.9611 |
