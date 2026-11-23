# GENOMICS NOTEBOOK FINDINGS

_Merged by the findings gate from the files of the model notebooks; a section is replaced when its notebook runs again. Generated: 2026-11-23T14:42Z_

<!-- section: genomics__approve__01_recommend -->
## Approval Recommendation

### model_approval

39 row(s), first 30 shown

| task_id | model_name | role | decision | restriction | overridden | primary_metric | validation_value | evaluated_value |
|---|---|---|---|---|---|---|---|---|
| variant_pathogenicity.pathogenic | gbt_lightgbm | selected | approved |  | False | pr_auc | 1.0 | 0.9991 |
| variant_pathogenicity.pathogenic | gbt_sklearn | runner_up | not_approved |  | False | pr_auc | 1.0 | 0.9988 |
| variant_pathogenicity.pathogenic | base_rate | baseline_fallback | approved |  | False | pr_auc | 0.2085 | 0.2315 |
| variant_high_impact.high_impact | gbt_lightgbm | selected | approved |  | False | pr_auc | 1.0 | 1.0 |
| variant_high_impact.high_impact | gbt_sklearn | runner_up | not_approved |  | False | pr_auc | 1.0 | 1.0 |
| variant_high_impact.high_impact | base_rate | baseline_fallback | approved |  | False | pr_auc | 0.3755 | 0.3627 |
| variant_carrier_screening.carrier | gbt_lightgbm | selected | approved |  | False | pr_auc | 1.0 | 1.0 |
| variant_carrier_screening.carrier | gbt_sklearn | runner_up | not_approved |  | False | pr_auc | 1.0 | 1.0 |
| variant_carrier_screening.carrier | base_rate | baseline_fallback | approved |  | False | pr_auc | 0.0906 | 0.0887 |
| variant_actionable_rare.actionable | gbt_lightgbm | selected | approved |  | False | pr_auc | 1.0 | 1.0 |
| variant_actionable_rare.actionable | gbt_sklearn | runner_up | not_approved |  | False | pr_auc | 1.0 | 1.0 |
| variant_actionable_rare.actionable | base_rate | baseline_fallback | approved |  | False | pr_auc | 0.0139 | 0.0149 |
| structural_variant_disruption.disruption | gbt_lightgbm | selected | approved |  | False | pr_auc | 1.0 | 1.0 |
| structural_variant_disruption.disruption | gbt_sklearn | runner_up | not_approved |  | False | pr_auc | 1.0 | 1.0 |
| structural_variant_disruption.disruption | base_rate | baseline_fallback | approved |  | False | pr_auc | 0.8285 | 0.8287 |
| gene_pharmacogene.pharmacogene | logistic | selected | approved |  | False | pr_auc | 1.0 | 1.0 |
| gene_pharmacogene.pharmacogene | gbt_lightgbm | runner_up | not_approved |  | False | pr_auc | 1.0 | 1.0 |
| gene_pharmacogene.pharmacogene | base_rate | baseline_fallback | approved |  | False | pr_auc | 0.1086 | 0.1096 |
| gene_cancer_role.cancer_role | gbt_lightgbm | selected | approved |  | False | balanced_accuracy | 0.8406 | 0.8668 |
| gene_cancer_role.cancer_role | gbt_sklearn | runner_up | not_approved |  | False | balanced_accuracy | 0.8406 | 0.8668 |
| gene_cancer_role.cancer_role | majority_class | baseline_fallback | approved |  | False | balanced_accuracy | 0.3333 | 0.3333 |
| benchmark_clinvar_vs_benign.label | logistic | selected | approved |  | False | pr_auc | 0.9182 | 0.9085 |
| benchmark_clinvar_vs_benign.label | gbt_lightgbm | runner_up | not_approved |  | False | pr_auc | 0.9152 | 0.91 |
| benchmark_clinvar_vs_benign.label | base_rate | baseline_fallback | approved |  | False | pr_auc | 0.4667 | 0.4339 |
| benchmark_gnomad_balanced.label | gbt_sklearn | selected | approved |  | False | pr_auc | 0.5623 | 0.5585 |
| benchmark_gnomad_balanced.label | gbt_lightgbm | runner_up | not_approved |  | False | pr_auc | 0.5618 | 0.558 |
| benchmark_gnomad_balanced.label | base_rate | baseline_fallback | approved |  | False | pr_auc | 0.5 | 0.4992 |
| benchmark_proteingym_clinical.label | gbt_lightgbm | selected | approved |  | False | pr_auc | 0.7355 | 0.7464 |
| benchmark_proteingym_clinical.label | gbt_sklearn | runner_up | not_approved |  | False | pr_auc | 0.7344 | 0.7461 |
| benchmark_proteingym_clinical.label | base_rate | baseline_fallback | approved |  | False | pr_auc | 0.4891 | 0.5181 |

<!-- section: genomics__approve__02_record_decisions -->
## Approval Decisions

### model_approval

39 row(s), first 30 shown

| task_id | model_name | role | decision | restriction | overridden | primary_metric | validation_value | evaluated_value |
|---|---|---|---|---|---|---|---|---|
| variant_pathogenicity.pathogenic | gbt_lightgbm | selected | approved |  | False | pr_auc | 1.0 | 0.9991 |
| variant_pathogenicity.pathogenic | gbt_sklearn | runner_up | not_approved |  | False | pr_auc | 1.0 | 0.9988 |
| variant_pathogenicity.pathogenic | base_rate | baseline_fallback | approved |  | False | pr_auc | 0.2085 | 0.2315 |
| variant_high_impact.high_impact | gbt_lightgbm | selected | approved |  | False | pr_auc | 1.0 | 1.0 |
| variant_high_impact.high_impact | gbt_sklearn | runner_up | not_approved |  | False | pr_auc | 1.0 | 1.0 |
| variant_high_impact.high_impact | base_rate | baseline_fallback | approved |  | False | pr_auc | 0.3755 | 0.3627 |
| variant_carrier_screening.carrier | gbt_lightgbm | selected | approved |  | False | pr_auc | 1.0 | 1.0 |
| variant_carrier_screening.carrier | gbt_sklearn | runner_up | not_approved |  | False | pr_auc | 1.0 | 1.0 |
| variant_carrier_screening.carrier | base_rate | baseline_fallback | approved |  | False | pr_auc | 0.0906 | 0.0887 |
| variant_actionable_rare.actionable | gbt_lightgbm | selected | approved |  | False | pr_auc | 1.0 | 1.0 |
| variant_actionable_rare.actionable | gbt_sklearn | runner_up | not_approved |  | False | pr_auc | 1.0 | 1.0 |
| variant_actionable_rare.actionable | base_rate | baseline_fallback | approved |  | False | pr_auc | 0.0139 | 0.0149 |
| structural_variant_disruption.disruption | gbt_lightgbm | selected | approved |  | False | pr_auc | 1.0 | 1.0 |
| structural_variant_disruption.disruption | gbt_sklearn | runner_up | not_approved |  | False | pr_auc | 1.0 | 1.0 |
| structural_variant_disruption.disruption | base_rate | baseline_fallback | approved |  | False | pr_auc | 0.8285 | 0.8287 |
| gene_pharmacogene.pharmacogene | logistic | selected | approved |  | False | pr_auc | 1.0 | 1.0 |
| gene_pharmacogene.pharmacogene | gbt_lightgbm | runner_up | not_approved |  | False | pr_auc | 1.0 | 1.0 |
| gene_pharmacogene.pharmacogene | base_rate | baseline_fallback | approved |  | False | pr_auc | 0.1086 | 0.1096 |
| gene_cancer_role.cancer_role | gbt_lightgbm | selected | approved |  | False | balanced_accuracy | 0.8406 | 0.8668 |
| gene_cancer_role.cancer_role | gbt_sklearn | runner_up | not_approved |  | False | balanced_accuracy | 0.8406 | 0.8668 |
| gene_cancer_role.cancer_role | majority_class | baseline_fallback | approved |  | False | balanced_accuracy | 0.3333 | 0.3333 |
| benchmark_clinvar_vs_benign.label | logistic | selected | approved |  | False | pr_auc | 0.9182 | 0.9085 |
| benchmark_clinvar_vs_benign.label | gbt_lightgbm | runner_up | not_approved |  | False | pr_auc | 0.9152 | 0.91 |
| benchmark_clinvar_vs_benign.label | base_rate | baseline_fallback | approved |  | False | pr_auc | 0.4667 | 0.4339 |
| benchmark_gnomad_balanced.label | gbt_sklearn | selected | approved |  | False | pr_auc | 0.5623 | 0.5585 |
| benchmark_gnomad_balanced.label | gbt_lightgbm | runner_up | not_approved |  | False | pr_auc | 0.5618 | 0.558 |
| benchmark_gnomad_balanced.label | base_rate | baseline_fallback | approved |  | False | pr_auc | 0.5 | 0.4992 |
| benchmark_proteingym_clinical.label | gbt_lightgbm | selected | approved |  | False | pr_auc | 0.7355 | 0.7464 |
| benchmark_proteingym_clinical.label | gbt_sklearn | runner_up | not_approved |  | False | pr_auc | 0.7344 | 0.7461 |
| benchmark_proteingym_clinical.label | base_rate | baseline_fallback | approved |  | False | pr_auc | 0.4891 | 0.5181 |

<!-- section: genomics__approve__03_model_cards -->
## Model Cards

13 card(s) written to src/model_cards/genomics

<!-- section: genomics__evaluate__01_evaluate -->
## Model Evaluation

### evaluation_results

726 row(s), first 30 shown

| task_id | model_name | stage | partition | metric | value | validation_value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| variant_pathogenicity.pathogenic | base_rate | baseline | test | pr_auc | 0.2315 | 0.2085 | 314548 | ok | on test |
| variant_pathogenicity.pathogenic | base_rate | baseline | test | log_loss | 0.5435 | 0.5121 | 314548 | ok | on test |
| variant_pathogenicity.pathogenic | base_rate | baseline | test | calibration_error | 0.0284 | 0.0054 | 314548 | ok | on test |
| variant_pathogenicity.pathogenic | base_rate | baseline | test | base_rate | 0.2315 | 0.2085 | 314548 | ok | on test |
| variant_pathogenicity.pathogenic | base_rate | baseline | test | pred_mean | 0.2031 | 0.2031 | 314548 | ok | on test |
| variant_pathogenicity.pathogenic | base_rate | baseline | test | pred_positive_share | 0.0 | 0.0 | 314548 | ok | on test |
| variant_pathogenicity.pathogenic | base_rate | baseline | test | pr_auc_lift | 1.0 | 1.0 | 314548 | ok | on test |
| variant_pathogenicity.pathogenic | base_rate | baseline | test | primary_change_relative | -0.1104 |  | 314548 | ok | on test |
| variant_pathogenicity.pathogenic | base_rate | baseline | test | pred_finite_share | 1.0 |  | 314548 | ok | on test |
| variant_pathogenicity.pathogenic | base_rate | baseline | test | pred_std | 0.0 |  | 314548 | ok | on test |
| variant_pathogenicity.pathogenic | base_rate | baseline | test | reproduced_validation_value | 0.2085 |  | 314548 | ok | on test |
| variant_pathogenicity.pathogenic | base_rate | baseline | test | reproduction_gap_relative | 0.0 |  | 314548 | ok | on test |
| variant_pathogenicity.pathogenic | gbt_lightgbm | candidate | test | pr_auc | 0.9991 | 1.0 | 314548 | ok | on test |
| variant_pathogenicity.pathogenic | gbt_lightgbm | candidate | test | log_loss | 0.1308 | 0.0002 | 314548 | ok | on test |
| variant_pathogenicity.pathogenic | gbt_lightgbm | candidate | test | calibration_error | 0.0142 | 0.0001 | 314548 | ok | on test |
| variant_pathogenicity.pathogenic | gbt_lightgbm | candidate | test | base_rate | 0.2315 | 0.2085 | 314548 | ok | on test |
| variant_pathogenicity.pathogenic | gbt_lightgbm | candidate | test | pred_mean | 0.2174 | 0.2085 | 314548 | ok | on test |
| variant_pathogenicity.pathogenic | gbt_lightgbm | candidate | test | pred_positive_share | 0.2174 | 0.2085 | 314548 | ok | on test |
| variant_pathogenicity.pathogenic | gbt_lightgbm | candidate | test | pr_auc_lift | 4.3152 | 4.7959 | 314548 | ok | on test |
| variant_pathogenicity.pathogenic | gbt_lightgbm | candidate | test | primary_change_relative | 0.0009 |  | 314548 | ok | on test |
| variant_pathogenicity.pathogenic | gbt_lightgbm | candidate | test | skill_primary | 0.7676 |  | 314548 | ok | on test |
| variant_pathogenicity.pathogenic | gbt_lightgbm | candidate | test | skill_primary_ci_low | 0.6465 |  | 314548 | ok | on test |
| variant_pathogenicity.pathogenic | gbt_lightgbm | candidate | test | skill_primary_ci_high | 0.8019 |  | 314548 | ok | on test |
| variant_pathogenicity.pathogenic | gbt_lightgbm | candidate | test | bootstrap_resamples_valid | 200.0 |  | 314548 | ok | on test |
| variant_pathogenicity.pathogenic | gbt_lightgbm | candidate | test | bootstrap_blocks | 830.0 |  | 314548 | ok | on test |
| variant_pathogenicity.pathogenic | gbt_lightgbm | candidate | test | bootstrap_rows_used | 101708.0 |  | 314548 | ok | on test |
| variant_pathogenicity.pathogenic | gbt_lightgbm | candidate | test | pred_finite_share | 1.0 |  | 314548 | ok | on test |
| variant_pathogenicity.pathogenic | gbt_lightgbm | candidate | test | pred_std | 0.4123 |  | 314548 | ok | on test |
| variant_pathogenicity.pathogenic | gbt_lightgbm | candidate | test | reproduced_validation_value | 1.0 |  | 314548 | ok | on test |
| variant_pathogenicity.pathogenic | gbt_lightgbm | candidate | test | reproduction_gap_relative | 0.0 |  | 314548 | ok | on test |

### evaluation_flags

_no rows for this run_

### evaluation_run_context

45 row(s), first 30 shown

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| variant_pathogenicity.pathogenic | started | False | 51 |  | 2026-11-23 11:55:31.877499 |
| variant_pathogenicity.pathogenic | data_ready | False | 51 |  | 2026-11-23 11:55:38.656345 |
| variant_pathogenicity.pathogenic | finished | False | 51 | baseline_ok=True; fingerprint=a70db2f1d9a7 | 2026-11-23 11:56:12.506228 |
| variant_high_impact.high_impact | started | False | 51 |  | 2026-11-23 11:57:18.695238 |
| variant_high_impact.high_impact | data_ready | False | 51 |  | 2026-11-23 11:57:25.605547 |
| variant_high_impact.high_impact | finished | False | 51 | baseline_ok=True; fingerprint=51df841160f7 | 2026-11-23 11:58:18.585543 |
| variant_carrier_screening.carrier | started | False | 51 |  | 2026-11-23 11:58:59.172156 |
| variant_carrier_screening.carrier | data_ready | False | 51 |  | 2026-11-23 11:59:05.856115 |
| variant_carrier_screening.carrier | finished | False | 51 | baseline_ok=True; fingerprint=cd255c79db9e | 2026-11-23 11:59:38.708478 |
| variant_actionable_rare.actionable | started | False | 51 |  | 2026-11-23 12:00:22.553315 |
| variant_actionable_rare.actionable | data_ready | False | 51 |  | 2026-11-23 12:00:30.223785 |
| variant_actionable_rare.actionable | finished | False | 51 | baseline_ok=True; fingerprint=b2a3ef75e206 | 2026-11-23 12:01:04.368652 |
| structural_variant_disruption.disruption | started | False | 51 |  | 2026-11-23 12:01:18.725750 |
| structural_variant_disruption.disruption | data_ready | False | 51 |  | 2026-11-23 12:01:25.203089 |
| structural_variant_disruption.disruption | finished | False | 51 | baseline_ok=True; fingerprint=a0bf6095c4e7 | 2026-11-23 12:01:37.362312 |
| gene_pharmacogene.pharmacogene | started | False | 12 |  | 2026-11-23 12:01:54.717483 |
| gene_pharmacogene.pharmacogene | data_ready | False | 12 |  | 2026-11-23 12:02:01.192715 |
| gene_pharmacogene.pharmacogene | finished | False | 12 | baseline_ok=True; fingerprint=f99b762e0ea7 | 2026-11-23 12:02:23.067074 |
| gene_cancer_role.cancer_role | started | False | 12 |  | 2026-11-23 12:02:37.171793 |
| gene_cancer_role.cancer_role | data_ready | False | 12 |  | 2026-11-23 12:02:44.573930 |
| gene_cancer_role.cancer_role | finished | False | 12 | baseline_ok=True; fingerprint=5ebda7acdbd1 | 2026-11-23 12:03:01.810544 |
| benchmark_clinvar_vs_benign.label | started | False | 56 |  | 2026-11-23 12:03:12.852155 |
| benchmark_clinvar_vs_benign.label | data_ready | False | 56 |  | 2026-11-23 12:03:19.157928 |
| benchmark_clinvar_vs_benign.label | finished | False | 56 | baseline_ok=True; fingerprint=c197958876b0 | 2026-11-23 12:03:29.405829 |
| benchmark_gnomad_balanced.label | started | False | 56 |  | 2026-11-23 12:04:01.417780 |
| benchmark_gnomad_balanced.label | data_ready | False | 56 |  | 2026-11-23 12:04:08.960267 |
| benchmark_gnomad_balanced.label | finished | False | 56 | baseline_ok=True; fingerprint=bba3ce02e216 | 2026-11-23 12:04:50.323122 |
| benchmark_proteingym_clinical.label | started | False | 56 |  | 2026-11-23 12:05:01.341889 |
| benchmark_proteingym_clinical.label | data_ready | False | 56 |  | 2026-11-23 12:05:07.878401 |
| benchmark_proteingym_clinical.label | finished | False | 56 | baseline_ok=True; fingerprint=593c9b5df95e | 2026-11-23 12:05:18.000278 |

<!-- section: genomics__operate__01_reference_and_monitor -->
## Reference Profiles And Monitoring

### reference_profile

1890 row(s), first 30 shown

| task_id | model_name | subject | value_kind | n_rows | null_rate |
|---|---|---|---|---|---|
| benchmark_causal_eqtl.label | gbt_lightgbm | variant_key | categorical | 75559 | 0.0 |
| benchmark_causal_eqtl.label | gbt_lightgbm | assembly_basis | categorical | 75559 | 0.0 |
| benchmark_causal_eqtl.label | gbt_lightgbm | contig | numeric | 75559 | 0.0249 |
| benchmark_causal_eqtl.label | gbt_lightgbm | position | numeric | 75559 | 0.0 |
| benchmark_causal_eqtl.label | gbt_lightgbm | reference_allele | categorical | 75559 | 0.0 |
| benchmark_causal_eqtl.label | gbt_lightgbm | alternate_allele | categorical | 75559 | 0.0 |
| benchmark_causal_eqtl.label | gbt_lightgbm | consequence | numeric | 75559 | 1.0 |
| benchmark_causal_eqtl.label | gbt_lightgbm | gene | numeric | 75559 | 1.0 |
| benchmark_causal_eqtl.label | gbt_lightgbm | allele_count | numeric | 75559 | 1.0 |
| benchmark_causal_eqtl.label | gbt_lightgbm | allele_number | numeric | 75559 | 1.0 |
| benchmark_causal_eqtl.label | gbt_lightgbm | allele_frequency | numeric | 75559 | 1.0 |
| benchmark_causal_eqtl.label | gbt_lightgbm | minor_allele_frequency | numeric | 75559 | 1.0 |
| benchmark_causal_eqtl.label | gbt_lightgbm | minor_allele_count | numeric | 75559 | 1.0 |
| benchmark_causal_eqtl.label | gbt_lightgbm | tissue | categorical | 75559 | 0.0 |
| benchmark_causal_eqtl.label | gbt_lightgbm | distance_to_nearest_transcription_start_site | numeric | 75559 | 0.0 |
| benchmark_causal_eqtl.label | gbt_lightgbm | gpn_star_v100 | numeric | 75559 | 1.0 |
| benchmark_causal_eqtl.label | gbt_lightgbm | gpn_star_p243 | numeric | 75559 | 1.0 |
| benchmark_causal_eqtl.label | gbt_lightgbm | gpn_star_m447 | numeric | 75559 | 1.0 |
| benchmark_causal_eqtl.label | gbt_lightgbm | protein_id | numeric | 75559 | 1.0 |
| benchmark_causal_eqtl.label | gbt_lightgbm | mutant | numeric | 75559 | 1.0 |
| benchmark_causal_eqtl.label | gbt_lightgbm | protein_position | numeric | 75559 | 1.0 |
| benchmark_causal_eqtl.label | gbt_lightgbm | wild_type_aa | numeric | 75559 | 1.0 |
| benchmark_causal_eqtl.label | gbt_lightgbm | mutant_aa | numeric | 75559 | 1.0 |
| benchmark_causal_eqtl.label | gbt_lightgbm | sequence_length | numeric | 75559 | 1.0 |
| benchmark_causal_eqtl.label | gbt_lightgbm | __prediction__ | numeric | 75559 | 0.0 |
| benchmark_clinvar_vs_benign.label | logistic | variant_key | categorical | 35037 | 0.0 |
| benchmark_clinvar_vs_benign.label | logistic | assembly_basis | categorical | 35037 | 0.0 |
| benchmark_clinvar_vs_benign.label | logistic | contig | categorical | 35037 | 0.0 |
| benchmark_clinvar_vs_benign.label | logistic | position | numeric | 35037 | 0.0 |
| benchmark_clinvar_vs_benign.label | logistic | reference_allele | categorical | 35037 | 0.0 |

### monitoring_results

11340 row(s), first 30 shown

| task_id | model_name | window_id | check_kind | subject | value | level |
|---|---|---|---|---|---|---|
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | feature_drift | variant_key | 0.0025 | ok |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | null_rate | variant_key | 0.0 | ok |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | unseen_category | variant_key | 0.9913 | info |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | feature_drift | assembly_basis | 0.0 | ok |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | null_rate | assembly_basis | 0.0 | ok |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | unseen_category | assembly_basis | 0.0 | info |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | feature_drift | contig | 0.0002 | ok |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | null_rate | contig | 0.1347 | ok |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | out_of_range | contig | 0.0 | info |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | feature_drift | position | 0.0004 | ok |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | null_rate | position | 0.0 | ok |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | out_of_range | position | 0.0 | info |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | feature_drift | reference_allele | 0.0003 | ok |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | null_rate | reference_allele | 0.0 | ok |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | unseen_category | reference_allele | 0.0 | info |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | feature_drift | alternate_allele | 0.0006 | ok |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | null_rate | alternate_allele | 0.0 | ok |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | unseen_category | alternate_allele | 0.0 | info |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | feature_drift | consequence |  | not_applicable |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | null_rate | consequence | 0.0 | ok |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | out_of_range | consequence |  | not_applicable |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | feature_drift | gene |  | not_applicable |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | null_rate | gene | 0.0 | ok |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | out_of_range | gene |  | not_applicable |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | feature_drift | allele_count |  | not_applicable |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | null_rate | allele_count | 0.0 | ok |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | out_of_range | allele_count |  | not_applicable |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | feature_drift | allele_number |  | not_applicable |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | null_rate | allele_number | 0.0 | ok |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | out_of_range | allele_number |  | not_applicable |

### monitoring_flags

28 row(s)

| task_id | model_name | window_id | check_kind | subject | level | value | threshold |
|---|---|---|---|---|---|---|---|
| benchmark_causal_eqtl.label | gbt_lightgbm | held_out | feature_drift | contig | flag | 8.0235 | 0.25 |
| benchmark_causal_eqtl.label | gbt_lightgbm | held_out | feature_drift | position | flag | 1.0014 | 0.25 |
| benchmark_causal_eqtl.label | gbt_lightgbm | held_out | prediction_drift | __prediction__ | flag | 0.4986 | 0.25 |
| benchmark_clinvar_vs_benign.label | logistic | held_out | feature_drift | position | warning | 0.1022 | 0.1 |
| benchmark_maps.label | gbt_lightgbm | held_out | feature_drift | protein_id | flag | 0.2965 | 0.25 |
| benchmark_maps.label | gbt_lightgbm | held_out | feature_drift | protein_position | flag | 0.3615 | 0.25 |
| benchmark_maps.label | gbt_lightgbm | held_out | feature_drift | sequence_length | flag | 1.5393 | 0.25 |
| benchmark_maps.label | gbt_lightgbm | held_out | prediction_drift | __prediction__ | warning | 0.1848 | 0.1 |
| benchmark_pathogenic_coding.label | gbt_lightgbm | held_out | feature_drift | contig | flag | 9.5437 | 0.25 |
| benchmark_pathogenic_coding.label | gbt_lightgbm | held_out | feature_drift | position | flag | 0.9822 | 0.25 |
| benchmark_pathogenic_coding.label | gbt_lightgbm | held_out | feature_drift | consequence | warning | 0.1492 | 0.1 |
| benchmark_pathogenic_coding.label | gbt_lightgbm | held_out | prediction_drift | __prediction__ | warning | 0.1386 | 0.1 |
| variant_actionable_rare.actionable | gbt_lightgbm | validation | feature_drift | alphamissense_pathogenicity_context | flag | 2.5034 | 0.25 |
| variant_actionable_rare.actionable | gbt_lightgbm | held_out | feature_drift | alphamissense_pathogenicity_context | flag | 2.4712 | 0.25 |
| variant_carrier_screening.carrier | gbt_lightgbm | validation | feature_drift | alphamissense_pathogenicity_context | flag | 2.4751 | 0.25 |
| variant_carrier_screening.carrier | gbt_lightgbm | held_out | feature_drift | alphamissense_pathogenicity_context | flag | 2.4448 | 0.25 |
| variant_high_impact.high_impact | gbt_lightgbm | validation | null_rate | is_pharmgkb_annotated_variant | flag | 5.944 | 5.0 |
| variant_high_impact.high_impact | gbt_lightgbm | validation | feature_drift | alphamissense_pathogenicity_context | flag | 2.7943 | 0.25 |
| variant_pathogenicity.pathogenic | gbt_lightgbm | held_out | null_rate | contig | flag | 95.491 | 5.0 |
| variant_pathogenicity.pathogenic | gbt_lightgbm | held_out | null_rate | is_druggable_gene | flag | 11.788 | 5.0 |
| variant_pathogenicity.pathogenic | gbt_lightgbm | held_out | null_rate | is_key_protein_type | flag | 9.656 | 5.0 |
| variant_pathogenicity.pathogenic | gbt_lightgbm | held_out | null_rate | is_pharmgkb_annotated_variant | flag | 7.314 | 5.0 |
| variant_pathogenicity.pathogenic | gbt_lightgbm | held_out | feature_drift | alphamissense_pathogenicity_context | flag | 0.5158 | 0.25 |
| variant_high_impact.high_impact | gbt_lightgbm | held_out | null_rate | is_druggable_gene | flag | 7.859 | 5.0 |
| variant_high_impact.high_impact | gbt_lightgbm | held_out | null_rate | is_key_protein_type | flag | 6.235 | 5.0 |
| variant_high_impact.high_impact | gbt_lightgbm | held_out | feature_drift | alphamissense_pathogenicity_context | flag | 2.7312 | 0.25 |
| variant_pathogenicity.pathogenic | gbt_lightgbm | validation | null_rate | is_pharmgkb_annotated_variant | flag | 7.429 | 5.0 |
| variant_pathogenicity.pathogenic | gbt_lightgbm | validation | feature_drift | alphamissense_pathogenicity_context | flag | 0.5268 | 0.25 |

### model_predictions

2237140 row(s), first 30 shown

| task_id | model_name | window_id | row_key | prediction |
|---|---|---|---|---|
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | d26b8f3fbd78313030ea84619e67a4dabcf599f421db38a83264703196e887ba | 0.4005 |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | 6e0bf1ea712111d93f404d7fa54a8deb688df46b5bbfe2702420e892ff73bc0a | 0.3607 |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | 6ddc3d509877e152eed1632f45f4939646ecd892aa8cb75de2f0d87aa0b540b7 | 0.3054 |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | 62d7c019a36e0a5f61fa770791a2281c66781d1b5c0be22867c00c370cb72f58 | 0.1589 |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | d8475f37825cadd4ad245bb52765b22db91c20bd79b585ed84c4fd3407658e93 | 0.2226 |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | 0f2658c8d474b4d92a45c848a978b5edab16dac1755c16d84e6f121cb4240e37 | 0.7146 |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | c2590d53a87a08fc049a2c227a808cb0e1008cc2db12e18adbfabb2be3d2a4a7 | 0.4212 |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | ed295a749b1e4dc3d6369afef1d7b68d5a5e12dd82b826beff6db6ae324906da | 0.4975 |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | 83913944f15619e886236b0bfc64b1148954549b3d4568be0ebe7b68f99f7c32 | 0.7923 |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | ffaedef4a9940fa0adc5a5def19135663faac465a1b79ef421aee3acaa71a30d | 0.4062 |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | 1284afd33885b26b195066733f4a004aad4b9222528a99f8af9f53e5df474495 | 0.6071 |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | b398658c418024d89300fd9100565ca4765261691eb749bcd704497f137c5065 | 0.5668 |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | 3abf515240c91d4b39f2b3a6e6bb5fdc3139aa2e0d17d54e68f468cf2754bf2f | 0.394 |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | d1e17c1551a05b110d5a36b511498bcb2285fe5af9d8b5d141992d1c47271b1e | 0.6197 |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | 52928d4b3c775193061e17d7959b0b4e35231277f9acac94757c1986ce687e20 | 0.5606 |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | c630d03d95bb4db82daa481c61d90f509cf79894c5613f9d156bee6df8adde1f | 0.6952 |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | 0156e1666b04c381c42af0aa6a5cf36e782e54a5133005d761e9474b60802257 | 0.0162 |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | 7acf0fa66d0b3db3d590e22c3f9509daf530e3d1ce055dbf847ce1e94ead2519 | 0.4127 |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | c866943c0ed2d177e8f0ae7ad226c8b93c299160a94bd99dc803f9f9cb693113 | 0.4187 |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | 227854211c0f5e2cf11fbb3d61d9d99ffba3e9fe8d71b47bf15f23e997d1dc9c | 0.4038 |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | 6e9b81e2d5d0f300355f8146514a2e0ae834aa85aec30996f8c4b99c9cd3b564 | 0.4252 |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | 427f53635aea0790f6696a879ffe1b22562144eca9f94400285644fbd265cb49 | 0.3783 |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | 4f503651365195f48044daa3032f7fd5f340b0154bbc8fd0a386a5c16a12f3ec | 0.514 |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | 290e837d8504b15b7fdc0407516b217736279da78eb4f73bb3267f9e4eb73c1a | 0.4081 |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | 17598fb7645442955397fb5b0cbccf0d676a08975e6dff1bac460709844ad97f | 0.4298 |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | ce1281350fe452b9830d34d727af29af265dac249e177a29699b9228ecb38e20 | 0.5051 |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | 26fe4f6a07af03f78a9d9ea7a82b979dadb7d5d500e48aa2832efe79ce69d09b | 0.441 |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | 1dd35c430f124f38d58c4c1d25cd2d5d751b9a57d40d941773c6c8942c3ec198 | 0.4364 |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | 993e0910edf3d91ed99135273d3daaaa277992927bc0c6172df15b4681c1c80b | 0.4808 |
| benchmark_causal_eqtl.label | gbt_lightgbm | validation | 844e2720505480f3258ca84ed380eca8aabad39d69cd29fd4464a0028b4659fb | 0.3375 |

<!-- section: genomics__operate__02_performance_and_triggers -->
## Performance And Triggers

### monitoring_results

13 row(s)

| task_id | model_name | window_id | check_kind | subject | value | level |
|---|---|---|---|---|---|---|
| gene_cancer_role.cancer_role | gbt_lightgbm | held_out | performance | balanced_accuracy | -0.0312 | ok |
| benchmark_proteingym_clinical.label | gbt_lightgbm | held_out | performance | pr_auc | -0.0148 | ok |
| variant_pathogenicity.pathogenic | gbt_lightgbm | held_out | performance | pr_auc | 0.0009 | ok |
| benchmark_gnomad_balanced.label | gbt_sklearn | held_out | performance | pr_auc | 0.0069 | ok |
| benchmark_clinvar_vs_benign.label | logistic | held_out | performance | pr_auc | 0.0105 | ok |
| benchmark_causal_eqtl.label | gbt_lightgbm | held_out | performance | pr_auc | 0.1344 | ok |
| benchmark_maps.label | gbt_lightgbm | held_out | performance | pr_auc | -0.026 | ok |
| variant_high_impact.high_impact | gbt_lightgbm | held_out | performance | pr_auc | 0.0 | ok |
| structural_variant_disruption.disruption | gbt_lightgbm | held_out | performance | pr_auc | 0.0 | ok |
| variant_actionable_rare.actionable | gbt_lightgbm | held_out | performance | pr_auc | 0.0 | ok |
| variant_carrier_screening.carrier | gbt_lightgbm | held_out | performance | pr_auc | 0.0 | ok |
| benchmark_pathogenic_coding.label | gbt_lightgbm | held_out | performance | pr_auc | 0.0 | ok |
| gene_pharmacogene.pharmacogene | logistic | held_out | performance | pr_auc | 0.0 | ok |

### monitoring_flags

_no rows for this run_

### monitoring_triggers

7 row(s)

| task_id | model_name | trigger_kind | source | condition | value | threshold |
|---|---|---|---|---|---|---|
| variant_actionable_rare.actionable | gbt_lightgbm | drift_flag | monitoring | a drift or input-quality check reached the flag level | 2.5034 | 0.25 |
| variant_carrier_screening.carrier | gbt_lightgbm | drift_flag | monitoring | a drift or input-quality check reached the flag level | 2.4751 | 0.25 |
| variant_high_impact.high_impact | gbt_lightgbm | drift_flag | monitoring | a drift or input-quality check reached the flag level | 7.859 | 5.0 |
| benchmark_pathogenic_coding.label | gbt_lightgbm | drift_flag | monitoring | a drift or input-quality check reached the flag level | 9.5437 | 0.25 |
| benchmark_maps.label | gbt_lightgbm | drift_flag | monitoring | a drift or input-quality check reached the flag level | 1.5393 | 0.25 |
| variant_pathogenicity.pathogenic | gbt_lightgbm | drift_flag | monitoring | a drift or input-quality check reached the flag level | 95.491 | 5.0 |
| benchmark_causal_eqtl.label | gbt_lightgbm | drift_flag | monitoring | a drift or input-quality check reached the flag level | 8.0235 | 0.25 |

<!-- section: genomics__register__01_register_models -->
## Register Models

### model_registry

26 row(s)

| task_id | model_name | role | version | lifecycle_status | decision | validation_value | reason |
|---|---|---|---|---|---|---|---|
| variant_pathogenicity.pathogenic | gbt_lightgbm | selected | 1 | registered | approved | 1.0 | first registration |
| variant_pathogenicity.pathogenic | base_rate | baseline_fallback | 1 | registered | approved | 0.2085 | first registration |
| variant_high_impact.high_impact | gbt_lightgbm | selected | 1 | registered | approved | 1.0 | first registration |
| variant_high_impact.high_impact | base_rate | baseline_fallback | 1 | registered | approved | 0.3755 | first registration |
| variant_carrier_screening.carrier | gbt_lightgbm | selected | 1 | registered | approved | 1.0 | first registration |
| variant_carrier_screening.carrier | base_rate | baseline_fallback | 1 | registered | approved | 0.0906 | first registration |
| variant_actionable_rare.actionable | gbt_lightgbm | selected | 1 | registered | approved | 1.0 | first registration |
| variant_actionable_rare.actionable | base_rate | baseline_fallback | 1 | registered | approved | 0.0139 | first registration |
| structural_variant_disruption.disruption | gbt_lightgbm | selected | 1 | registered | approved | 1.0 | first registration |
| structural_variant_disruption.disruption | base_rate | baseline_fallback | 1 | registered | approved | 0.8285 | first registration |
| gene_pharmacogene.pharmacogene | logistic | selected | 1 | registered | approved | 1.0 | first registration |
| gene_pharmacogene.pharmacogene | base_rate | baseline_fallback | 1 | registered | approved | 0.1086 | first registration |
| gene_cancer_role.cancer_role | gbt_lightgbm | selected | 1 | registered | approved | 0.8406 | first registration |
| gene_cancer_role.cancer_role | majority_class | baseline_fallback | 1 | registered | approved | 0.3333 | first registration |
| benchmark_clinvar_vs_benign.label | logistic | selected | 1 | registered | approved | 0.9182 | first registration |
| benchmark_clinvar_vs_benign.label | base_rate | baseline_fallback | 1 | registered | approved | 0.4667 | first registration |
| benchmark_gnomad_balanced.label | gbt_sklearn | selected | 1 | registered | approved | 0.5623 | first registration |
| benchmark_gnomad_balanced.label | base_rate | baseline_fallback | 1 | registered | approved | 0.5 | first registration |
| benchmark_proteingym_clinical.label | gbt_lightgbm | selected | 1 | registered | approved | 0.7355 | first registration |
| benchmark_proteingym_clinical.label | base_rate | baseline_fallback | 1 | registered | approved | 0.4891 | first registration |
| benchmark_pathogenic_coding.label | gbt_lightgbm | selected | 1 | registered | approved | 1.0 | first registration |
| benchmark_pathogenic_coding.label | base_rate | baseline_fallback | 1 | registered | approved | 0.5567 | first registration |
| benchmark_maps.label | gbt_lightgbm | selected | 1 | registered | approved | 0.7436 | first registration |
| benchmark_maps.label | base_rate | baseline_fallback | 1 | registered | approved | 0.2725 | first registration |
| benchmark_causal_eqtl.label | gbt_lightgbm | selected | 1 | registered | approved | 0.8033 | first registration |
| benchmark_causal_eqtl.label | base_rate | baseline_fallback | 1 | registered | approved | 0.4983 | first registration |

<!-- section: genomics__register__02_reproducibility -->
## Reproducibility Check

### registry_check

26 row(s)

| task_id | model_name | role | registry_version | processor_type | reload_status | rescore_status | gap_relative | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| variant_pathogenicity.pathogenic | gbt_lightgbm | selected | 1 | aarch64 | ok | matched | 0.0 | passed | pr_auc on validation |
| variant_pathogenicity.pathogenic | base_rate | baseline_fallback | 1 | aarch64 | ok | matched | 0.0 | passed | pr_auc on validation |
| variant_high_impact.high_impact | gbt_lightgbm | selected | 1 | aarch64 | ok | matched | 0.0 | passed | pr_auc on validation |
| variant_high_impact.high_impact | base_rate | baseline_fallback | 1 | aarch64 | ok | matched | 0.0 | passed | pr_auc on validation |
| variant_carrier_screening.carrier | gbt_lightgbm | selected | 1 | aarch64 | ok | matched | 0.0 | passed | pr_auc on validation |
| variant_carrier_screening.carrier | base_rate | baseline_fallback | 1 | aarch64 | ok | matched | 0.0 | passed | pr_auc on validation |
| variant_actionable_rare.actionable | gbt_lightgbm | selected | 1 | aarch64 | ok | matched | 0.0 | passed | pr_auc on validation |
| variant_actionable_rare.actionable | base_rate | baseline_fallback | 1 | aarch64 | ok | matched | 0.0 | passed | pr_auc on validation |
| structural_variant_disruption.disruption | gbt_lightgbm | selected | 1 | aarch64 | ok | matched | 0.0 | passed | pr_auc on validation |
| structural_variant_disruption.disruption | base_rate | baseline_fallback | 1 | aarch64 | ok | matched | 0.0 | passed | pr_auc on validation |
| gene_pharmacogene.pharmacogene | logistic | selected | 1 | aarch64 | ok | matched | 0.0 | passed | pr_auc on validation |
| gene_pharmacogene.pharmacogene | base_rate | baseline_fallback | 1 | aarch64 | ok | matched | 0.0 | passed | pr_auc on validation |
| gene_cancer_role.cancer_role | gbt_lightgbm | selected | 1 | aarch64 | ok | matched | 0.0 | passed | balanced_accuracy on validation |
| gene_cancer_role.cancer_role | majority_class | baseline_fallback | 1 | aarch64 | ok | matched | 0.0 | passed | balanced_accuracy on validation |
| benchmark_clinvar_vs_benign.label | logistic | selected | 1 | aarch64 | ok | matched | 0.0 | passed | pr_auc on validation |
| benchmark_clinvar_vs_benign.label | base_rate | baseline_fallback | 1 | aarch64 | ok | matched | 0.0 | passed | pr_auc on validation |
| benchmark_gnomad_balanced.label | gbt_sklearn | selected | 1 | aarch64 | ok | matched | 0.0 | passed | pr_auc on validation |
| benchmark_gnomad_balanced.label | base_rate | baseline_fallback | 1 | aarch64 | ok | matched | 0.0 | passed | pr_auc on validation |
| benchmark_proteingym_clinical.label | gbt_lightgbm | selected | 1 | aarch64 | ok | matched | 0.0 | passed | pr_auc on validation |
| benchmark_proteingym_clinical.label | base_rate | baseline_fallback | 1 | aarch64 | ok | matched | 0.0 | passed | pr_auc on validation |
| benchmark_pathogenic_coding.label | gbt_lightgbm | selected | 1 | aarch64 | ok | matched | 0.0 | passed | pr_auc on validation |
| benchmark_pathogenic_coding.label | base_rate | baseline_fallback | 1 | aarch64 | ok | matched | 0.0 | passed | pr_auc on validation |
| benchmark_maps.label | gbt_lightgbm | selected | 1 | aarch64 | ok | matched | 0.0 | passed | pr_auc on validation |
| benchmark_maps.label | base_rate | baseline_fallback | 1 | aarch64 | ok | matched | 0.0 | passed | pr_auc on validation |
| benchmark_causal_eqtl.label | gbt_lightgbm | selected | 1 | aarch64 | ok | matched | 0.0 | passed | pr_auc on validation |
| benchmark_causal_eqtl.label | base_rate | baseline_fallback | 1 | aarch64 | ok | matched | 0.0 | passed | pr_auc on validation |

<!-- section: genomics__setup__02_model_setup -->
## Model Setup

### evaluation_spec

10 row(s)

| dataset_id | spec_key | spec_value |
|---|---|---|
| benchmark_pathogenic_coding | tuning_folds | 5 |
| benchmark_pathogenic_coding | tuning_rule | hash of source_record_id |
| benchmark_maps | tuning_folds | 5 |
| benchmark_maps | tuning_rule | hash of source_record_id |
| benchmark_causal_eqtl | tuning_folds | 5 |
| benchmark_causal_eqtl | tuning_rule | hash of source_record_id |
| benchmark_pathogenic_non_coding | scored_by | benchmark_pathogenic_coding.label |
| benchmark_pathogenic_non_coding | use | disclosure only |
| benchmark_spectralbio | scored_by | benchmark_pathogenic_coding.label |
| benchmark_spectralbio | use | disclosure only |

### approval_spec

_no rows for this run_

### monitoring_spec

_no rows for this run_

<!-- section: genomics__train__01_train -->
## Model Training

### candidate_results

476 row(s), first 30 shown

| task_id | model_name | family | stage | partition | metric | value | n_rows | status | detail |
|---|---|---|---|---|---|---|---|---|---|
| variant_pathogenicity.pathogenic | base_rate | baseline | baseline | validation | pr_auc | 0.2085 | 141509 | ok | seconds=1.2 |
| variant_pathogenicity.pathogenic | base_rate | baseline | baseline | validation | log_loss | 0.5121 | 141509 | ok | seconds=1.2 |
| variant_pathogenicity.pathogenic | base_rate | baseline | baseline | validation | calibration_error | 0.0054 | 141509 | ok | seconds=1.2 |
| variant_pathogenicity.pathogenic | base_rate | baseline | baseline | validation | base_rate | 0.2085 | 141509 | ok | seconds=1.2 |
| variant_pathogenicity.pathogenic | base_rate | baseline | baseline | validation | pred_mean | 0.2031 | 141509 | ok | seconds=1.2 |
| variant_pathogenicity.pathogenic | base_rate | baseline | baseline | validation | pred_positive_share | 0.0 | 141509 | ok | seconds=1.2 |
| variant_pathogenicity.pathogenic | base_rate | baseline | baseline | validation | pr_auc_lift | 1.0 | 141509 | ok | seconds=1.2 |
| variant_pathogenicity.pathogenic | logistic | tabular | candidate | validation | pr_auc | 0.9997 | 141509 | ok | seconds=23.4 |
| variant_pathogenicity.pathogenic | logistic | tabular | candidate | validation | log_loss | 0.0063 | 141509 | ok | seconds=23.4 |
| variant_pathogenicity.pathogenic | logistic | tabular | candidate | validation | calibration_error | 0.0002 | 141509 | ok | seconds=23.4 |
| variant_pathogenicity.pathogenic | logistic | tabular | candidate | validation | base_rate | 0.2085 | 141509 | ok | seconds=23.4 |
| variant_pathogenicity.pathogenic | logistic | tabular | candidate | validation | pred_mean | 0.2087 | 141509 | ok | seconds=23.4 |
| variant_pathogenicity.pathogenic | logistic | tabular | candidate | validation | pred_positive_share | 0.209 | 141509 | ok | seconds=23.4 |
| variant_pathogenicity.pathogenic | logistic | tabular | candidate | validation | pr_auc_lift | 4.7945 | 141509 | ok | seconds=23.4 |
| variant_pathogenicity.pathogenic | logistic | tabular | candidate | validation | train_pr_auc | 0.9998 | 141509 | ok | seconds=23.4 |
| variant_pathogenicity.pathogenic | logistic | tabular | candidate | validation | train_log_loss | 0.0056 | 141509 | ok | seconds=23.4 |
| variant_pathogenicity.pathogenic | gbt_lightgbm | tabular | candidate | validation | pr_auc | 1.0 | 141509 | ok | seconds=30.0 |
| variant_pathogenicity.pathogenic | gbt_lightgbm | tabular | candidate | validation | log_loss | 0.0002 | 141509 | ok | seconds=30.0 |
| variant_pathogenicity.pathogenic | gbt_lightgbm | tabular | candidate | validation | calibration_error | 0.0001 | 141509 | ok | seconds=30.0 |
| variant_pathogenicity.pathogenic | gbt_lightgbm | tabular | candidate | validation | base_rate | 0.2085 | 141509 | ok | seconds=30.0 |
| variant_pathogenicity.pathogenic | gbt_lightgbm | tabular | candidate | validation | pred_mean | 0.2085 | 141509 | ok | seconds=30.0 |
| variant_pathogenicity.pathogenic | gbt_lightgbm | tabular | candidate | validation | pred_positive_share | 0.2085 | 141509 | ok | seconds=30.0 |
| variant_pathogenicity.pathogenic | gbt_lightgbm | tabular | candidate | validation | pr_auc_lift | 4.7959 | 141509 | ok | seconds=30.0 |
| variant_pathogenicity.pathogenic | gbt_lightgbm | tabular | candidate | validation | train_pr_auc | 1.0 | 141509 | ok | seconds=30.0 |
| variant_pathogenicity.pathogenic | gbt_lightgbm | tabular | candidate | validation | train_log_loss | 0.0 | 141509 | ok | seconds=30.0 |
| variant_pathogenicity.pathogenic | gbt_sklearn | tabular | candidate | validation | pr_auc | 1.0 | 141509 | ok | seconds=150.2 |
| variant_pathogenicity.pathogenic | gbt_sklearn | tabular | candidate | validation | log_loss | 0.0002 | 141509 | ok | seconds=150.2 |
| variant_pathogenicity.pathogenic | gbt_sklearn | tabular | candidate | validation | calibration_error | 0.0 | 141509 | ok | seconds=150.2 |
| variant_pathogenicity.pathogenic | gbt_sklearn | tabular | candidate | validation | base_rate | 0.2085 | 141509 | ok | seconds=150.2 |
| variant_pathogenicity.pathogenic | gbt_sklearn | tabular | candidate | validation | pred_mean | 0.2085 | 141509 | ok | seconds=150.2 |

### task_run_context

39 row(s), first 30 shown

| task_id | status | smoke | frozen_delta_version | detail | recorded_at |
|---|---|---|---|---|---|
| variant_pathogenicity.pathogenic | started | False | 51 |  | 2026-11-22 21:14:11.582292 |
| variant_pathogenicity.pathogenic | data_ready | False | 51 |  | 2026-11-22 21:16:40.344268 |
| variant_pathogenicity.pathogenic | finished | False | 51 | baseline_ok=True; fingerprint=9006604911c4 | 2026-11-22 21:20:16.869744 |
| variant_high_impact.high_impact | started | False | 51 |  | 2026-11-22 21:20:21.706829 |
| variant_high_impact.high_impact | data_ready | False | 51 |  | 2026-11-22 21:22:26.916863 |
| variant_high_impact.high_impact | finished | False | 51 | baseline_ok=True; fingerprint=fc9bfe91f390 | 2026-11-22 21:25:46.527951 |
| variant_carrier_screening.carrier | started | False | 51 |  | 2026-11-22 21:25:51.573218 |
| variant_carrier_screening.carrier | data_ready | False | 51 |  | 2026-11-22 21:28:07.612934 |
| variant_carrier_screening.carrier | finished | False | 51 | baseline_ok=True; fingerprint=a5c0649b8dd8 | 2026-11-22 21:32:04.842607 |
| variant_actionable_rare.actionable | started | False | 51 |  | 2026-11-22 21:32:10.260246 |
| variant_actionable_rare.actionable | data_ready | False | 51 |  | 2026-11-22 21:34:30.892310 |
| variant_actionable_rare.actionable | finished | False | 51 | baseline_ok=True; fingerprint=bb1a7c7f2b5a | 2026-11-22 21:37:01.701719 |
| structural_variant_disruption.disruption | started | False | 51 |  | 2026-11-22 21:37:06.887588 |
| structural_variant_disruption.disruption | data_ready | False | 51 |  | 2026-11-22 21:37:15.656319 |
| structural_variant_disruption.disruption | finished | False | 51 | baseline_ok=True; fingerprint=4b9d613755f6 | 2026-11-22 21:38:19.544323 |
| gene_pharmacogene.pharmacogene | started | False | 12 |  | 2026-11-22 21:38:24.145268 |
| gene_pharmacogene.pharmacogene | data_ready | False | 12 |  | 2026-11-22 21:39:02.232341 |
| gene_pharmacogene.pharmacogene | finished | False | 12 | baseline_ok=True; fingerprint=a378ccddee16 | 2026-11-22 21:44:15.648441 |
| gene_cancer_role.cancer_role | started | False | 12 |  | 2026-11-22 21:44:20.561468 |
| gene_cancer_role.cancer_role | data_ready | False | 12 |  | 2026-11-22 21:44:36.291526 |
| gene_cancer_role.cancer_role | finished | False | 12 | baseline_ok=True; fingerprint=2b6b607ae0be | 2026-11-22 21:46:16.946828 |
| benchmark_clinvar_vs_benign.label | started | False | 56 |  | 2026-11-22 21:46:21.542118 |
| benchmark_clinvar_vs_benign.label | data_ready | False | 56 |  | 2026-11-22 21:46:28.877481 |
| benchmark_clinvar_vs_benign.label | finished | False | 56 | baseline_ok=True; fingerprint=d1285388c299 | 2026-11-22 21:47:13.231943 |
| benchmark_gnomad_balanced.label | started | False | 56 |  | 2026-11-22 21:47:17.752606 |
| benchmark_gnomad_balanced.label | data_ready | False | 56 |  | 2026-11-22 21:48:48.518907 |
| benchmark_gnomad_balanced.label | finished | False | 56 | baseline_ok=True; fingerprint=e9604b7cc666 | 2026-11-22 21:51:42.743298 |
| benchmark_proteingym_clinical.label | started | False | 56 |  | 2026-11-22 21:51:47.696300 |
| benchmark_proteingym_clinical.label | data_ready | False | 56 |  | 2026-11-22 21:51:55.143772 |
| benchmark_proteingym_clinical.label | finished | False | 56 | baseline_ok=True; fingerprint=7ebd4254734b | 2026-11-22 21:52:31.064985 |
