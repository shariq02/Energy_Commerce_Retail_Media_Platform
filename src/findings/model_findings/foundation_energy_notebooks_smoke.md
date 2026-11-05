# FOUNDATION ENERGY NOTEBOOK FINDINGS (SMOKE RUN)

_Merged by the findings gate from the files of the model notebooks; a section is replaced when its notebook runs again. Generated: 2026-10-25T13:29Z_

<!-- section: foundation_energy__01_foundation_ts_energy -->
## Foundation Time Series Energy

### summary

| task | foundation candidates | ok | failed or skipped |
|---|---|---|---|
| price_daily.price | 5 | 5 | 0 |
| price_quarter_hour.price | 5 | 5 | 0 |
| load.load | 6 | 6 | 0 |
| bias.bias | 5 | 5 | 0 |
| zone_generation.generation | 5 | 5 | 0 |
| honda_forecast.increment | 5 | 5 | 0 |

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
| load.load | ttm_r2_zero_shot | no skill over the baseline | skill_mae = -0.2255 |
| load.load | energy_ttm_zero_shot | no skill over the baseline | skill_mae = -0.307 |
| bias.bias | zero_correction | biased predictions | mean error is +84% of the mean absolute target |

### price_daily.price

_dataset `price_daily`, frozen version 0, primary metric `mae` (lower is better)_

#### data seen by the models

- frame 1: 2280 rows x 90 columns, sample fraction 1.0, rows by partition {'train': 1916, 'validation': 364}, highest null rates {'fold_id': 0.5206, 'price_eur_per_mwh_lag7': 0.0031}
- target, train: rows 1916, mean 96.93, std 97.77, min -53.87, max 699.4
- target, validation: rows 364, mean 78.55, std 36.43, min 1.78, max 395.3

#### candidates

| model | family | stage | status | artifact | rows | mae | detail |
|---|---|---|---|---|---|---|---|
| empirical_quantiles | baseline | baseline | ok | ok | 364 |  |  |
| seasonal_naive | baseline | baseline | ok | ok | 364 | 28.04 |  |
| chronos2_lora | foundation_ts | candidate | ok | ok | 364 | 20.21 |  |
| chronos2_zero_shot | foundation_ts | candidate | ok | ok | 364 | 20.21 |  |
| timesfm25_zero_shot | foundation_ts | candidate | ok | ok | 364 | 19.99 |  |
| toto2_zero_shot | foundation_ts | candidate | ok | ok | 364 | 19.97 |  |
| ttm_r2_zero_shot | foundation_ts | candidate | ok | ok | 364 | 24.36 |  |

#### metrics (validation partition)

| metric | empirical_quantiles | seasonal_naive | chronos2_lora | chronos2_zero_shot | timesfm25_zero_shot | toto2_zero_shot | ttm_r2_zero_shot |
|---|---|---|---|---|---|---|---|
| pinball_q10 | 5.803 |  | 4.778 | 4.779 | 4.905 | 4.8 |  |
| pinball_q50 | 14.58 | 14.02 | 10.11 | 10.1 | 9.994 | 9.985 | 12.18 |
| pinball_q90 | 16.8 |  | 4.737 | 4.737 | 4.837 | 4.713 |  |
| interval_coverage_80 | 0.9313 |  | 0.7308 | 0.7335 | 0.7198 | 0.7995 |  |
| mae |  | 28.04 | 20.21 | 20.21 | 19.99 | 19.97 | 24.36 |
| rmse |  | 42.9 | 29.78 | 29.78 | 29.77 | 29.79 | 34.95 |
| mean_error |  | -1.588 | -1.023 | -1.014 | -1.873 | -1.125 | -0.5582 |
| mean_error_relative |  | -0.02021 | -0.01303 | -0.01291 | -0.02384 | -0.01432 | -0.007106 |
| pred_std_ratio |  | 1.018 | 0.6576 | 0.6571 | 0.6271 | 0.6094 | 0.4434 |
| skill_pinball_q10 |  |  | 0.1766 | 0.1764 | 0.1547 | 0.1729 |  |
| skill_pinball_q50 |  |  | 0.307 | 0.3071 | 0.3147 | 0.3153 |  |
| skill_pinball_q90 |  |  | 0.7181 | 0.718 | 0.7121 | 0.7195 |  |
| skill_mae |  |  | 0.2792 | 0.2794 | 0.2872 | 0.2879 | 0.1313 |
| skill_rmse |  |  | 0.3058 | 0.3058 | 0.3062 | 0.3056 | 0.1854 |
| forecast_items |  |  | 364 | 364 | 364 | 364 | 364 |
| rows_without_context |  |  | 0 | 0 | 0 | 0 | 0 |
| context_rule_violations |  |  | 0 | 0 | 0 | 0 | 0 |

### price_quarter_hour.price

_dataset `price_quarter_hour`, frozen version 0, primary metric `mae` (lower is better)_

#### data seen by the models

- frame 1: 8000 rows x 45 columns, sample fraction 1.0, rows by partition {'train': 4000, 'validation': 4000}, highest null rates {'fold_id': 0.7362, 'price_eur_per_mwh_lag2d': 0.0041}
- target, train: rows 4000, mean 94, std 60.58, min -231, max 805.1
- target, validation: rows 4000, mean 87.66, std 48.12, min -61.08, max 493.9

#### candidates

| model | family | stage | status | artifact | rows | mae | detail |
|---|---|---|---|---|---|---|---|
| empirical_quantiles | baseline | baseline | ok | ok | 4000 |  |  |
| persistence | baseline | baseline | ok | ok | 4000 | 33.14 |  |
| chronos2_lora | foundation_ts | candidate | ok | ok | 4000 | 30.65 |  |
| chronos2_zero_shot | foundation_ts | candidate | ok | ok | 4000 | 30.74 |  |
| timesfm25_zero_shot | foundation_ts | candidate | ok | ok | 4000 | 29.02 |  |
| toto2_zero_shot | foundation_ts | candidate | ok | ok | 4000 | 30.6 |  |
| ttm_r2_zero_shot | foundation_ts | candidate | ok | ok | 4000 | 27.65 |  |

#### metrics (validation partition)

| metric | empirical_quantiles | persistence | chronos2_lora | chronos2_zero_shot | timesfm25_zero_shot | toto2_zero_shot | ttm_r2_zero_shot |
|---|---|---|---|---|---|---|---|
| pinball_q10 | 7.947 |  | 6.699 | 6.727 | 7.462 | 7.778 |  |
| pinball_q50 | 15.71 | 16.57 | 15.33 | 15.37 | 14.51 | 15.3 | 13.82 |
| pinball_q90 | 9.624 |  | 8.257 | 8.273 | 7.692 | 7.772 |  |
| interval_coverage_80 | 0.8315 |  | 0.7985 | 0.7973 | 0.7775 | 0.8167 |  |
| mae |  | 33.14 | 30.65 | 30.74 | 29.02 | 30.6 | 27.65 |
| rmse |  | 51.39 | 47.03 | 47.12 | 44.2 | 47.45 | 42.36 |
| mean_error |  | -1.051 | -5.543 | -5.626 | -3.854 | -2.117 | -1.644 |
| mean_error_relative |  | -0.01195 | -0.06304 | -0.06398 | -0.04383 | -0.02408 | -0.0187 |
| pred_std_ratio |  | 0.9641 | 0.8187 | 0.8176 | 0.7184 | 0.7511 | 0.6855 |
| skill_pinball_q10 |  |  | 0.157 | 0.1536 | 0.06107 | 0.02123 |  |
| skill_pinball_q50 |  |  | 0.02463 | 0.02199 | 0.07661 | 0.02631 |  |
| skill_pinball_q90 |  |  | 0.1421 | 0.1403 | 0.2007 | 0.1924 |  |
| skill_mae |  |  | 0.07503 | 0.07253 | 0.1243 | 0.07663 | 0.1657 |
| skill_rmse |  |  | 0.08484 | 0.08316 | 0.1399 | 0.07675 | 0.1757 |
| forecast_items |  |  | 183 | 183 | 183 | 183 | 183 |
| rows_without_context |  |  | 0 | 0 | 0 | 0 | 0 |
| context_rule_violations |  |  | 0 | 0 | 0 | 0 | 0 |

### load.load

_dataset `load`, frozen version 0, primary metric `mae` (lower is better)_

#### data seen by the models

- frame 1: 6184 rows x 101 columns, sample fraction 1.0, rows by partition {'train': 4000, 'validation': 2184}, highest null rates {'fold_id': 0.6276}
- target, train: rows 4000, mean 6.009e+05, std 4.302e+05, min 1.014e+05, max 1.682e+06
- target, validation: rows 2184, mean 5.485e+05, std 3.941e+05, min 1.035e+05, max 1.611e+06

#### candidates

| model | family | stage | status | artifact | rows | mae | detail |
|---|---|---|---|---|---|---|---|
| seasonal_mean | baseline | baseline | ok | ok | 2184 | 5.881e+04 |  |
| chronos2_lora | foundation_ts | candidate | ok | ok | 2184 | 3.933e+04 |  |
| chronos2_zero_shot | foundation_ts | candidate | ok | ok | 2184 | 3.933e+04 |  |
| energy_ttm_zero_shot | foundation_ts | candidate | ok | ok | 2184 | 7.686e+04 |  |
| timesfm25_zero_shot | foundation_ts | candidate | ok | ok | 2184 | 3.931e+04 |  |
| toto2_zero_shot | foundation_ts | candidate | ok | ok | 2184 | 4.202e+04 |  |
| ttm_r2_zero_shot | foundation_ts | candidate | ok | ok | 2184 | 7.206e+04 |  |

#### metrics (validation partition)

| metric | seasonal_mean | chronos2_lora | chronos2_zero_shot | energy_ttm_zero_shot | timesfm25_zero_shot | toto2_zero_shot | ttm_r2_zero_shot |
|---|---|---|---|---|---|---|---|
| mae | 5.881e+04 | 3.933e+04 | 3.933e+04 | 7.686e+04 | 3.931e+04 | 4.202e+04 | 7.206e+04 |
| rmse | 1.128e+05 | 8.675e+04 | 8.675e+04 | 1.233e+05 | 8.644e+04 | 9.404e+04 | 1.168e+05 |
| mean_error | 4.129e+04 | 3099 | 3214 | -1.56e+04 | -2298 | -149.5 | -2273 |
| mean_error_relative | 0.07527 | 0.005649 | 0.00586 | -0.02845 | -0.00419 | -0.0002725 | -0.004143 |
| pred_std_ratio | 1.045 | 0.9836 | 0.9838 | 0.9364 | 0.9768 | 0.9825 | 0.953 |
| skill_mae |  | 0.3311 | 0.3311 | -0.307 | 0.3315 | 0.2855 | -0.2255 |
| skill_rmse |  | 0.2312 | 0.2312 | -0.09289 | 0.2339 | 0.1666 | -0.03495 |
| forecast_items |  | 2184 | 2184 | 2184 | 2184 | 2184 | 2184 |
| rows_without_context |  | 0 | 0 | 0 | 0 | 0 | 0 |
| context_rule_violations |  | 0 | 0 | 0 | 0 | 0 | 0 |

### bias.bias

_dataset `bias`, frozen version 0, primary metric `mae` (lower is better)_

#### data seen by the models

- frame 1: 5819 rows x 91 columns, sample fraction 1.0, rows by partition {'train': 4000, 'validation': 1819}, highest null rates {'fold_id': 0.6087}
- target, train: rows 4000, mean 1605, std 1.981e+04, min -1.792e+05, max 1.105e+05
- target, validation: rows 1819, mean -7.85e+04, std 1.8e+05, min -8.85e+05, max 1.91e+05

#### candidates

| model | family | stage | status | artifact | rows | mae | detail |
|---|---|---|---|---|---|---|---|
| zero_correction | baseline | baseline | ok | ok | 1819 | 9.365e+04 |  |
| chronos2_lora | foundation_ts | candidate | ok | ok | 1819 | 2.886e+04 |  |
| chronos2_zero_shot | foundation_ts | candidate | ok | ok | 1819 | 2.887e+04 |  |
| timesfm25_zero_shot | foundation_ts | candidate | ok | ok | 1819 | 2.918e+04 |  |
| toto2_zero_shot | foundation_ts | candidate | ok | ok | 1819 | 2.931e+04 |  |
| ttm_r2_zero_shot | foundation_ts | candidate | ok | ok | 1819 | 3.486e+04 |  |

#### metrics (validation partition)

| metric | zero_correction | chronos2_lora | chronos2_zero_shot | timesfm25_zero_shot | toto2_zero_shot | ttm_r2_zero_shot |
|---|---|---|---|---|---|---|
| mae | 9.365e+04 | 2.886e+04 | 2.887e+04 | 2.918e+04 | 2.931e+04 | 3.486e+04 |
| rmse | 1.964e+05 | 5.001e+04 | 5.004e+04 | 5.109e+04 | 5.281e+04 | 6.313e+04 |
| mean_error | 7.85e+04 | 4052 | 4213 | 3203 | 2504 | -926.2 |
| mean_error_relative | 0.8383 | 0.04327 | 0.04498 | 0.0342 | 0.02673 | -0.00989 |
| pred_std_ratio | 0 | 0.9383 | 0.937 | 0.9479 | 0.9561 | 0.9564 |
| skill_mae_vs_scope_mean | 0.004228 |  |  |  |  |  |
| mae__forecast_scope__offshore_wind | 9038 |  |  |  |  |  |
| mae__forecast_scope__onshore_wind | 1.831e+04 |  |  |  |  |  |
| mae__forecast_scope__other | 4.291e+05 |  |  |  |  |  |
| mae__forecast_scope__photovoltaic | 1.198e+04 |  |  |  |  |  |
| mae__forecast_scope__total | 9.836e+04 |  |  |  |  |  |
| mae__forecast_scope__wind_and_photovoltaic | 2.71e+04 |  |  |  |  |  |
| skill_mae |  | 0.6918 | 0.6917 | 0.6884 | 0.687 | 0.6278 |
| skill_rmse |  | 0.7453 | 0.7452 | 0.7398 | 0.7311 | 0.6785 |
| forecast_items |  | 1819 | 1819 | 1819 | 1819 | 1819 |
| rows_without_context |  | 0 | 0 | 0 | 0 | 0 |
| context_rule_violations |  | 0 | 0 | 0 | 0 | 0 |

### zone_generation.generation

_dataset `zone_generation`, frozen version 0, primary metric `mae` (lower is better)_

#### data seen by the models

- frame 1: 7640 rows x 23 columns, sample fraction 1.0, rows by partition {'train': 4000, 'validation': 3640}, highest null rates {'fold_id': 0.6971, 'capacity_net_mw': 0.3995}
- target, train: rows 4000, mean 4.949e+04, std 5.651e+04, min 48.5, max 3.929e+05
- target, validation: rows 3640, mean 5.498e+04, std 6.022e+04, min 21.5, max 4.382e+05

#### candidates

| model | family | stage | status | artifact | rows | mae | detail |
|---|---|---|---|---|---|---|---|
| capacity_factor | baseline | baseline | ok | ok | 3640 | 2.599e+04 |  |
| chronos2_lora | foundation_ts | candidate | ok | ok | 3640 | 2.314e+04 |  |
| chronos2_zero_shot | foundation_ts | candidate | ok | ok | 3640 | 2.314e+04 |  |
| timesfm25_zero_shot | foundation_ts | candidate | ok | ok | 3640 | 2.302e+04 |  |
| toto2_zero_shot | foundation_ts | candidate | ok | ok | 3640 | 2.375e+04 |  |
| ttm_r2_zero_shot | foundation_ts | candidate | ok | ok | 3640 | 2.402e+04 |  |

#### metrics (validation partition)

| metric | capacity_factor | chronos2_lora | chronos2_zero_shot | timesfm25_zero_shot | toto2_zero_shot | ttm_r2_zero_shot |
|---|---|---|---|---|---|---|
| mae | 2.599e+04 | 2.314e+04 | 2.314e+04 | 2.302e+04 | 2.375e+04 | 2.402e+04 |
| rmse | 4.193e+04 | 3.879e+04 | 3.88e+04 | 3.891e+04 | 4.072e+04 | 3.92e+04 |
| mean_error | -671.5 | -1106 | -1126 | -3134 | -3593 | 529.6 |
| mean_error_relative | -0.01222 | -0.02012 | -0.02048 | -0.05702 | -0.06535 | 0.009634 |
| pred_std_ratio | 0.8145 | 0.7881 | 0.7887 | 0.7658 | 0.7305 | 0.7695 |
| skill_mae |  | 0.1099 | 0.1099 | 0.1144 | 0.08631 | 0.07611 |
| skill_rmse |  | 0.07485 | 0.07475 | 0.07214 | 0.02884 | 0.06506 |
| forecast_items |  | 3640 | 3640 | 3640 | 3640 | 3640 |
| rows_without_context |  | 0 | 0 | 0 | 0 | 0 |
| context_rule_violations |  | 0 | 0 | 0 | 0 | 0 |

### honda_forecast.increment

_dataset `honda_forecast`, frozen version 0, primary metric `mae` (lower is better)_

#### data seen by the models

- frame 1: 8000 rows x 22 columns, sample fraction 1.0, rows by partition {'train': 4000, 'validation': 4000}, highest null rates {'fold_id': 0.6241, 'increment_lag2d': 0.0011}
- target, train: rows 4000, mean 71.89, std 83.87, min -261.1, max 643.5
- target, validation: rows 4000, mean 13.08, std 78.93, min -494.1, max 426.2

#### candidates

| model | family | stage | status | artifact | rows | mae | detail |
|---|---|---|---|---|---|---|---|
| persistence | baseline | baseline | ok | ok | 4000 | 23.72 |  |
| chronos2_lora | foundation_ts | candidate | ok | ok | 4000 | 18.95 |  |
| chronos2_zero_shot | foundation_ts | candidate | ok | ok | 4000 | 18.96 |  |
| timesfm25_zero_shot | foundation_ts | candidate | ok | ok | 4000 | 19.55 |  |
| toto2_zero_shot | foundation_ts | candidate | ok | ok | 4000 | 19.62 |  |
| ttm_r2_zero_shot | foundation_ts | candidate | ok | ok | 4000 | 21.11 |  |

#### metrics (validation partition)

| metric | persistence | chronos2_lora | chronos2_zero_shot | timesfm25_zero_shot | toto2_zero_shot | ttm_r2_zero_shot |
|---|---|---|---|---|---|---|
| mae | 23.72 | 18.95 | 18.96 | 19.55 | 19.62 | 21.11 |
| rmse | 49.61 | 39.18 | 39.18 | 39.05 | 39.65 | 39.39 |
| mean_error | -0.3695 | -1.161 | -1.137 | -1.398 | -0.5007 | -0.6402 |
| mean_error_relative | -0.008132 | -0.02556 | -0.02502 | -0.03078 | -0.01102 | -0.01409 |
| pred_std_ratio | 0.9969 | 0.941 | 0.941 | 0.93 | 0.9472 | 0.8421 |
| skill_mae |  | 0.2011 | 0.2006 | 0.176 | 0.1729 | 0.1102 |
| skill_rmse |  | 0.2102 | 0.2101 | 0.2127 | 0.2008 | 0.206 |
| forecast_items |  | 1584 | 1584 | 1584 | 1584 | 1584 |
| rows_without_context |  | 0 | 0 | 0 | 0 | 0 |
| context_rule_violations |  | 0 | 0 | 0 | 0 | 0 |

<!-- section: foundation_energy__02_foundation_tab_energy -->
## Foundation Tabular Energy

### summary

| task | foundation candidates | ok | failed or skipped |
|---|---|---|---|
| price_quarter_hour.negative_price | 3 | 3 | 0 |
| ccpp.output | 3 | 3 | 0 |
| capacity_additions.additions | 3 | 3 | 0 |
| redispatch.any_event | 3 | 3 | 0 |
| redispatch.event_energy | 3 | 3 | 0 |

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
| capacity_additions.additions | tabpfn_25_in_context | biased predictions | mean error is -40% of the mean absolute target |
| capacity_additions.additions | tabpfn_v2_in_context | biased predictions | mean error is -45% of the mean absolute target |
| capacity_additions.additions | tabicl_v2_in_context | no skill over the baseline | skill_mae = -2.793 |
| capacity_additions.additions | tabicl_v2_in_context | biased predictions | mean error is +284% of the mean absolute target |

### price_quarter_hour.negative_price

_dataset `price_quarter_hour`, frozen version 0, primary metric `pr_auc` (higher is better)_

#### data seen by the models

- frame 1: 8000 rows x 44 columns, sample fraction 1.0, rows by partition {'train': 4000, 'validation': 4000}, highest null rates {'fold_id': 0.7362}
- target, train: rows 4000, positive_rate 0.06
- target, validation: rows 4000, positive_rate 0.041

#### candidates

| model | family | stage | status | artifact | rows | pr_auc | detail |
|---|---|---|---|---|---|---|---|
| base_rate | baseline | baseline | ok | ok | 4000 | 0.041 |  |
| tabicl_v2_in_context | foundation_tab | candidate | ok | ok | 4000 | 0.5385 |  |
| tabpfn_25_in_context | foundation_tab | candidate | ok | ok | 4000 | 0.633 |  |
| tabpfn_v2_in_context | foundation_tab | candidate | ok | ok | 4000 | 0.5762 |  |

#### metrics (validation partition)

| metric | base_rate | tabicl_v2_in_context | tabpfn_25_in_context | tabpfn_v2_in_context |
|---|---|---|---|---|
| pr_auc | 0.041 | 0.5385 | 0.633 | 0.5762 |
| log_loss | 0.1747 | 0.0943 | 0.08315 | 0.08886 |
| calibration_error | 0.019 | 0.01754 | 0.01784 | 0.01255 |
| base_rate | 0.041 | 0.041 | 0.041 | 0.041 |
| pred_mean | 0.06 | 0.04854 | 0.05805 | 0.05242 |
| pred_positive_share | 0 | 0.03575 | 0.04825 | 0.03975 |
| pr_auc_lift | 1 | 13.13 | 15.44 | 14.05 |

### ccpp.output

_dataset `ccpp`, frozen version 0, primary metric `mae` (lower is better)_

#### data seen by the models

- frame 1: 5369 rows x 8 columns, sample fraction 1.0, rows by partition {'train': 4000, 'validation': 1369}, highest null rates {'fold_id': 0.255}
- target, train: rows 4000, mean 465.1, std 12.86, min 430.4, max 495.8
- target, validation: rows 1369, mean 455, std 17, min 421.6, max 494.8

#### candidates

| model | family | stage | status | artifact | rows | mae | detail |
|---|---|---|---|---|---|---|---|
| train_mean | baseline | baseline | ok | ok | 1369 | 16.75 |  |
| tabicl_v2_in_context | foundation_tab | candidate | ok | ok | 1369 | 3.087 |  |
| tabpfn_25_in_context | foundation_tab | candidate | ok | ok | 1369 | 2.751 |  |
| tabpfn_v2_in_context | foundation_tab | candidate | ok | ok | 1369 | 2.522 |  |

#### metrics (validation partition)

| metric | train_mean | tabicl_v2_in_context | tabpfn_25_in_context | tabpfn_v2_in_context |
|---|---|---|---|---|
| mae | 16.75 | 3.087 | 2.751 | 2.522 |
| rmse | 19.78 | 4.673 | 3.973 | 3.658 |
| mean_error | 10.11 | 1.161 | 0.9699 | -0.2657 |
| mean_error_relative | 0.02222 | 0.002552 | 0.002132 | -0.000584 |
| pred_std_ratio | 1.003e-14 | 0.902 | 0.9265 | 0.9991 |
| skill_mae |  | 0.8157 | 0.8358 | 0.8494 |
| skill_rmse |  | 0.7637 | 0.7991 | 0.815 |

### capacity_additions.additions

_dataset `capacity_additions`, frozen version 0, primary metric `mae` (lower is better)_

#### data seen by the models

- frame 1: 1161 rows x 7 columns, sample fraction 1.0, rows by partition {'train': 972, 'validation': 189}, highest null rates {'fold_id': 0.528}
- target, train: rows 972, mean 25.04, std 81.82, min 0.0006, max 1052
- target, validation: rows 189, mean 26.48, std 87.47, min 0.0038, max 481.2

#### candidates

| model | family | stage | status | artifact | rows | mae | detail |
|---|---|---|---|---|---|---|---|
| seasonal_mean | baseline | baseline | ok | ok | 189 | 20.47 |  |
| tabicl_v2_in_context | foundation_tab | candidate | ok | ok | 189 | 77.63 |  |
| tabpfn_25_in_context | foundation_tab | candidate | ok | ok | 189 | 14.1 |  |
| tabpfn_v2_in_context | foundation_tab | candidate | ok | ok | 189 | 16.13 |  |

#### metrics (validation partition)

| metric | seasonal_mean | tabicl_v2_in_context | tabpfn_25_in_context | tabpfn_v2_in_context |
|---|---|---|---|---|
| mae | 20.47 | 77.63 | 14.1 | 16.13 |
| rmse | 55.44 | 111 | 48.52 | 54.56 |
| mean_error | -1.284 | 75.1 | -10.47 | -11.84 |
| mean_error_relative | -0.0485 | 2.836 | -0.3953 | -0.4472 |
| pred_std_ratio | 0.6675 | 1.486 | 0.5349 | 0.4603 |
| skill_mae |  | -2.793 | 0.3113 | 0.2119 |
| skill_rmse |  | -1.002 | 0.1248 | 0.01587 |

### redispatch.any_event

_dataset `redispatch`, frozen version 2, primary metric `pr_auc` (higher is better)_

#### data seen by the models

- frame 1: 5452 rows x 32 columns, sample fraction 1.0, rows by partition {'train': 4000, 'validation': 1452}, highest null rates {'fold_id': 0.6346}
- target, train: rows 4000, positive_rate 0.525
- target, validation: rows 1452, positive_rate 0.6749

#### candidates

| model | family | stage | status | artifact | rows | pr_auc | detail |
|---|---|---|---|---|---|---|---|
| base_rate | baseline | baseline | ok | ok | 1452 | 0.6749 |  |
| tabicl_v2_in_context | foundation_tab | candidate | ok | ok | 1452 | 0.8685 |  |
| tabpfn_25_in_context | foundation_tab | candidate | ok | ok | 1452 | 0.8746 |  |
| tabpfn_v2_in_context | foundation_tab | candidate | ok | ok | 1452 | 0.8784 |  |

#### metrics (validation partition)

| metric | base_rate | tabicl_v2_in_context | tabpfn_25_in_context | tabpfn_v2_in_context |
|---|---|---|---|---|
| pr_auc | 0.6749 | 0.8685 | 0.8746 | 0.8784 |
| log_loss | 0.6769 | 0.5605 | 0.5272 | 0.5225 |
| calibration_error | 0.1499 | 0.1124 | 0.04456 | 0.0487 |
| base_rate | 0.6749 | 0.6749 | 0.6749 | 0.6749 |
| pred_mean | 0.525 | 0.5625 | 0.6407 | 0.6497 |
| pred_positive_share | 1 | 0.6219 | 0.7328 | 0.7417 |
| pr_auc_lift | 1 | 1.287 | 1.296 | 1.301 |

### redispatch.event_energy

_dataset `redispatch`, frozen version 2, primary metric `mae` (lower is better)_

#### data seen by the models

- frame 1: 3063 rows x 32 columns, sample fraction 1.0, rows by partition {'train': 2093, 'validation': 970}, highest null rates {'fold_id': 0.6249}
- target, train: rows 2093, mean 8.395, std 1.541, min 2.773, max 11.6
- target, validation: rows 970, mean 8.414, std 1.427, min 1.946, max 11.28

#### candidates

| model | family | stage | status | artifact | rows | mae | detail |
|---|---|---|---|---|---|---|---|
| zone_mean | baseline | baseline | ok | ok | 970 | 1.134 |  |
| tabicl_v2_in_context | foundation_tab | candidate | ok | ok | 970 | 1.012 |  |
| tabpfn_25_in_context | foundation_tab | candidate | ok | ok | 970 | 1.025 |  |
| tabpfn_v2_in_context | foundation_tab | candidate | ok | ok | 970 | 1.053 |  |

#### metrics (validation partition)

| metric | zone_mean | tabicl_v2_in_context | tabpfn_25_in_context | tabpfn_v2_in_context |
|---|---|---|---|---|
| mae | 1.134 | 1.012 | 1.025 | 1.053 |
| rmse | 1.428 | 1.355 | 1.401 | 1.416 |
| mean_error | -0.05057 | 0.1528 | 0.3311 | 0.268 |
| mean_error_relative | -0.00601 | 0.01816 | 0.03935 | 0.03185 |
| pred_std_ratio | 0.2196 | 0.4281 | 0.4582 | 0.4661 |
| skill_mae |  | 0.1073 | 0.09558 | 0.07116 |
| skill_rmse |  | 0.05108 | 0.01879 | 0.008111 |

<!-- section: foundation_energy__04_foundation_anomaly_energy -->
## Foundation Anomaly Energy

### summary

| task | foundation candidates | ok | failed or skipped |
|---|---|---|---|
| honda_anomaly.anomaly | 2 | 2 | 0 |
| weather_imputation.reconstruction | 2 | 2 | 0 |

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
| weather_imputation.reconstruction | causal_baseline | no candidate beats the baseline | skill_mae_mean |

### honda_anomaly.anomaly

_dataset `honda_anomaly`, frozen version 0, primary metric `pr_auc` (higher is better)_

#### data seen by the models

- frame 1: 8000 rows x 19 columns, sample fraction 1.0, rows by partition {'train': 4000, 'validation': 4000}, highest null rates none

#### candidates

| model | family | stage | status | artifact | rows | pr_auc | detail |
|---|---|---|---|---|---|---|---|
| seasonal_zscore | baseline | baseline | ok | ok | 4000 | 0.2192 |  |
| energy_tspulse_zero_shot | foundation_recon | candidate | ok | ok | 4000 | 0.2789 |  |
| moment_zero_shot | foundation_recon | candidate | ok | ok | 4000 | 0.1395 |  |

#### metrics (validation partition)

| metric | seasonal_zscore | energy_tspulse_zero_shot | moment_zero_shot |
|---|---|---|---|
| precision | 0.2012 | 0.6111 | 0.4667 |
| recall | 0.7462 | 0.08462 | 0.05385 |
| flag_rate | 0.09775 | 0.00425 | 0.00675 |
| pr_auc | 0.2192 | 0.2789 | 0.1395 |
| threshold | 2.247 | 0.3635 | 2.916 |
| train_flag_rate | 0.01 | 0.01 | 0.01 |

### weather_imputation.reconstruction

_dataset `weather_imputation`, frozen version 2, primary metric `skill_mae_mean` (higher is better)_

#### data seen by the models

- frame 1: 218814 rows x 13 columns, sample fraction 1.0, rows by partition {'train': 183898, 'validation': 34916}, highest null rates {'soil_temperature': 0.2512, 'pressure_sea_level': 0.2508, 'visibility': 0.0192, 'wind_speed': 0.01, 'wind_direction': 0.0089}

#### candidates

| model | family | stage | status | artifact | rows | skill_mae_mean | detail |
|---|---|---|---|---|---|---|---|
| causal_baseline | baseline | baseline | ok | ok | 10 | 0 |  |
| energy_tspulse_zero_shot | foundation_recon | candidate | ok | ok | 10 | -0.3095 |  |
| moment_zero_shot | foundation_recon | candidate | ok | ok | 10 | -0.1189 |  |

#### metrics (validation partition)

| metric | causal_baseline | energy_tspulse_zero_shot | moment_zero_shot |
|---|---|---|---|
| mae__weather__air_temperature | 3.322 | 4.588 | 3.518 |
| skill_mae__weather__air_temperature | 0 | -0.3813 | -0.05902 |
| mae_heldout__weather__air_temperature |  |  |  |
| mae__weather__dew_point_temperature | 2.222 | 2.957 | 2.774 |
| skill_mae__weather__dew_point_temperature | 0 | -0.3304 | -0.248 |
| mae_heldout__weather__dew_point_temperature |  |  |  |
| mae__weather__relative_humidity | 13.52 | 11.6 | 12.01 |
| skill_mae__weather__relative_humidity | 0 | 0.1418 | 0.1114 |
| mae_heldout__weather__relative_humidity |  |  |  |
| mae__weather__pressure_station | 3.553 | 6.644 | 4.415 |
| skill_mae__weather__pressure_station | 0 | -0.87 | -0.2426 |
| mae_heldout__weather__pressure_station |  |  |  |
| mae__weather__pressure_sea_level | 4.389 | 8.758 | 6.757 |
| skill_mae__weather__pressure_sea_level | 0 | -0.9952 | -0.5394 |
| mae_heldout__weather__pressure_sea_level |  |  |  |
| mae__weather__wind_speed | 1.662 | 1.669 | 1.718 |
| skill_mae__weather__wind_speed | 0 | -0.003994 | -0.03341 |
| mae_heldout__weather__wind_speed |  |  |  |
| mae__weather__wind_direction | 54.47 | 65.21 | 55.39 |
| skill_mae__weather__wind_direction | 0 | -0.197 | -0.01691 |
| mae_heldout__weather__wind_direction |  |  |  |
| mae__weather__cloud_cover | 26.94 | 30.78 | 27.1 |
| skill_mae__weather__cloud_cover | 0 | -0.1427 | -0.006071 |
| mae_heldout__weather__cloud_cover |  |  |  |
| mae__weather__visibility | 1.624e+04 | 1.991e+04 | 1.769e+04 |
| skill_mae__weather__visibility | 0 | -0.2259 | -0.0891 |
| mae_heldout__weather__visibility |  |  |  |
| mae__weather__soil_temperature | 3.306 | 3.605 | 3.523 |
| skill_mae__weather__soil_temperature | 0 | -0.09038 | -0.06565 |
| mae_heldout__weather__soil_temperature |  |  |  |
| skill_mae_mean | 0 | -0.3095 | -0.1189 |

<!-- section: foundation_energy__05_foundation_survival_matching_energy -->
## Foundation Survival And Matching Energy

### summary

| task | foundation candidates | ok | failed or skipped |
|---|---|---|---|
| survival.unit_lifetime | 2 | 0 | 2 |
| redispatch_matching.tier | 3 | 3 | 0 |

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
| survival.unit_lifetime | tabicl_discrete_in_context | failed | TimeoutError: tabicl_discrete_in_context survival.unit_lifetime: timeout after 1800 s |
| survival.unit_lifetime | survpfn_in_context | failed | ModuleNotFoundError: No module named 'pfns' |
| redispatch_matching.tier |  | fewer than two classes estimable in the evaluation | the primary metric rests on a single class |
| redispatch_matching.tier |  | identical primary metric across models | balanced_accuracy repeats on 3 or more models |

### survival.unit_lifetime

_dataset `survival`, frozen version 0, primary metric `concordance` (higher is better)_

#### data seen by the models

- frame 1: 8000 rows x 21 columns, sample fraction 1.0, rows by partition {'train': 4000, 'validation': 4000}, highest null rates none

#### candidates

| model | family | stage | status | artifact | rows | concordance | detail |
|---|---|---|---|---|---|---|---|
| kaplan_meier | baseline | baseline | ok | ok | 4000 | 0.5 |  |
| survpfn_in_context | foundation_surv | candidate | failed | none |  |  | ModuleNotFoundError: No module named 'pfns' |
| tabicl_discrete_in_context | foundation_surv | candidate | failed | none |  |  | TimeoutError: tabicl_discrete_in_context survival.unit_lifetime: timeout after 1800 s |

#### metrics (validation partition)

| metric | kaplan_meier |
|---|---|
| concordance | 0.5 |
| brier_1y | 0 |
| calibration_gap_1y | 0 |
| brier_3y | 0.0002504 |
| calibration_gap_3y | 0.0002504 |
| brier_5y | 0.0005074 |
| calibration_gap_5y | 0.0002433 |

### redispatch_matching.tier

_dataset `redispatch_matching`, frozen version 2, primary metric `balanced_accuracy` (higher is better)_

#### data seen by the models

- frame 1: 576 rows x 12 columns, sample fraction 1.0, rows by partition {'train': 431, 'validation': 145}, highest null rates none

#### candidates

| model | family | stage | status | artifact | rows | balanced_accuracy | detail |
|---|---|---|---|---|---|---|---|
| majority_tier | baseline | baseline | ok | ok | 145 | 0.5 |  |
| tabicl_v2_in_context | foundation_tab | candidate | ok | ok | 145 | 0.6215 |  |
| tabpfn_25_in_context | foundation_tab | candidate | ok | ok | 145 | 0.5 |  |
| tabpfn_v2_in_context | foundation_tab | candidate | ok | ok | 145 | 0.5 |  |

#### metrics (validation partition)

| metric | majority_tier | tabicl_v2_in_context | tabpfn_25_in_context | tabpfn_v2_in_context |
|---|---|---|---|---|
| accuracy | 0.9724 | 0.9724 | 0.9724 | 0.9724 |
| precision__exact_normalised |  |  |  |  |
| recall__exact_normalised |  |  |  |  |
| n_true__exact_normalised | 4 | 4 | 4 | 4 |
| estimable__exact_normalised | 0 | 0 | 0 | 0 |
| precision__unmatched | 0.9724 | 0.979 | 0.9724 | 0.9724 |
| recall__unmatched | 1 | 0.9929 | 1 | 1 |
| n_true__unmatched | 141 | 141 | 141 | 141 |
| estimable__unmatched | 1 | 1 | 1 | 1 |
| estimable_tiers | 1 | 1 | 1 | 1 |
| balanced_accuracy | 0.5 | 0.6215 | 0.5 | 0.5 |
| train_accuracy | 0.9698 |  |  |  |
| train_balanced_accuracy | 0.5 |  |  |  |

<!-- section: foundation_energy__06_foundation_rl_energy -->
## Foundation Policy Energy

### summary

| task | foundation candidates | ok | failed or skipped |
|---|---|---|---|
| rl_pumped_storage.policy | 1 | 1 | 0 |
| rl_redispatch.policy | 1 | 1 | 0 |

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

### rl_pumped_storage.policy

_dataset `rl_pumped_storage`, frozen version 2, primary metric `reward_timing` (higher is better)_

#### data seen by the models

- frame 1: 8000 rows x 10 columns, sample fraction 1.0, rows by partition {'train': 4000, 'validation': 4000}, highest null rates none

#### candidates

| model | family | stage | status | artifact | rows | reward_timing | detail |
|---|---|---|---|---|---|---|---|
| rule_policy | baseline | baseline | ok | ok | 4000 | 1.142e+04 |  |
| decision_transformer | offline_rl | candidate | ok | ok | 4000 | 2.078e+04 |  |

#### metrics (validation partition)

| metric | rule_policy | decision_transformer |
|---|---|---|
| action_mae | 477.9 | 432.2 |
| reward_policy | 4778 | 3.346e+04 |
| reward_timing | 1.142e+04 | 2.078e+04 |
| reward_logged | 1.315e+04 | 1.315e+04 |
| sign_agreement | 0.7718 | 0.7295 |
| reward_gain | -8371 | 2.031e+04 |
| reward_timing_gain | -1734 | 7627 |
| episode_net_abs_policy | 5223 | 6468 |
| episode_net_abs_logged | 3467 | 3467 |
| train_action_mae | 565.2 | 471.3 |
| train_sign_agreement | 0.7412 | 0.74 |

### rl_redispatch.policy

_dataset `rl_redispatch`, frozen version 2, primary metric `action_agreement` (higher is better)_

#### data seen by the models

- frame 1: 8000 rows x 33 columns, sample fraction 1.0, rows by partition {'train': 4000, 'validation': 4000}, highest null rates none

#### candidates

| model | family | stage | status | artifact | rows | action_agreement | detail |
|---|---|---|---|---|---|---|---|
| majority_action | baseline | baseline | ok | ok | 4000 | 0.419 |  |
| decision_transformer | offline_rl | candidate | ok | ok | 4000 | 0.6542 |  |

#### metrics (validation partition)

| metric | majority_action | decision_transformer |
|---|---|---|
| action_agreement | 0.419 | 0.6542 |
| reward_when_agree | -1871 | -1949 |
| reward_when_disagree | -1770 | -1553 |
| reward_weighted_agreement | 0.4326 | 0.7038 |
| train_action_agreement | 0.6242 | 0.811 |

<!-- section: foundation_energy__08_foundation_stacking_energy -->
## Foundation Stacking Energy

### summary

| task | foundation candidates | ok | failed or skipped |
|---|---|---|---|
| price_daily.price | 2 | 2 | 0 |
| price_quarter_hour.price | 2 | 2 | 0 |
| load.load | 2 | 2 | 0 |
| bias.bias | 2 | 2 | 0 |
| zone_generation.generation | 2 | 2 | 0 |
| honda_forecast.increment | 2 | 2 | 0 |

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
| bias.bias | lgbm_stack_chronos2_zero_shot | no skill over the baseline | skill_mae = -0.006663 |
| bias.bias | lgbm_stack_chronos2_zero_shot | biased predictions | mean error is +84% of the mean absolute target |
| bias.bias | lgbm_stack_chronos2_zero_shot | near-constant predictions | prediction spread is 0.057 of the target spread |
| bias.bias | hgb_stack_chronos2_zero_shot | no skill over the baseline | skill_mae = -0.00443 |
| bias.bias | hgb_stack_chronos2_zero_shot | biased predictions | mean error is +84% of the mean absolute target |
| bias.bias | hgb_stack_chronos2_zero_shot | near-constant predictions | prediction spread is 0.059 of the target spread |
| bias.bias | zero_correction | biased predictions | mean error is +84% of the mean absolute target |
| bias.bias | zero_correction | no candidate beats the baseline | mae |
| honda_forecast.increment | lgbm_stack_chronos2_zero_shot | no skill over the baseline | skill_mae = -0.1447 |
| honda_forecast.increment | hgb_stack_chronos2_zero_shot | no skill over the baseline | skill_mae = -0.1438 |
| honda_forecast.increment | persistence | no candidate beats the baseline | mae |

### price_daily.price

_dataset `price_daily`, frozen version 0, primary metric `mae` (lower is better)_

#### data seen by the models

- frame 1: 2280 rows x 90 columns, sample fraction 1.0, rows by partition {'train': 1916, 'validation': 364}, highest null rates {'fold_id': 0.5206, 'price_eur_per_mwh_lag7': 0.0031}
- target, train: rows 1916, mean 96.93, std 97.77, min -53.87, max 699.4
- target, validation: rows 364, mean 78.55, std 36.43, min 1.78, max 395.3

#### candidates

| model | family | stage | status | artifact | rows | mae | detail |
|---|---|---|---|---|---|---|---|
| empirical_quantiles | baseline | baseline | ok | ok | 364 |  |  |
| seasonal_naive | baseline | baseline | ok | ok | 364 | 28.04 |  |
| hgb_stack_chronos2_zero_shot | foundation_stack | candidate | ok | ok | 364 | 11.66 |  |
| lgbm_stack_chronos2_zero_shot | foundation_stack | candidate | ok | ok | 364 | 11.83 |  |

#### metrics (validation partition)

| metric | empirical_quantiles | seasonal_naive | hgb_stack_chronos2_zero_shot | lgbm_stack_chronos2_zero_shot |
|---|---|---|---|---|
| pinball_q10 | 5.803 |  |  |  |
| pinball_q50 | 14.58 | 14.02 | 5.832 | 5.913 |
| pinball_q90 | 16.8 |  |  |  |
| interval_coverage_80 | 0.9313 |  |  |  |
| mae |  | 28.04 | 11.66 | 11.83 |
| rmse |  | 42.9 | 20.17 | 20.17 |
| mean_error |  | -1.588 | -5.979 | -5.831 |
| mean_error_relative |  | -0.02021 | -0.07612 | -0.07423 |
| pred_std_ratio |  | 1.018 | 0.9108 | 0.9029 |
| skill_mae |  |  | 0.5841 | 0.5783 |
| skill_rmse |  |  | 0.5298 | 0.5299 |
| forecast_items |  |  | 364 | 364 |
| rows_without_context |  |  | 0 | 0 |
| context_rule_violations |  |  | 0 | 0 |

### price_quarter_hour.price

_dataset `price_quarter_hour`, frozen version 0, primary metric `mae` (lower is better)_

#### data seen by the models

- frame 1: 8000 rows x 45 columns, sample fraction 1.0, rows by partition {'train': 4000, 'validation': 4000}, highest null rates {'fold_id': 0.7362, 'price_eur_per_mwh_lag2d': 0.0041}
- target, train: rows 4000, mean 94, std 60.58, min -231, max 805.1
- target, validation: rows 4000, mean 87.66, std 48.12, min -61.08, max 493.9

#### candidates

| model | family | stage | status | artifact | rows | mae | detail |
|---|---|---|---|---|---|---|---|
| empirical_quantiles | baseline | baseline | ok | ok | 4000 |  |  |
| persistence | baseline | baseline | ok | ok | 4000 | 33.14 |  |
| hgb_stack_chronos2_zero_shot | foundation_stack | candidate | ok | ok | 4000 | 19.04 |  |
| lgbm_stack_chronos2_zero_shot | foundation_stack | candidate | ok | ok | 4000 | 18.99 |  |

#### metrics (validation partition)

| metric | empirical_quantiles | persistence | hgb_stack_chronos2_zero_shot | lgbm_stack_chronos2_zero_shot |
|---|---|---|---|---|
| pinball_q10 | 7.947 |  |  |  |
| pinball_q50 | 15.71 | 16.57 | 9.52 | 9.495 |
| pinball_q90 | 9.624 |  |  |  |
| interval_coverage_80 | 0.8315 |  |  |  |
| mae |  | 33.14 | 19.04 | 18.99 |
| rmse |  | 51.39 | 28.52 | 28.2 |
| mean_error |  | -1.051 | -4.226 | -4.086 |
| mean_error_relative |  | -0.01195 | -0.04806 | -0.04647 |
| pred_std_ratio |  | 0.9641 | 0.9756 | 0.9748 |
| skill_mae |  |  | 0.4255 | 0.427 |
| skill_rmse |  |  | 0.445 | 0.4512 |
| forecast_items |  |  | 183 | 183 |
| rows_without_context |  |  | 0 | 0 |
| context_rule_violations |  |  | 0 | 0 |

### load.load

_dataset `load`, frozen version 0, primary metric `mae` (lower is better)_

#### data seen by the models

- frame 1: 6184 rows x 101 columns, sample fraction 1.0, rows by partition {'train': 4000, 'validation': 2184}, highest null rates {'fold_id': 0.6276}
- target, train: rows 4000, mean 6.009e+05, std 4.302e+05, min 1.014e+05, max 1.682e+06
- target, validation: rows 2184, mean 5.485e+05, std 3.941e+05, min 1.035e+05, max 1.611e+06

#### candidates

| model | family | stage | status | artifact | rows | mae | detail |
|---|---|---|---|---|---|---|---|
| seasonal_mean | baseline | baseline | ok | ok | 2184 | 5.881e+04 |  |
| hgb_stack_chronos2_zero_shot | foundation_stack | candidate | ok | ok | 2184 | 2.413e+04 |  |
| lgbm_stack_chronos2_zero_shot | foundation_stack | candidate | ok | ok | 2184 | 2.415e+04 |  |

#### metrics (validation partition)

| metric | seasonal_mean | hgb_stack_chronos2_zero_shot | lgbm_stack_chronos2_zero_shot |
|---|---|---|---|
| mae | 5.881e+04 | 2.413e+04 | 2.415e+04 |
| rmse | 1.128e+05 | 4.278e+04 | 4.294e+04 |
| mean_error | 4.129e+04 | -7218 | -6843 |
| mean_error_relative | 0.07527 | -0.01316 | -0.01248 |
| pred_std_ratio | 1.045 | 0.9828 | 0.9855 |
| skill_mae |  | 0.5897 | 0.5893 |
| skill_rmse |  | 0.6209 | 0.6194 |
| forecast_items |  | 2184 | 2184 |
| rows_without_context |  | 0 | 0 |
| context_rule_violations |  | 0 | 0 |

### bias.bias

_dataset `bias`, frozen version 0, primary metric `mae` (lower is better)_

#### data seen by the models

- frame 1: 5819 rows x 91 columns, sample fraction 1.0, rows by partition {'train': 4000, 'validation': 1819}, highest null rates {'fold_id': 0.6087}
- target, train: rows 4000, mean 1605, std 1.981e+04, min -1.792e+05, max 1.105e+05
- target, validation: rows 1819, mean -7.85e+04, std 1.8e+05, min -8.85e+05, max 1.91e+05

#### candidates

| model | family | stage | status | artifact | rows | mae | detail |
|---|---|---|---|---|---|---|---|
| zero_correction | baseline | baseline | ok | ok | 1819 | 9.365e+04 |  |
| hgb_stack_chronos2_zero_shot | foundation_stack | candidate | ok | ok | 1819 | 9.406e+04 |  |
| lgbm_stack_chronos2_zero_shot | foundation_stack | candidate | ok | ok | 1819 | 9.427e+04 |  |

#### metrics (validation partition)

| metric | zero_correction | hgb_stack_chronos2_zero_shot | lgbm_stack_chronos2_zero_shot |
|---|---|---|---|
| mae | 9.365e+04 | 9.406e+04 | 9.427e+04 |
| rmse | 1.964e+05 | 1.977e+05 | 1.983e+05 |
| mean_error | 7.85e+04 | 7.83e+04 | 7.824e+04 |
| mean_error_relative | 0.8383 | 0.836 | 0.8355 |
| pred_std_ratio | 0 | 0.05858 | 0.05708 |
| skill_mae_vs_scope_mean | 0.004228 |  |  |
| mae__forecast_scope__offshore_wind | 9038 |  |  |
| mae__forecast_scope__onshore_wind | 1.831e+04 |  |  |
| mae__forecast_scope__other | 4.291e+05 |  |  |
| mae__forecast_scope__photovoltaic | 1.198e+04 |  |  |
| mae__forecast_scope__total | 9.836e+04 |  |  |
| mae__forecast_scope__wind_and_photovoltaic | 2.71e+04 |  |  |
| skill_mae |  | -0.00443 | -0.006663 |
| skill_rmse |  | -0.006552 | -0.009755 |
| forecast_items |  | 1819 | 1819 |
| rows_without_context |  | 0 | 0 |
| context_rule_violations |  | 0 | 0 |

### zone_generation.generation

_dataset `zone_generation`, frozen version 0, primary metric `mae` (lower is better)_

#### data seen by the models

- frame 1: 7640 rows x 23 columns, sample fraction 1.0, rows by partition {'train': 4000, 'validation': 3640}, highest null rates {'fold_id': 0.6971, 'capacity_net_mw': 0.3995}
- target, train: rows 4000, mean 4.949e+04, std 5.651e+04, min 48.5, max 3.929e+05
- target, validation: rows 3640, mean 5.498e+04, std 6.022e+04, min 21.5, max 4.382e+05

#### candidates

| model | family | stage | status | artifact | rows | mae | detail |
|---|---|---|---|---|---|---|---|
| capacity_factor | baseline | baseline | ok | ok | 3640 | 2.599e+04 |  |
| hgb_stack_chronos2_zero_shot | foundation_stack | candidate | ok | ok | 3640 | 1.19e+04 |  |
| lgbm_stack_chronos2_zero_shot | foundation_stack | candidate | ok | ok | 3640 | 1.215e+04 |  |

#### metrics (validation partition)

| metric | capacity_factor | hgb_stack_chronos2_zero_shot | lgbm_stack_chronos2_zero_shot |
|---|---|---|---|
| mae | 2.599e+04 | 1.19e+04 | 1.215e+04 |
| rmse | 4.193e+04 | 1.823e+04 | 1.894e+04 |
| mean_error | -671.5 | 2830 | 1998 |
| mean_error_relative | -0.01222 | 0.05148 | 0.03634 |
| pred_std_ratio | 0.8145 | 0.982 | 1 |
| skill_mae |  | 0.5422 | 0.5327 |
| skill_rmse |  | 0.5652 | 0.5482 |
| forecast_items |  | 3640 | 3640 |
| rows_without_context |  | 0 | 0 |
| context_rule_violations |  | 0 | 0 |

### honda_forecast.increment

_dataset `honda_forecast`, frozen version 0, primary metric `mae` (lower is better)_

#### data seen by the models

- frame 1: 8000 rows x 22 columns, sample fraction 1.0, rows by partition {'train': 4000, 'validation': 4000}, highest null rates {'fold_id': 0.6241, 'increment_lag2d': 0.0011}
- target, train: rows 4000, mean 71.89, std 83.87, min -261.1, max 643.5
- target, validation: rows 4000, mean 13.08, std 78.93, min -494.1, max 426.2

#### candidates

| model | family | stage | status | artifact | rows | mae | detail |
|---|---|---|---|---|---|---|---|
| persistence | baseline | baseline | ok | ok | 4000 | 23.72 |  |
| hgb_stack_chronos2_zero_shot | foundation_stack | candidate | ok | ok | 4000 | 27.13 |  |
| lgbm_stack_chronos2_zero_shot | foundation_stack | candidate | ok | ok | 4000 | 27.15 |  |

#### metrics (validation partition)

| metric | persistence | hgb_stack_chronos2_zero_shot | lgbm_stack_chronos2_zero_shot |
|---|---|---|---|
| mae | 23.72 | 27.13 | 27.15 |
| rmse | 49.61 | 45.73 | 45.85 |
| mean_error | -0.3695 | 8.69 | 9.203 |
| mean_error_relative | -0.008132 | 0.1913 | 0.2026 |
| pred_std_ratio | 0.9969 | 0.8524 | 0.8435 |
| skill_mae |  | -0.1438 | -0.1447 |
| skill_rmse |  | 0.07818 | 0.07566 |
| forecast_items |  | 1584 | 1584 |
| rows_without_context |  | 0 | 0 |
| context_rule_violations |  | 0 | 0 |
