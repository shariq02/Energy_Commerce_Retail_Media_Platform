# Catalog inventory: energy_commerce_retail_media

Generated: 2026-10-06T16:04:35Z

Size is the data files of the current table version (older versions kept for time travel are not counted). A view has no size.

## Overall

| schemas | tables | rows | files | size_bytes | size_gb |
| --- | --- | --- | --- | --- | --- |
| 21 | 330 | 1,867,517,095 | 1,772 | 69,396,838,114 | 64.631 |

## Per schema

| schema | tables | rows | files | size_bytes | size_mb | size_gb |
| --- | --- | --- | --- | --- | --- | --- |
| bronze | 55 | 338,682,835 | 141 | 3,248,704,476 | 3098.21 | 3.026 |
| commerce_analytics | 2 | 9,881 | 2 | 69,223 | 0.07 | 0.0 |
| commerce_gold | 10 | 143,431,474 | 41 | 1,407,419,136 | 1342.22 | 1.311 |
| commerce_ml_datasets | 42 | 873,783,424 | 482 | 27,379,811,756 | 26111.42 | 25.499 |
| commerce_ml_models | 20 | 1,552,586 | 109 | 24,676,783 | 23.53 | 0.023 |
| commerce_semantic | 1 | 3 | 1 | 4,260 | 0.0 | 0.0 |
| commerce_silver | 3 | 114,582,197 | 209 | 12,700,673,848 | 12112.31 | 11.828 |
| commerce_silver_reference | 0 | 0 | 0 | 0 | 0.0 | 0.0 |
| default | 0 | 0 | 0 | 0 | 0.0 | 0.0 |
| eda | 0 | 0 | 0 | 0 | 0.0 | 0.0 |
| energy_analytics | 5 | 138,871 | 5 | 946,395 | 0.9 | 0.001 |
| energy_gold | 30 | 169,733,068 | 211 | 10,268,599,394 | 9792.9 | 9.563 |
| energy_ml_datasets | 74 | 12,796,936 | 172 | 163,678,054 | 156.1 | 0.152 |
| energy_ml_models | 20 | 1,454,391 | 170 | 14,449,916 | 13.78 | 0.013 |
| energy_semantic | 1 | 5 | 1 | 4,468 | 0.0 | 0.0 |
| energy_silver | 34 | 179,010,526 | 131 | 13,102,224,761 | 12495.26 | 12.202 |
| energy_silver_reference | 15 | 24,109,525 | 34 | 816,801,064 | 778.96 | 0.761 |
| gold | 0 | 0 | 0 | 0 | 0.0 | 0.0 |
| quality | 4 | 1,887,512 | 41 | 39,129,809 | 37.32 | 0.036 |
| shared_conformed | 14 | 6,343,861 | 22 | 229,644,771 | 219.01 | 0.214 |
| silver | 0 | 0 | 0 | 0 | 0.0 | 0.0 |

## Per table

| schema | table | type | rows | files | size_bytes | size_mb | error |
| --- | --- | --- | --- | --- | --- | --- | --- |
| bronze | dwd_air_temperature | MANAGED | 17,336,388 | 1 | 42,040,783 | 40.09 |  |
| bronze | dwd_cloud_type | MANAGED | 13,101,463 | 1 | 48,541,877 | 46.29 |  |
| bronze | dwd_cloudiness | MANAGED | 12,973,316 | 8 | 10,905,182 | 10.4 |  |
| bronze | dwd_device_instrument | MANAGED | 4,008 | 1 | 32,485 | 0.03 |  |
| bronze | dwd_dew_point | MANAGED | 13,524,783 | 1 | 37,493,076 | 35.76 |  |
| bronze | dwd_extreme_wind | MANAGED | 6,480,621 | 4 | 8,735,436 | 8.33 |  |
| bronze | dwd_missing_value_periods | MANAGED | 5,897,409 | 7 | 21,224,003 | 20.24 |  |
| bronze | dwd_moisture | MANAGED | 12,449,858 | 2 | 99,508,690 | 94.9 |  |
| bronze | dwd_parameter_unit | MANAGED | 5,418 | 1 | 24,771 | 0.02 |  |
| bronze | dwd_precipitation | MANAGED | 7,154,301 | 5 | 5,527,700 | 5.27 |  |
| bronze | dwd_pressure | MANAGED | 13,227,922 | 1 | 34,549,225 | 32.95 |  |
| bronze | dwd_soil_temperature | MANAGED | 7,183,226 | 8 | 30,237,188 | 28.84 |  |
| bronze | dwd_solar | MANAGED | 5,809,249 | 8 | 24,926,282 | 23.77 |  |
| bronze | dwd_station_geography | MANAGED | 222 | 1 | 6,526 | 0.01 |  |
| bronze | dwd_station_name_history | MANAGED | 106 | 1 | 3,067 | 0.0 |  |
| bronze | dwd_sun | MANAGED | 12,650,564 | 8 | 10,055,692 | 9.59 |  |
| bronze | dwd_visibility | MANAGED | 13,124,582 | 1 | 20,084,903 | 19.15 |  |
| bronze | dwd_weather_phenomena | MANAGED | 12,915,288 | 1 | 17,912,130 | 17.08 |  |
| bronze | dwd_wind | MANAGED | 15,957,198 | 1 | 31,203,087 | 29.76 |  |
| bronze | dwd_wind_synop | MANAGED | 13,524,783 | 1 | 29,537,612 | 28.17 |  |
| bronze | ga4_events | MANAGED | 779,485 | 1 | 25,902,201 | 24.7 |  |
| bronze | honda_iot_cooling | MANAGED | 6,835,855 | 2 | 50,501,340 | 48.16 |  |
| bronze | honda_iot_electricity | MANAGED | 6,835,916 | 2 | 120,150,032 | 114.58 |  |
| bronze | honda_iot_heating | MANAGED | 6,835,906 | 2 | 51,072,493 | 48.71 |  |
| bronze | honda_iot_weather | MANAGED | 3,419,507 | 3 | 41,263,292 | 39.35 |  |
| bronze | mastr_anlagen_eeg_biomasse | MANAGED | 15,644 | 1 | 747,895 | 0.71 |  |
| bronze | mastr_anlagen_eeg_geothermie_gsgk | MANAGED | 132 | 1 | 10,498 | 0.01 |  |
| bronze | mastr_anlagen_eeg_wasser | MANAGED | 7,485 | 1 | 303,236 | 0.29 |  |
| bronze | mastr_anlagen_eeg_wind | MANAGED | 43,457 | 1 | 1,483,125 | 1.41 |  |
| bronze | mastr_anlagen_kwk | MANAGED | 92,514 | 1 | 2,726,455 | 2.6 |  |
| bronze | mastr_bilanzierungsgebiete | MANAGED | 1,118 | 1 | 17,096 | 0.02 |  |
| bronze | mastr_code_lookup | MANAGED | 52 | 4 | 6,336 | 0.01 |  |
| bronze | mastr_einheiten_aenderung_netzbetreiberzuordnungen | MANAGED | 226,405 | 1 | 5,739,794 | 5.47 |  |
| bronze | mastr_einheiten_biomasse | MANAGED | 24,334 | 1 | 2,302,674 | 2.2 |  |
| bronze | mastr_einheiten_genehmigung | MANAGED | 37,722 | 1 | 1,679,203 | 1.6 |  |
| bronze | mastr_einheiten_geothermie_gsgk | MANAGED | 336 | 1 | 48,501 | 0.05 |  |
| bronze | mastr_einheiten_kernkraft | MANAGED | 6 | 1 | 13,789 | 0.01 |  |
| bronze | mastr_einheiten_verbrennung | MANAGED | 94,027 | 1 | 6,779,898 | 6.47 |  |
| bronze | mastr_einheiten_wasser | MANAGED | 8,814 | 1 | 798,504 | 0.76 |  |
| bronze | mastr_einheiten_wind | MANAGED | 43,457 | 1 | 3,733,949 | 3.56 |  |
| bronze | mastr_ertuechtigungen | MANAGED | 1,681 | 1 | 33,971 | 0.03 |  |
| bronze | mastr_geloeschte_deaktivierte_einheiten | MANAGED | 244,295 | 1 | 2,923,502 | 2.79 |  |
| bronze | mastr_geloeschte_deaktivierte_marktakteure | MANAGED | 285,315 | 1 | 3,242,833 | 3.09 |  |
| bronze | mastr_katalogkategorien | MANAGED | 123 | 1 | 2,399 | 0.0 |  |
| bronze | mastr_katalogwerte | MANAGED | 1,737 | 1 | 24,359 | 0.02 |  |
| bronze | mastr_lokationen | MANAGED | 6,776,394 | 4 | 225,606,612 | 215.16 |  |
| bronze | mastr_marktakteure | MANAGED | 5,616,357 | 2 | 120,078,875 | 114.52 |  |
| bronze | mastr_marktakteure_und_rollen | MANAGED | 15,689 | 1 | 338,068 | 0.32 |  |
| bronze | mastr_netzanschlusspunkte | MANAGED | 5,867,232 | 4 | 190,773,766 | 181.94 |  |
| bronze | mastr_netze | MANAGED | 1,740 | 1 | 52,014 | 0.05 |  |
| bronze | power_plant_capacity_additions | MANAGED | 17 | 1 | 3,093 | 0.0 |  |
| bronze | power_plant_list | MANAGED | 2,613 | 1 | 98,131 | 0.09 |  |
| bronze | redispatch_measures | MANAGED | 40,118 | 1 | 392,398 | 0.37 |  |
| bronze | rees46_events | MANAGED | 109,950,743 | 29 | 1,913,065,542 | 1824.44 |  |
| bronze | smard_energy_timeseries | MANAGED | 1,255,904 | 2 | 4,238,887 | 4.04 |  |
| commerce_analytics | ga4_demand_daily | MANAGED | 7,389 | 1 | 39,713 | 0.04 |  |
| commerce_analytics | rees46_demand_daily | MANAGED | 2,492 | 1 | 29,510 | 0.03 |  |
| commerce_gold | ga4_event | VIEW | 779,485 |  |  |  |  |
| commerce_gold | ga4_event_item | VIEW | 3,982,732 |  |  |  |  |
| commerce_gold | ga4_product | MANAGED | 1,393 | 1 | 53,914 | 0.05 |  |
| commerce_gold | ga4_purchase | MANAGED | 4,786 | 1 | 452,322 | 0.43 |  |
| commerce_gold | ga4_session | MANAGED | 168,963 | 1 | 6,735,536 | 6.42 |  |
| commerce_gold | ga4_user | MANAGED | 133,010 | 1 | 2,937,909 | 2.8 |  |
| commerce_gold | rees46_event | VIEW | 109,819,980 |  |  |  |  |
| commerce_gold | rees46_product | MANAGED | 206,876 | 11 | 7,031,106 | 6.71 |  |
| commerce_gold | rees46_session | MANAGED | 23,017,601 | 21 | 1,299,758,367 | 1239.55 |  |
| commerce_gold | rees46_user | MANAGED | 5,316,648 | 5 | 90,449,982 | 86.26 |  |
| commerce_ml_datasets | assembled_lapse_ga4 | VIEW | 73,601 |  |  |  |  |
| commerce_ml_datasets | assembled_lapse_rees46 | VIEW | 3,022,289 |  |  |  |  |
| commerce_ml_datasets | assembled_next_item_ga4 | VIEW | 3,769,789 |  |  |  |  |
| commerce_ml_datasets | assembled_next_item_rees46 | VIEW | 73,916,883 |  |  |  |  |
| commerce_ml_datasets | assembled_rl_session_sequences | VIEW | 109,819,980 |  |  |  |  |
| commerce_ml_datasets | assembled_session_purchase_ga4 | VIEW | 17,218 |  |  |  |  |
| commerce_ml_datasets | assembled_session_purchase_rees46 | VIEW | 14,543,421 |  |  |  |  |
| commerce_ml_datasets | dataset_lapse_ga4 | MANAGED | 33,593 | 1 | 825,367 | 0.79 |  |
| commerce_ml_datasets | dataset_lapse_rees46 | MANAGED | 3,022,289 | 3 | 59,826,792 | 57.06 |  |
| commerce_ml_datasets | dataset_manifest | MANAGED | 7 | 7 | 36,153 | 0.03 |  |
| commerce_ml_datasets | dataset_next_item_ga4 | MANAGED | 3,769,789 | 4 | 217,680,676 | 207.6 |  |
| commerce_ml_datasets | dataset_next_item_rees46 | MANAGED | 33,581,598 | 43 | 2,540,481,374 | 2422.79 |  |
| commerce_ml_datasets | dataset_rl_session_sequences | MANAGED | 49,989,095 | 86 | 4,989,186,134 | 4758.06 |  |
| commerce_ml_datasets | dataset_session_purchase_ga4 | MANAGED | 17,218 | 2 | 1,013,985 | 0.97 |  |
| commerce_ml_datasets | dataset_session_purchase_rees46 | MANAGED | 6,700,427 | 9 | 541,500,853 | 516.42 |  |
| commerce_ml_datasets | feature_contract | MANAGED | 124 | 7 | 17,997 | 0.02 |  |
| commerce_ml_datasets | features_item_sequence_ga4 | MANAGED | 3,852,091 | 2 | 30,867,850 | 29.44 |  |
| commerce_ml_datasets | features_item_sequence_rees46 | MANAGED | 109,819,980 | 86 | 5,209,540,926 | 4968.21 |  |
| commerce_ml_datasets | features_lapse_ga4 | MANAGED | 73,601 | 2 | 1,083,727 | 1.03 |  |
| commerce_ml_datasets | features_lapse_rees46 | MANAGED | 3,022,289 | 4 | 44,658,868 | 42.59 |  |
| commerce_ml_datasets | features_product_asof_ga4 | MANAGED | 51,188 | 1 | 196,959 | 0.19 |  |
| commerce_ml_datasets | features_product_asof_rees46 | MANAGED | 5,001,492 | 10 | 13,701,638 | 13.07 |  |
| commerce_ml_datasets | features_session_prefix_ga4 | MANAGED | 17,452 | 1 | 723,125 | 0.69 |  |
| commerce_ml_datasets | features_session_prefix_rees46 | MANAGED | 14,747,521 | 14 | 793,483,515 | 756.72 |  |
| commerce_ml_datasets | features_user_asof_ga4 | MANAGED | 168,963 | 1 | 3,984,698 | 3.8 |  |
| commerce_ml_datasets | features_user_asof_rees46 | MANAGED | 23,017,601 | 13 | 738,096,882 | 703.9 |  |
| commerce_ml_datasets | gate_results | MANAGED | 105 | 2 | 6,950 | 0.01 |  |
| commerce_ml_datasets | imputer_parameters | MANAGED | 162 | 30 | 230,713 | 0.22 |  |
| commerce_ml_datasets | mask_specification | MANAGED | 0 | 0 | 0 | 0.0 |  |
| commerce_ml_datasets | null_class_registry | MANAGED | 85 | 7 | 18,062 | 0.02 |  |
| commerce_ml_datasets | null_rate_profile | MANAGED | 0 | 0 | 0 | 0.0 |  |
| commerce_ml_datasets | partition_manifest | MANAGED | 205,163,181 | 102 | 5,903,636,193 | 5630.15 |  |
| commerce_ml_datasets | split_specification | MANAGED | 7 | 1 | 4,590 | 0.0 |  |
| commerce_ml_datasets | target_basket_ga4 | MANAGED | 4,446 | 1 | 144,125 | 0.14 |  |
| commerce_ml_datasets | target_basket_rees46 | MANAGED | 1,402,758 | 2 | 43,650,706 | 41.63 |  |
| commerce_ml_datasets | target_lapse_ga4 | MANAGED | 73,601 | 2 | 717,156 | 0.68 |  |
| commerce_ml_datasets | target_lapse_rees46 | MANAGED | 3,022,289 | 2 | 13,193,354 | 12.58 |  |
| commerce_ml_datasets | target_next_item_ga4 | MANAGED | 3,769,789 | 1 | 6,757,247 | 6.44 |  |
| commerce_ml_datasets | target_next_item_rees46 | MANAGED | 73,916,883 | 9 | 537,945,785 | 513.03 |  |
| commerce_ml_datasets | target_rl_session_rees46 | MANAGED | 109,819,980 | 18 | 5,287,014,008 | 5042.09 |  |
| commerce_ml_datasets | target_session_purchase_ga4 | MANAGED | 17,218 | 2 | 439,244 | 0.42 |  |
| commerce_ml_datasets | target_session_purchase_rees46 | MANAGED | 14,543,421 | 7 | 399,146,104 | 380.66 |  |
| commerce_ml_models | approval_spec | MANAGED | 14 | 1 | 3,340 | 0.0 |  |
| commerce_ml_models | candidate_results | MANAGED | 512 | 13 | 85,290 | 0.08 |  |
| commerce_ml_models | candidate_selection | MANAGED | 21 | 1 | 4,995 | 0.0 |  |
| commerce_ml_models | evaluation_flags | MANAGED | 1 | 1 | 3,101 | 0.0 |  |
| commerce_ml_models | evaluation_results | MANAGED | 618 | 11 | 67,903 | 0.06 |  |
| commerce_ml_models | evaluation_run_context | MANAGED | 42 | 11 | 62,144 | 0.06 |  |
| commerce_ml_models | evaluation_spec | MANAGED | 17 | 2 | 4,697 | 0.0 |  |
| commerce_ml_models | evaluation_split_manifest | MANAGED | 0 | 0 | 0 | 0.0 |  |
| commerce_ml_models | library_availability | MANAGED | 9 | 1 | 1,749 | 0.0 |  |
| commerce_ml_models | model_approval | MANAGED | 21 | 1 | 9,565 | 0.01 |  |
| commerce_ml_models | model_predictions | MANAGED | 1,550,811 | 1 | 24,089,595 | 22.97 |  |
| commerce_ml_models | model_registry | MANAGED | 14 | 1 | 10,235 | 0.01 |  |
| commerce_ml_models | monitoring_flags | MANAGED | 13 | 5 | 19,485 | 0.02 |  |
| commerce_ml_models | monitoring_results | MANAGED | 349 | 21 | 105,276 | 0.1 |  |
| commerce_ml_models | monitoring_spec | MANAGED | 6 | 1 | 2,430 | 0.0 |  |
| commerce_ml_models | monitoring_triggers | MANAGED | 2 | 2 | 7,748 | 0.01 |  |
| commerce_ml_models | reference_profile | MANAGED | 57 | 7 | 45,340 | 0.04 |  |
| commerce_ml_models | registry_check | MANAGED | 14 | 1 | 6,948 | 0.01 |  |
| commerce_ml_models | task_registry | MANAGED | 7 | 1 | 4,165 | 0.0 |  |
| commerce_ml_models | task_run_context | MANAGED | 58 | 27 | 142,777 | 0.14 |  |
| commerce_semantic | metric_definition | MANAGED | 3 | 1 | 4,260 | 0.0 |  |
| commerce_silver | ga4_event | MANAGED | 779,485 | 2 | 69,319,275 | 66.11 |  |
| commerce_silver | ga4_event_item | MANAGED | 3,982,732 | 2 | 170,994,682 | 163.07 |  |
| commerce_silver | rees46_event | MANAGED | 109,819,980 | 205 | 12,460,359,891 | 11883.13 |  |
| energy_analytics | generation_by_carrier_daily | MANAGED | 99,847 | 1 | 472,922 | 0.45 |  |
| energy_analytics | market_price_daily | MANAGED | 2,897 | 1 | 45,855 | 0.04 |  |
| energy_analytics | site_energy_daily | MANAGED | 16,984 | 1 | 126,308 | 0.12 |  |
| energy_analytics | site_energy_weather_daily | MANAGED | 16,984 | 1 | 247,528 | 0.24 |  |
| energy_analytics | site_weather_daily | MANAGED | 2,159 | 1 | 53,782 | 0.05 |  |
| energy_gold | balancing_area | VIEW | 1,118 |  |  |  |  |
| energy_gold | capacity_plan | MANAGED | 13 | 1 | 6,702 | 0.01 |  |
| energy_gold | channel_reading | MANAGED | 63,243,582 | 41 | 2,278,291,230 | 2172.75 |  |
| energy_gold | energy_balance_component | MANAGED | 13,973,351 | 3 | 221,784,893 | 211.51 |  |
| energy_gold | energy_channel | MANAGED | 8 | 1 | 3,018 | 0.0 |  |
| energy_gold | entity_link | MANAGED | 33,963,083 | 45 | 999,891,239 | 953.57 |  |
| energy_gold | forecast_actual | MANAGED | 17,142 | 4 | 541,644 | 0.52 |  |
| energy_gold | generation_forecast | VIEW | 2,897 |  |  |  |  |
| energy_gold | generation_unit | MANAGED | 170,974 | 1 | 19,177,378 | 18.29 |  |
| energy_gold | grid_connection_point | VIEW | 5,867,232 |  |  |  |  |
| energy_gold | grid_intervention_event | VIEW | 39,550 |  |  |  |  |
| energy_gold | grid_location | MANAGED | 6,776,394 | 2 | 249,467,624 | 237.91 |  |
| energy_gold | grid_location_coordinate_conflict | VIEW | 48,904 |  |  |  |  |
| energy_gold | grid_network | MANAGED | 1,740 | 1 | 65,665 | 0.06 |  |
| energy_gold | iot_device | MANAGED | 198,164 | 1 | 11,739,516 | 11.2 |  |
| energy_gold | iot_device_observation | MANAGED | 198,164 | 1 | 13,817,628 | 13.18 |  |
| energy_gold | market_actor | MANAGED | 5,616,357 | 2 | 137,302,016 | 130.94 |  |
| energy_gold | market_actor_role | VIEW | 15,689 |  |  |  |  |
| energy_gold | market_price | VIEW | 72,689 |  |  |  |  |
| energy_gold | place_instrument_period | MANAGED | 3,965 | 1 | 178,016 | 0.17 |  |
| energy_gold | place_parameter_period | MANAGED | 5,412 | 1 | 206,979 | 0.2 |  |
| energy_gold | plant_sensor_observation | VIEW | 9,568 |  |  |  |  |
| energy_gold | power_plant | MANAGED | 2,613 | 1 | 213,474 | 0.2 |  |
| energy_gold | registry_change_event | MANAGED | 756,015 | 1 | 23,290,821 | 22.21 |  |
| energy_gold | support_registration | VIEW | 159,232 |  |  |  |  |
| energy_gold | unit_authorisation | VIEW | 37,722 |  |  |  |  |
| energy_gold | unit_repowering | VIEW | 1,681 |  |  |  |  |
| energy_gold | weather_daily | VIEW | 2,144,456 |  |  |  |  |
| energy_gold | weather_observation | MANAGED | 27,251,632 | 88 | 5,537,479,343 | 5280.95 |  |
| energy_gold | weather_observation_radiation | MANAGED | 9,153,721 | 16 | 775,142,208 | 739.23 |  |
| energy_ml_datasets | assembled_bias | VIEW | 16,208 |  |  |  |  |
| energy_ml_datasets | assembled_capacity_additions | VIEW | 7,371 |  |  |  |  |
| energy_ml_datasets | assembled_ccpp | VIEW | 9,527 |  |  |  |  |
| energy_ml_datasets | assembled_honda_anomaly | VIEW | 144,691 |  |  |  |  |
| energy_ml_datasets | assembled_honda_forecast | VIEW | 723,470 |  |  |  |  |
| energy_ml_datasets | assembled_load | VIEW | 22,844 |  |  |  |  |
| energy_ml_datasets | assembled_price_daily | VIEW | 2,897 |  |  |  |  |
| energy_ml_datasets | assembled_price_quarter_hour | VIEW | 69,792 |  |  |  |  |
| energy_ml_datasets | assembled_redispatch | VIEW | 11,324 |  |  |  |  |
| energy_ml_datasets | assembled_redispatch_matching | VIEW | 719 |  |  |  |  |
| energy_ml_datasets | assembled_rl_pumped_storage | VIEW | 69,681 |  |  |  |  |
| energy_ml_datasets | assembled_rl_redispatch | VIEW | 39,440 |  |  |  |  |
| energy_ml_datasets | assembled_self_supervised_other | VIEW | 167,535 |  |  |  |  |
| energy_ml_datasets | assembled_survival | VIEW | 34,956 |  |  |  |  |
| energy_ml_datasets | assembled_weak_supervision | VIEW | 173,756 |  |  |  |  |
| energy_ml_datasets | assembled_weather_imputation | VIEW | 1,943,510 |  |  |  |  |
| energy_ml_datasets | assembled_zone_generation | VIEW | 42,643 |  |  |  |  |
| energy_ml_datasets | dataset_bias | MANAGED | 16,187 | 1 | 2,891,336 | 2.76 |  |
| energy_ml_datasets | dataset_capacity_additions | MANAGED | 1,419 | 1 | 17,699 | 0.02 |  |
| energy_ml_datasets | dataset_ccpp | MANAGED | 9,527 | 1 | 742,926 | 0.71 |  |
| energy_ml_datasets | dataset_honda_anomaly | MANAGED | 144,547 | 1 | 8,217,791 | 7.84 |  |
| energy_ml_datasets | dataset_honda_forecast | MANAGED | 722,750 | 1 | 34,607,977 | 33.0 |  |
| energy_ml_datasets | dataset_load | MANAGED | 17,344 | 1 | 2,112,170 | 2.01 |  |
| energy_ml_datasets | dataset_manifest | MANAGED | 19 | 19 | 92,799 | 0.09 |  |
| energy_ml_datasets | dataset_price_daily | MANAGED | 2,893 | 1 | 843,039 | 0.8 |  |
| energy_ml_datasets | dataset_price_quarter_hour | MANAGED | 69,408 | 1 | 6,690,792 | 6.38 |  |
| energy_ml_datasets | dataset_redispatch | MANAGED | 11,308 | 1 | 465,792 | 0.44 |  |
| energy_ml_datasets | dataset_redispatch_matching | MANAGED | 719 | 1 | 25,437 | 0.02 |  |
| energy_ml_datasets | dataset_rl_pumped_storage | MANAGED | 69,297 | 1 | 2,382,833 | 2.27 |  |
| energy_ml_datasets | dataset_rl_redispatch | MANAGED | 39,314 | 1 | 2,177,745 | 2.08 |  |
| energy_ml_datasets | dataset_self_supervised_other | MANAGED | 161,891 | 2 | 1,233,807 | 1.18 |  |
| energy_ml_datasets | dataset_survival | MANAGED | 34,956 | 1 | 1,869,386 | 1.78 |  |
| energy_ml_datasets | dataset_weak_supervision | MANAGED | 173,756 | 1 | 2,263,833 | 2.16 |  |
| energy_ml_datasets | dataset_weather_imputation | MANAGED | 1,940,822 | 3 | 32,326,966 | 30.83 |  |
| energy_ml_datasets | dataset_zone_generation | MANAGED | 28,910 | 1 | 1,163,426 | 1.11 |  |
| energy_ml_datasets | derived_pv_forecast | VIEW | 2,897 |  |  |  |  |
| energy_ml_datasets | feature_contract | MANAGED | 408 | 17 | 46,007 | 0.04 |  |
| energy_ml_datasets | features_calendar_market_area | MANAGED | 32,868 | 1 | 117,608 | 0.11 |  |
| energy_ml_datasets | features_honda_site | MANAGED | 723,470 | 1 | 23,824,653 | 22.72 |  |
| energy_ml_datasets | features_hub_height_fill | MANAGED | 35,266 | 8 | 286,996 | 0.27 |  |
| energy_ml_datasets | features_market_daily | MANAGED | 2,897 | 1 | 463,363 | 0.44 |  |
| energy_ml_datasets | features_market_quarter_hour | MANAGED | 69,792 | 1 | 2,809,214 | 2.68 |  |
| energy_ml_datasets | features_pv_forecast_daily | MANAGED | 2,897 | 1 | 43,684 | 0.04 |  |
| energy_ml_datasets | features_redispatch_daily | MANAGED | 11,324 | 1 | 209,177 | 0.2 |  |
| energy_ml_datasets | features_redispatch_match | MANAGED | 719 | 1 | 18,123 | 0.02 |  |
| energy_ml_datasets | features_station_day_radiation | MANAGED | 43,383 | 1 | 202,994 | 0.19 |  |
| energy_ml_datasets | features_station_day_wind | MANAGED | 80,300 | 1 | 553,320 | 0.53 |  |
| energy_ml_datasets | features_unit_static | MANAGED | 35,266 | 1 | 1,193,865 | 1.14 |  |
| energy_ml_datasets | features_zone_capacity_asof | MANAGED | 17,382 | 1 | 56,393 | 0.05 |  |
| energy_ml_datasets | features_zone_weather | MANAGED | 46,184 | 5 | 516,889 | 0.49 |  |
| energy_ml_datasets | gap_limits | MANAGED | 19 | 1 | 2,914 | 0.0 |  |
| energy_ml_datasets | gate_results | MANAGED | 272 | 2 | 8,079 | 0.01 |  |
| energy_ml_datasets | imputer_parameters | MANAGED | 703 | 27 | 162,030 | 0.15 |  |
| energy_ml_datasets | mask_specification | MANAGED | 14 | 1 | 3,863 | 0.0 |  |
| energy_ml_datasets | null_class_registry | MANAGED | 313 | 17 | 44,586 | 0.04 |  |
| energy_ml_datasets | null_rate_profile | MANAGED | 401 | 17 | 39,946 | 0.04 |  |
| energy_ml_datasets | partition_manifest | MANAGED | 3,540,063 | 1 | 22,823,442 | 21.77 |  |
| energy_ml_datasets | split_specification | MANAGED | 17 | 1 | 5,468 | 0.01 |  |
| energy_ml_datasets | target_capacity_additions | MANAGED | 7,371 | 1 | 64,016 | 0.06 |  |
| energy_ml_datasets | target_ccpp | MANAGED | 9,527 | 1 | 348,224 | 0.33 |  |
| energy_ml_datasets | target_forecast_bias | MANAGED | 16,208 | 6 | 241,778 | 0.23 |  |
| energy_ml_datasets | target_honda_increment | MANAGED | 723,470 | 1 | 3,274,084 | 3.12 |  |
| energy_ml_datasets | target_load | MANAGED | 22,844 | 1 | 153,275 | 0.15 |  |
| energy_ml_datasets | target_price_daily | MANAGED | 2,897 | 1 | 16,787 | 0.02 |  |
| energy_ml_datasets | target_price_quarter_hour | MANAGED | 69,792 | 1 | 138,781 | 0.13 |  |
| energy_ml_datasets | target_redispatch_daily | MANAGED | 11,324 | 4 | 100,245 | 0.1 |  |
| energy_ml_datasets | target_redispatch_match | MANAGED | 719 | 1 | 9,651 | 0.01 |  |
| energy_ml_datasets | target_rl_pumped_storage | MANAGED | 69,681 | 1 | 2,178,917 | 2.08 |  |
| energy_ml_datasets | target_rl_redispatch_events | MANAGED | 39,440 | 1 | 1,714,057 | 1.63 |  |
| energy_ml_datasets | target_unit_survival | MANAGED | 34,956 | 1 | 503,877 | 0.48 |  |
| energy_ml_datasets | target_weak_labels | MANAGED | 173,756 | 3 | 1,117,601 | 1.07 |  |
| energy_ml_datasets | target_zone_generation | MANAGED | 42,643 | 1 | 254,945 | 0.24 |  |
| energy_ml_datasets | zone_code_map | VIEW | 4 |  |  |  |  |
| energy_ml_datasets | zone_weather_parameters | MANAGED | 18 | 1 | 3,448 | 0.0 |  |
| energy_ml_models | approval_spec | MANAGED | 14 | 1 | 3,396 | 0.0 |  |
| energy_ml_models | candidate_results | MANAGED | 1,758 | 19 | 136,802 | 0.13 |  |
| energy_ml_models | candidate_selection | MANAGED | 56 | 1 | 6,557 | 0.01 |  |
| energy_ml_models | evaluation_flags | MANAGED | 25 | 1 | 4,101 | 0.0 |  |
| energy_ml_models | evaluation_results | MANAGED | 2,863 | 29 | 191,951 | 0.18 |  |
| energy_ml_models | evaluation_run_context | MANAGED | 139 | 15 | 83,743 | 0.08 |  |
| energy_ml_models | evaluation_spec | MANAGED | 26 | 2 | 5,007 | 0.0 |  |
| energy_ml_models | evaluation_split_manifest | MANAGED | 174,475 | 1 | 2,134,262 | 2.04 |  |
| energy_ml_models | library_availability | MANAGED | 9 | 1 | 1,749 | 0.0 |  |
| energy_ml_models | model_approval | MANAGED | 56 | 1 | 15,630 | 0.01 |  |
| energy_ml_models | model_predictions | MANAGED | 1,273,563 | 2 | 11,382,488 | 10.86 |  |
| energy_ml_models | model_registry | MANAGED | 34 | 1 | 13,031 | 0.01 |  |
| energy_ml_models | monitoring_flags | MANAGED | 68 | 23 | 88,675 | 0.08 |  |
| energy_ml_models | monitoring_results | MANAGED | 933 | 20 | 96,935 | 0.09 |  |
| energy_ml_models | monitoring_spec | MANAGED | 6 | 1 | 2,430 | 0.0 |  |
| energy_ml_models | monitoring_triggers | MANAGED | 11 | 7 | 27,984 | 0.03 |  |
| energy_ml_models | reference_profile | MANAGED | 152 | 17 | 105,914 | 0.1 |  |
| energy_ml_models | registry_check | MANAGED | 34 | 1 | 7,679 | 0.01 |  |
| energy_ml_models | task_registry | MANAGED | 19 | 1 | 5,018 | 0.0 |  |
| energy_ml_models | task_run_context | MANAGED | 150 | 26 | 136,564 | 0.13 |  |
| energy_semantic | metric_definition | MANAGED | 5 | 1 | 4,468 | 0.0 |  |
| energy_silver | actor_deletion_event | MANAGED | 285,315 | 1 | 4,745,238 | 4.53 |  |
| energy_silver | device_telemetry_snapshot | MANAGED | 198,164 | 1 | 18,116,157 | 17.28 |  |
| energy_silver | electricity_balance | MANAGED | 3,577,595 | 3 | 322,915,430 | 307.96 |  |
| energy_silver | electricity_generation_forecast | MANAGED | 2,897 | 1 | 313,064 | 0.3 |  |
| energy_silver | electricity_price | MANAGED | 72,689 | 1 | 5,030,194 | 4.8 |  |
| energy_silver | energy_meter_reading | MANAGED | 3,417,957 | 2 | 361,720,841 | 344.96 |  |
| energy_silver | generation_unit | MANAGED | 170,974 | 6 | 19,425,640 | 18.53 |  |
| energy_silver | grid_intervention_event | MANAGED | 39,550 | 1 | 3,938,911 | 3.76 |  |
| energy_silver | grid_location_coordinate_conflict | MANAGED | 48,904 | 1 | 635,351 | 0.61 |  |
| energy_silver | grid_operator_change_event | MANAGED | 226,405 | 1 | 13,428,001 | 12.81 |  |
| energy_silver | plant_operating_sample | MANAGED | 9,568 | 1 | 740,682 | 0.71 |  |
| energy_silver | power_plant_capacity_plan | MANAGED | 13 | 1 | 5,894 | 0.01 |  |
| energy_silver | power_plant_register | MANAGED | 2,613 | 1 | 210,911 | 0.2 |  |
| energy_silver | register_link | MANAGED | 15,358,350 | 6 | 706,589,925 | 673.86 |  |
| energy_silver | support_registration | MANAGED | 159,232 | 5 | 11,027,671 | 10.52 |  |
| energy_silver | thermal_energy | MANAGED | 3,417,960 | 2 | 249,399,955 | 237.85 |  |
| energy_silver | unit_authorisation | MANAGED | 37,722 | 1 | 1,834,114 | 1.75 |  |
| energy_silver | unit_deletion_event | MANAGED | 244,295 | 1 | 4,278,051 | 4.08 |  |
| energy_silver | unit_repowering | MANAGED | 1,681 | 1 | 35,971 | 0.03 |  |
| energy_silver | weather_cloud | MANAGED | 12,649,094 | 8 | 992,542,765 | 946.56 |  |
| energy_silver | weather_daily | MANAGED | 2,144,456 | 4 | 150,064,863 | 143.11 |  |
| energy_silver | weather_humidity | MANAGED | 17,468,859 | 9 | 1,318,829,562 | 1257.73 |  |
| energy_silver | weather_longwave_radiation | MANAGED | 1,764,083 | 2 | 131,355,806 | 125.27 |  |
| energy_silver | weather_precipitation | MANAGED | 6,954,742 | 4 | 488,285,381 | 465.67 |  |
| energy_silver | weather_present_weather | MANAGED | 7,866,901 | 5 | 578,828,202 | 552.01 |  |
| energy_silver | weather_pressure | MANAGED | 12,354,629 | 8 | 930,428,302 | 887.33 |  |
| energy_silver | weather_soil_temperature | MANAGED | 6,986,583 | 5 | 536,408,602 | 511.56 |  |
| energy_silver | weather_solar_geometry | MANAGED | 5,809,249 | 3 | 437,833,423 | 417.55 |  |
| energy_silver | weather_solar_radiation | MANAGED | 9,035,706 | 6 | 672,275,718 | 641.13 |  |
| energy_silver | weather_sunshine_duration | MANAGED | 18,292,694 | 7 | 1,317,002,992 | 1255.99 |  |
| energy_silver | weather_temperature | MANAGED | 20,854,453 | 15 | 1,627,833,103 | 1552.42 |  |
| energy_silver | weather_uv_index | MANAGED | 6,727 | 2 | 474,823 | 0.45 |  |
| energy_silver | weather_visibility | MANAGED | 12,927,956 | 6 | 932,407,131 | 889.21 |  |
| energy_silver | weather_wind | MANAGED | 16,622,510 | 10 | 1,263,262,087 | 1204.74 |  |
| energy_silver_reference | balancing_area | MANAGED | 1,118 | 1 | 56,473 | 0.05 |  |
| energy_silver_reference | grid_connection_point | MANAGED | 5,867,232 | 4 | 202,269,297 | 192.9 |  |
| energy_silver_reference | grid_location | MANAGED | 6,776,394 | 4 | 246,863,118 | 235.43 |  |
| energy_silver_reference | grid_network | MANAGED | 1,740 | 1 | 61,120 | 0.06 |  |
| energy_silver_reference | market_actor | MANAGED | 5,616,357 | 2 | 137,132,014 | 130.78 |  |
| energy_silver_reference | market_actor_role | MANAGED | 15,689 | 1 | 436,069 | 0.42 |  |
| energy_silver_reference | mastr_code_list | MANAGED | 1,912 | 1 | 92,369 | 0.09 |  |
| energy_silver_reference | weather_location | MANAGED | 80 | 4 | 30,818 | 0.03 |  |
| energy_silver_reference | weather_location_validity | MANAGED | 222 | 1 | 18,690 | 0.02 |  |
| energy_silver_reference | weather_missing_value_period | MANAGED | 5,817,858 | 3 | 229,352,829 | 218.73 |  |
| energy_silver_reference | weather_missingness_reconciliation | MANAGED | 1,391 | 1 | 61,867 | 0.06 |  |
| energy_silver_reference | weather_parameter_catalog | MANAGED | 55 | 8 | 32,218 | 0.03 |  |
| energy_silver_reference | weather_parameter_period | MANAGED | 5,412 | 1 | 206,739 | 0.2 |  |
| energy_silver_reference | weather_station_instrument | MANAGED | 3,965 | 1 | 177,774 | 0.17 |  |
| energy_silver_reference | weather_station_name_history | MANAGED | 100 | 1 | 9,669 | 0.01 |  |
| quality | field_class_registry | MANAGED | 2,823 | 3 | 27,551 | 0.03 |  |
| quality | pipeline_watermarks | MANAGED | 1,117 | 1 | 24,665 | 0.02 |  |
| quality | quality_audit_log | MANAGED | 3,201 | 29 | 144,582 | 0.14 |  |
| quality | quarantine | MANAGED | 1,880,371 | 8 | 38,933,011 | 37.13 |  |
| shared_conformed | data_gap_period | MANAGED | 5,817,858 | 3 | 229,350,234 | 218.73 |  |
| shared_conformed | dim_date | MANAGED | 50,768 | 8 | 70,358 | 0.07 |  |
| shared_conformed | dim_geography | MANAGED | 17 | 1 | 3,300 | 0.0 |  |
| shared_conformed | dim_place | MANAGED | 80 | 1 | 15,623 | 0.01 |  |
| shared_conformed | dim_place_validity | MANAGED | 322 | 2 | 30,168 | 0.03 |  |
| shared_conformed | dim_time | MANAGED | 1,440 | 2 | 5,383 | 0.01 |  |
| shared_conformed | dim_weather_context | MANAGED | 0 | 0 | 0 | 0.0 |  |
| shared_conformed | geo_plz_gemeinde_xref | MANAGED | 0 | 0 | 0 | 0.0 |  |
| shared_conformed | quality_exception | VIEW | 469,839 |  |  |  |  |
| shared_conformed | ref_energy_carrier | MANAGED | 84 | 1 | 4,200 | 0.0 |  |
| shared_conformed | ref_energy_carrier_map | MANAGED | 95 | 1 | 5,573 | 0.01 |  |
| shared_conformed | ref_measure | MANAGED | 55 | 1 | 6,210 | 0.01 |  |
| shared_conformed | ref_vocabulary | MANAGED | 1,912 | 1 | 92,600 | 0.09 |  |
| shared_conformed | series_coverage | MANAGED | 1,391 | 1 | 61,122 | 0.06 |  |

## Errors (0)

None.
