# Layer retirement plan

_Generated: 2026-11-20T15:31Z_

Switches: tables and views True, Volumes False, schemas False

## Schemas to drop

| layer | schema | tables | views | volumes | decision |
| --- | --- | --- | --- | --- | --- |
| gold and analytics (old tables) | energy_gold | 7 | 0 | 0 | drop |
| gold and analytics (old tables) | commerce_gold | 5 | 0 | 0 | drop |
| gold and analytics (old tables) | genomics_gold | 11 | 0 | 0 | drop |
| gold and analytics (old tables) | shared_conformed | 0 | 0 | 0 | drop |
| gold and analytics (old tables) | energy_analytics | 2 | 0 | 0 | drop |
| gold and analytics (old tables) | commerce_analytics | 2 | 0 | 0 | drop |
| gold and analytics (old tables) | genomics_analytics | 0 | 0 | 0 | drop |

## Successor check

| layer | successor schema | tables | problem | passes |
| --- | --- | --- | --- | --- |
| gold and analytics (old tables) | energy_silver | 16 |  | 1 |
| gold and analytics (old tables) | energy_silver_reference | 12 |  | 1 |
| gold and analytics (old tables) | commerce_silver | 5 |  | 1 |
| gold and analytics (old tables) | genomics_silver | 27 |  | 1 |
| gold and analytics (old tables) | genomics_silver_reference | 11 |  | 1 |

## Tables by last write (cutoff 2026-11-19T21:00:00Z)

| schema | table | last write | provenance column | decision |
| --- | --- | --- | --- | --- |
| energy_gold | balancing_area | 2026-11-19 21:28 | yes | keep (written after the cutoff or never) |
| energy_gold | capacity_plan | 2026-11-19 21:35 | yes | keep (written after the cutoff or never) |
| energy_gold | channel_reading | 2026-11-19 21:52 | yes | keep (written after the cutoff or never) |
| energy_gold | energy_component | 2026-11-19 21:52 | yes | keep (written after the cutoff or never) |
| energy_gold | entity_link | 2026-11-19 21:54 | yes | keep (written after the cutoff or never) |
| energy_gold | forecast_actual | 2026-11-19 21:53 | yes | keep (written after the cutoff or never) |
| energy_gold | generation_unit | 2026-11-19 21:34 | yes | keep (written after the cutoff or never) |
| energy_gold | grid_connection_point | 2026-11-19 21:44 | yes | keep (written after the cutoff or never) |
| energy_gold | grid_intervention_event | 2026-11-19 21:51 | yes | keep (written after the cutoff or never) |
| energy_gold | grid_location | 2026-11-19 21:24 | yes | keep (written after the cutoff or never) |
| energy_gold | grid_network | 2026-11-19 21:28 | yes | keep (written after the cutoff or never) |
| energy_gold | iot_device | 2026-11-19 21:51 | yes | keep (written after the cutoff or never) |
| energy_gold | market_actor | 2026-11-19 21:23 | yes | keep (written after the cutoff or never) |
| energy_gold | market_actor_role | 2026-11-19 21:35 | yes | keep (written after the cutoff or never) |
| energy_gold | market_energy_component | 2026-11-18 22:27 | yes | old |
| energy_gold | market_price | 2026-11-19 21:52 | yes | keep (written after the cutoff or never) |
| energy_gold | plant_sensor_observation | 2026-11-19 21:51 | yes | keep (written after the cutoff or never) |
| energy_gold | power_plant | 2026-11-19 21:34 | yes | keep (written after the cutoff or never) |
| energy_gold | site_energy_component | 2026-11-18 22:27 | yes | old |
| energy_gold | storage_plant | 2026-11-19 21:18 | yes | keep (written after the cutoff or never) |
| energy_gold | support_registration | 2026-11-18 22:22 | yes | old |
| energy_gold | unit_event | 2026-11-19 21:34 | yes | keep (written after the cutoff or never) |
| energy_gold | weather_air_state | 2026-11-18 22:28 | yes | old |
| energy_gold | weather_daily | 2026-11-19 21:51 | yes | keep (written after the cutoff or never) |
| energy_gold | weather_ground_sun | 2026-11-18 22:29 | yes | old |
| energy_gold | weather_observation | 2026-11-19 21:53 | yes | keep (written after the cutoff or never) |
| energy_gold | weather_sky | 2026-11-18 22:28 | yes | old |
| energy_gold | weather_wind_precipitation | 2026-11-18 22:28 | yes | old |
| commerce_gold | commerce_cart_event | 2026-11-18 19:39 | yes | old |
| commerce_gold | commerce_event | 2026-11-19 21:16 | yes | keep (written after the cutoff or never) |
| commerce_gold | commerce_product | 2026-11-19 21:14 | yes | keep (written after the cutoff or never) |
| commerce_gold | commerce_purchase_event | 2026-11-18 19:41 | yes | old |
| commerce_gold | commerce_session | 2026-11-19 21:11 | yes | keep (written after the cutoff or never) |
| commerce_gold | commerce_user | 2026-11-18 19:36 | yes | old |
| commerce_gold | commerce_view_event | 2026-11-18 19:38 | yes | old |
| commerce_gold | commerce_web_event | 2026-11-18 19:42 | yes | old |
| genomics_gold | benchmark_coding_variant | 2026-11-18 20:19 | yes | old |
| genomics_gold | benchmark_population_variant | 2026-11-18 20:19 | yes | old |
| genomics_gold | benchmark_regulatory_variant | 2026-11-18 20:19 | yes | old |
| genomics_gold | benchmark_variant | 2026-11-19 22:10 | yes | keep (written after the cutoff or never) |
| genomics_gold | case_assay_record | 2026-11-19 22:05 | yes | keep (written after the cutoff or never) |
| genomics_gold | cell_line_profile | 2026-11-19 21:58 | yes | keep (written after the cutoff or never) |
| genomics_gold | clinical_trial_profile | 2026-11-19 21:58 | yes | keep (written after the cutoff or never) |
| genomics_gold | clinvar_classification_change | 2026-11-19 22:02 | yes | keep (written after the cutoff or never) |
| genomics_gold | disease_profile | 2026-11-19 21:58 | yes | keep (written after the cutoff or never) |
| genomics_gold | dms_assay | 2026-11-19 22:10 | yes | keep (written after the cutoff or never) |
| genomics_gold | dms_assay_mutant | 2026-11-19 22:10 | yes | keep (written after the cutoff or never) |
| genomics_gold | drug_profile | 2026-11-19 21:58 | yes | keep (written after the cutoff or never) |
| genomics_gold | embedding_split | 2026-11-19 22:10 | yes | keep (written after the cutoff or never) |
| genomics_gold | entity_identity_record | 2026-11-19 22:06 | yes | keep (written after the cutoff or never) |
| genomics_gold | epitope_profile | 2026-11-19 21:59 | yes | keep (written after the cutoff or never) |
| genomics_gold | gene_cancer_profile | 2026-11-18 19:59 | yes | old |
| genomics_gold | gene_clinical_profile | 2026-11-18 19:58 | yes | old |
| genomics_gold | gene_disease_profile | 2026-11-19 22:00 | yes | keep (written after the cutoff or never) |
| genomics_gold | gene_drug_profile | 2026-11-18 19:59 | yes | old |
| genomics_gold | gene_expression_profile | 2026-11-18 19:59 | yes | old |
| genomics_gold | gene_measurement | 2026-11-19 22:04 | yes | keep (written after the cutoff or never) |
| genomics_gold | gene_profile | 2026-11-19 21:34 | yes | keep (written after the cutoff or never) |
| genomics_gold | genetic_test_profile | 2026-11-19 21:58 | yes | keep (written after the cutoff or never) |
| genomics_gold | multi_value_attribute | 2026-11-19 21:33 | yes | keep (written after the cutoff or never) |
| genomics_gold | multi_value_score | 2026-11-19 21:35 | yes | keep (written after the cutoff or never) |
| genomics_gold | protein_profile | 2026-11-19 21:57 | yes | keep (written after the cutoff or never) |
| genomics_gold | protein_variant_record | 2026-11-19 22:05 | yes | keep (written after the cutoff or never) |
| genomics_gold | somatic_mutation_profile | 2026-11-19 21:59 | yes | keep (written after the cutoff or never) |
| genomics_gold | somatic_site_profile | 2026-11-19 22:02 | yes | keep (written after the cutoff or never) |
| genomics_gold | structural_variant_profile | 2026-11-19 22:01 | yes | keep (written after the cutoff or never) |
| genomics_gold | transcript_profile | 2026-11-19 21:57 | yes | keep (written after the cutoff or never) |
| genomics_gold | tumor_sample_profile | 2026-11-19 21:59 | yes | keep (written after the cutoff or never) |
| genomics_gold | variant_clinical_profile | 2026-11-18 20:06 | yes | old |
| genomics_gold | variant_frequency_profile | 2026-11-18 20:09 | yes | old |
| genomics_gold | variant_impact_profile | 2026-11-18 20:07 | yes | old |
| genomics_gold | variant_profile | 2026-11-19 21:49 | yes | keep (written after the cutoff or never) |
| genomics_gold | variant_score_profile | 2026-11-18 20:07 | yes | old |
| shared_conformed | dim_date |  | no | keep (no column) |
| shared_conformed | dim_geography |  | no | keep (no column) |
| shared_conformed | dim_place | 2026-11-19 21:23 | yes | keep (written after the cutoff or never) |
| shared_conformed | dim_time |  | no | keep (no column) |
| shared_conformed | dim_weather_context |  | no | keep (no column) |
| shared_conformed | geo_plz_gemeinde_xref |  | no | keep (no column) |
| shared_conformed | metric_definition | 2026-11-19 21:23 | yes | keep (written after the cutoff or never) |
| shared_conformed | ref_energy_carrier | 2026-11-19 21:12 | yes | keep (written after the cutoff or never) |
| shared_conformed | ref_energy_carrier_map | 2026-11-19 21:11 | yes | keep (written after the cutoff or never) |
| shared_conformed | ref_measure | 2026-11-19 21:14 | yes | keep (written after the cutoff or never) |
| shared_conformed | series_coverage | 2026-11-19 21:23 | yes | keep (written after the cutoff or never) |
| energy_analytics | capacity_margin_yearly | 2026-11-19 22:01 | yes | keep (written after the cutoff or never) |
| energy_analytics | capacity_registry | 2026-11-19 22:00 | yes | keep (written after the cutoff or never) |
| energy_analytics | capacity_utilisation_monthly | 2026-11-19 22:01 | yes | keep (written after the cutoff or never) |
| energy_analytics | cross_border_trade | 2026-11-19 21:58 | yes | keep (written after the cutoff or never) |
| energy_analytics | extreme_weather_stress_event | 2026-11-19 22:05 | yes | keep (written after the cutoff or never) |
| energy_analytics | fleet_lifecycle_event | 2026-11-19 22:00 | yes | keep (written after the cutoff or never) |
| energy_analytics | forecast_accuracy | 2026-11-19 21:58 | yes | keep (written after the cutoff or never) |
| energy_analytics | generation_by_carrier_daily | 2026-11-19 21:57 | yes | keep (written after the cutoff or never) |
| energy_analytics | grid_intervention_daily | 2026-11-19 21:58 | yes | keep (written after the cutoff or never) |
| energy_analytics | market_price_daily | 2026-11-19 21:57 | yes | keep (written after the cutoff or never) |
| energy_analytics | market_timeseries | 2026-11-19 21:57 | yes | keep (written after the cutoff or never) |
| energy_analytics | metric_definition | 2026-11-18 22:33 | yes | old |
| energy_analytics | negative_price_event | 2026-11-19 21:58 | yes | keep (written after the cutoff or never) |
| energy_analytics | operator_concentration | 2026-11-19 22:02 | yes | keep (written after the cutoff or never) |
| energy_analytics | price_formation_daily | 2026-11-18 22:32 | yes | old |
| energy_analytics | site_energy_weather_daily | 2026-11-19 22:02 | yes | keep (written after the cutoff or never) |
| energy_analytics | support_scheme_buildout | 2026-11-19 22:02 | yes | keep (written after the cutoff or never) |
| energy_analytics | weather_energy_daily | 2026-11-19 22:04 | yes | keep (written after the cutoff or never) |
| commerce_analytics | customer_journey_daily | 2026-11-19 21:34 | yes | keep (written after the cutoff or never) |
| commerce_analytics | ga4_demand_daily | 2026-11-19 21:34 | yes | keep (written after the cutoff or never) |
| commerce_analytics | metric_definition | 2026-11-18 19:52 | yes | old |
| commerce_analytics | product_demand_daily | 2026-11-19 21:35 | yes | keep (written after the cutoff or never) |
| commerce_analytics | rees46_demand_daily | 2026-11-18 19:49 | yes | old |
| genomics_analytics | classification_change_summary | 2026-11-19 22:13 | yes | keep (written after the cutoff or never) |
| genomics_analytics | disease_overview | 2026-11-19 22:12 | yes | keep (written after the cutoff or never) |
| genomics_analytics | gene_prioritization | 2026-11-19 22:12 | yes | keep (written after the cutoff or never) |
| genomics_analytics | genetic_test_panel | 2026-11-19 22:12 | yes | keep (written after the cutoff or never) |
| genomics_analytics | population_risk | 2026-11-19 22:12 | yes | keep (written after the cutoff or never) |
| genomics_analytics | variant_review_queue | 2026-11-19 22:12 | yes | keep (written after the cutoff or never) |
