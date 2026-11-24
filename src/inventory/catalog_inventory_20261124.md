# Catalog inventory: energy_commerce_retail_media

Generated: 2026-11-24T16:49:40Z

Size is the data files of the current table version (older versions kept for time travel are not counted). A view has no size. `tables` counts tables and views; `views` is the part that are views.

## Guard lines

- WARNING metastore: 970 tables and views in all catalogs (guard 400)

## Overall

| schemas | tables | views | rows | files | size_bytes | size_gb | volumes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 27 | 339 | 0 | 3,697,290,471 | 3,387 | 202,426,870,276 | 188.525 | 5 |

## Catalogs

| catalog | tables_and_views | error |
| --- | --- | --- |
| energy_commerce_retail_media | 339 |  |
| samples | 124 |  |
| system | 507 |  |

## Quota limits and usage

| quota | parent | parent_type | used | limit | used_percent |
| --- | --- | --- | --- | --- | --- |
| table-quota | energy_commerce_retail_media.genomics_bronze | SecurableType.SCHEMA | 84 | 100 | 84.0 |
| table-quota | 1a00c03f-d59a-4486-bdcc-33a6b812f863 | SecurableType.METASTORE | 340 | 500 | 68.0 |
| schema-quota | energy_commerce_retail_media | SecurableType.CATALOG | 27 | 50 | 54.0 |
| table-quota | energy_commerce_retail_media.genomics_gold | SecurableType.SCHEMA | 37 | 100 | 37.0 |
| table-quota | energy_commerce_retail_media.bronze | SecurableType.SCHEMA | 35 | 100 | 35.0 |
| table-quota | energy_commerce_retail_media.energy_gold | SecurableType.SCHEMA | 28 | 100 | 28.0 |
| table-quota | energy_commerce_retail_media.commerce_ml_models | SecurableType.SCHEMA | 27 | 100 | 27.0 |
| table-quota | energy_commerce_retail_media.genomics_silver | SecurableType.SCHEMA | 27 | 100 | 27.0 |
| table-quota | energy_commerce_retail_media.energy_ml_models | SecurableType.SCHEMA | 27 | 100 | 27.0 |
| table-quota | energy_commerce_retail_media.energy_analytics | SecurableType.SCHEMA | 20 | 100 | 20.0 |
| table-quota | energy_commerce_retail_media.ml_models | SecurableType.SCHEMA | 19 | 100 | 19.0 |
| volume-quota | 1a00c03f-d59a-4486-bdcc-33a6b812f863 | SecurableType.METASTORE | 8 | 50 | 16.0 |
| table-quota | energy_commerce_retail_media.energy_silver | SecurableType.SCHEMA | 16 | 100 | 16.0 |
| table-quota | energy_commerce_retail_media.energy_ml_datasets | SecurableType.SCHEMA | 13 | 100 | 13.0 |
| table-quota | energy_commerce_retail_media.energy_silver_reference | SecurableType.SCHEMA | 12 | 100 | 12.0 |
| table-quota | energy_commerce_retail_media.shared_conformed | SecurableType.SCHEMA | 11 | 100 | 11.0 |
| table-quota | energy_commerce_retail_media.genomics_silver_reference | SecurableType.SCHEMA | 11 | 100 | 11.0 |
| table-quota | energy_commerce_retail_media.commerce_ml_datasets | SecurableType.SCHEMA | 7 | 100 | 7.0 |
| table-quota | energy_commerce_retail_media.ml_datasets_shared | SecurableType.SCHEMA | 7 | 100 | 7.0 |
| catalog-quota | 1a00c03f-d59a-4486-bdcc-33a6b812f863 | SecurableType.METASTORE | 3 | 50 | 6.0 |
| table-quota | energy_commerce_retail_media.genomics_analytics | SecurableType.SCHEMA | 6 | 100 | 6.0 |
| table-quota | energy_commerce_retail_media.knowledge | SecurableType.SCHEMA | 6 | 100 | 6.0 |
| table-quota | energy_commerce_retail_media.commerce_silver | SecurableType.SCHEMA | 5 | 100 | 5.0 |
| table-quota | energy_commerce_retail_media.commerce_analytics | SecurableType.SCHEMA | 5 | 100 | 5.0 |
| table-quota | energy_commerce_retail_media.quality | SecurableType.SCHEMA | 5 | 100 | 5.0 |
| privilege-quota | 1a00c03f-d59a-4486-bdcc-33a6b812f863 | SecurableType.METASTORE | 9 | 200 | 4.5 |
| schema-quota | system | SecurableType.CATALOG | 2 | 50 | 4.0 |
| volume-quota | energy_commerce_retail_media.knowledge | SecurableType.SCHEMA | 2 | 50 | 4.0 |
| table-quota | energy_commerce_retail_media.commerce_gold | SecurableType.SCHEMA | 4 | 100 | 4.0 |
| volume-quota | energy_commerce_retail_media.energy_ml_models | SecurableType.SCHEMA | 2 | 50 | 4.0 |
| privilege-quota | energy_commerce_retail_media | SecurableType.CATALOG | 6 | 200 | 3.0 |
| table-quota | energy_commerce_retail_media.genomics_ml_datasets | SecurableType.SCHEMA | 3 | 100 | 3.0 |
| privilege-quota | samples | SecurableType.CATALOG | 5 | 200 | 2.5 |
| provider-quota | 1a00c03f-d59a-4486-bdcc-33a6b812f863 | SecurableType.METASTORE | 1 | 50 | 2.0 |
| volume-quota | samples.sec | SecurableType.SCHEMA | 1 | 50 | 2.0 |
| privilege-quota | system.ai | SecurableType.SCHEMA | 4 | 200 | 2.0 |
| privilege-quota | energy_commerce_retail_media.energy_analytics | SecurableType.SCHEMA | 4 | 200 | 2.0 |
| privilege-quota | system.__internal_ai | SecurableType.SCHEMA | 4 | 200 | 2.0 |
| volume-quota | system.__internal_ai | SecurableType.SCHEMA | 1 | 50 | 2.0 |
| volume-quota | energy_commerce_retail_media.bronze | SecurableType.SCHEMA | 1 | 50 | 2.0 |
| volume-quota | samples.databricks | SecurableType.SCHEMA | 1 | 50 | 2.0 |
| privilege-quota | energy_commerce_retail_media.quality | SecurableType.SCHEMA | 4 | 200 | 2.0 |
| privilege-quota | system | SecurableType.CATALOG | 2 | 200 | 1.0 |
| privilege-quota | system.lakeflow | SecurableType.SCHEMA | 2 | 200 | 1.0 |
| privilege-quota | system.sharing | SecurableType.SCHEMA | 2 | 200 | 1.0 |
| privilege-quota | system.lakeflow_pipeline_events_preview | SecurableType.SCHEMA | 2 | 200 | 1.0 |
| privilege-quota | energy_commerce_retail_media.energy_semantic | SecurableType.SCHEMA | 2 | 200 | 1.0 |
| table-quota | energy_commerce_retail_media.energy_semantic | SecurableType.SCHEMA | 1 | 100 | 1.0 |
| table-quota | system.__internal_data_quality_monitoring | SecurableType.SCHEMA | 1 | 100 | 1.0 |
| privilege-quota | system.access | SecurableType.SCHEMA | 2 | 200 | 1.0 |
| table-quota | energy_commerce_retail_media.commerce_silver_reference | SecurableType.SCHEMA | 1 | 100 | 1.0 |
| privilege-quota | system.tags | SecurableType.SCHEMA | 2 | 200 | 1.0 |
| privilege-quota | energy_commerce_retail_media.commerce_semantic | SecurableType.SCHEMA | 2 | 200 | 1.0 |
| table-quota | energy_commerce_retail_media.commerce_semantic | SecurableType.SCHEMA | 1 | 100 | 1.0 |
| privilege-quota | system.query | SecurableType.SCHEMA | 2 | 200 | 1.0 |
| privilege-quota | system.storage | SecurableType.SCHEMA | 2 | 200 | 1.0 |
| privilege-quota | system.billing | SecurableType.SCHEMA | 2 | 200 | 1.0 |
| privilege-quota | system.mlflow | SecurableType.SCHEMA | 2 | 200 | 1.0 |
| privilege-quota | energy_commerce_retail_media.commerce_analytics | SecurableType.SCHEMA | 2 | 200 | 1.0 |
| privilege-quota | system.compute | SecurableType.SCHEMA | 2 | 200 | 1.0 |
| privilege-quota | system.ai_gateway | SecurableType.SCHEMA | 2 | 200 | 1.0 |
| table-quota | system.data_quality_monitoring | SecurableType.SCHEMA | 1 | 100 | 1.0 |
| privilege-quota | system.alert | SecurableType.SCHEMA | 2 | 200 | 1.0 |
| privilege-quota | system.serving | SecurableType.SCHEMA | 2 | 200 | 1.0 |
| privilege-quota | energy_commerce_retail_media.ml_models | SecurableType.SCHEMA | 2 | 200 | 1.0 |
| privilege-quota | system.data_quality_monitoring | SecurableType.SCHEMA | 1 | 200 | 0.5 |
| schema-quota | samples | SecurableType.CATALOG | 0 | 50 | 0.0 |
| external_location-quota | 1a00c03f-d59a-4486-bdcc-33a6b812f863 | SecurableType.METASTORE | 0 | 50 | 0.0 |
| recipient-quota | 1a00c03f-d59a-4486-bdcc-33a6b812f863 | SecurableType.METASTORE | 0 | 50 | 0.0 |
| registered-model-quota | 1a00c03f-d59a-4486-bdcc-33a6b812f863 | SecurableType.METASTORE | 0 | 100 | 0.0 |
| share-quota | 1a00c03f-d59a-4486-bdcc-33a6b812f863 | SecurableType.METASTORE | 0 | 50 | 0.0 |
| storage_credential-quota | 1a00c03f-d59a-4486-bdcc-33a6b812f863 | SecurableType.METASTORE | 0 | 1,000 | 0.0 |
| function-quota | energy_commerce_retail_media.energy_silver_reference | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.energy_silver_reference | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | system.lakeflow | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | system.lakeflow | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | system.lakeflow | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | samples.accuweather | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | samples.accuweather | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | samples.accuweather | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | samples.sec | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | samples.sec | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | samples.sec | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.genomics_analytics | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.genomics_analytics | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.energy_gold | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.energy_gold | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | system.sharing | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | system.sharing | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | system.sharing | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.commerce_ml_models | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.commerce_ml_models | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | system.lakeflow_pipeline_events_preview | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | system.lakeflow_pipeline_events_preview | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | system.lakeflow_pipeline_events_preview | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | system.__internal_functions | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | system.__internal_functions | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | system.__internal_functions | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.energy_semantic | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.energy_semantic | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | samples.bakehouse | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | samples.bakehouse | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | samples.bakehouse | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | system.__internal_data_quality_monitoring | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | system.__internal_data_quality_monitoring | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.commerce_silver | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.commerce_silver | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | system.access | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | system.access | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | system.access | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.commerce_silver_reference | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.commerce_silver_reference | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | system.tags | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | system.tags | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | system.tags | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | samples.tpcds_sf1000 | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | samples.tpcds_sf1000 | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | samples.tpcds_sf1000 | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.knowledge | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.knowledge | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | system.ai | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | system.ai | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | system.ai | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.default | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.default | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | energy_commerce_retail_media.default | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.commerce_semantic | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.commerce_semantic | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | system.query | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | system.query | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | system.query | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | system.storage | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | system.storage | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | system.storage | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | samples.tpch_sf100 | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | samples.tpch_sf100 | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | samples.tpch_sf100 | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | system.billing | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | system.billing | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | system.billing | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.genomics_ml_datasets | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.genomics_ml_datasets | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.shared_conformed | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.shared_conformed | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.commerce_ml_datasets | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.commerce_ml_datasets | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | system.mlflow | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | system.mlflow | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | system.mlflow | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | samples.tpch_sf10 | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | samples.tpch_sf10 | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | samples.tpch_sf10 | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.genomics_gold | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.genomics_gold | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | samples.tpch_sf1000 | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | samples.tpch_sf1000 | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | samples.tpch_sf1000 | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.commerce_gold | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.commerce_gold | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.commerce_analytics | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.commerce_analytics | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | samples.wanderbricks | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | samples.wanderbricks | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | samples.wanderbricks | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | samples.healthverity | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | samples.healthverity | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | samples.healthverity | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.energy_ml_datasets | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.energy_ml_datasets | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.genomics_bronze | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.genomics_bronze | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| volume-quota | energy_commerce_retail_media.genomics_bronze | SecurableType.SCHEMA | 0 | 50 | 0.0 |
| function-quota | energy_commerce_retail_media.energy_silver | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.energy_silver | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.ml_datasets_shared | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.ml_datasets_shared | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.energy_analytics | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.energy_analytics | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | system.compute | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | system.compute | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | system.compute | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | samples.tpch | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | samples.tpch | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | samples.tpch | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | system.__internal_ai | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | system.__internal_ai | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | system.__internal_ai | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.bronze | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.bronze | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | system.ai_gateway | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | system.ai_gateway | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | system.ai_gateway | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.genomics_silver_reference | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.genomics_silver_reference | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | samples.tpcds_sf1 | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | samples.tpcds_sf1 | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | samples.tpcds_sf1 | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | system.data_quality_monitoring | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | system.data_quality_monitoring | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.genomics_silver | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.genomics_silver | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | system.alert | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | system.alert | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | system.alert | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | system.serving | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | system.serving | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | system.serving | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.ml_models | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.ml_models | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | samples.nyctaxi | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | samples.nyctaxi | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | samples.nyctaxi | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | samples.tpch_sf1 | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | samples.tpch_sf1 | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | samples.tpch_sf1 | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | samples.databricks | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | samples.databricks | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| table-quota | samples.databricks | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.quality | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.quality | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| function-quota | energy_commerce_retail_media.energy_ml_models | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| registered-model-quota | energy_commerce_retail_media.energy_ml_models | SecurableType.SCHEMA | 0 | 100 | 0.0 |
| privilege-quota | energy_commerce_retail_media.quality.quality_audit_log | SecurableType.TABLE | 0 | 100 | 0.0 |
| privilege-quota | system.data_quality_monitoring.table_results | SecurableType.TABLE | 0 | 100 | 0.0 |

## Per schema

| schema | tables | views | rows | files | size_bytes | size_mb | size_gb |
| --- | --- | --- | --- | --- | --- | --- | --- |
| bronze | 35 | 0 | 361,301,383 | 130 | 4,145,975,496 | 3953.91 | 3.861 |
| commerce_analytics | 3 | 0 | 11,291 | 3 | 199,029 | 0.19 | 0.0 |
| commerce_gold | 4 | 0 | 137,977,029 | 212 | 24,742,143,600 | 23595.95 | 23.043 |
| commerce_ml_datasets | 7 | 0 | 103,573,888 | 121 | 8,557,124,952 | 8160.71 | 7.969 |
| commerce_ml_models | 0 | 0 | 0 | 0 | 0 | 0.0 | 0.0 |
| commerce_semantic | 1 | 0 | 3 | 1 | 4,260 | 0.0 | 0.0 |
| commerce_silver | 5 | 0 | 143,426,687 | 295 | 16,383,896,048 | 15624.9 | 15.259 |
| commerce_silver_reference | 0 | 0 | 0 | 0 | 0 | 0.0 | 0.0 |
| default | 0 | 0 | 0 | 0 | 0 | 0.0 | 0.0 |
| energy_analytics | 16 | 0 | 60,516,244 | 61 | 1,061,196,591 | 1012.04 | 0.988 |
| energy_gold | 21 | 0 | 214,654,365 | 113 | 9,615,467,630 | 9170.02 | 8.955 |
| energy_ml_datasets | 13 | 0 | 13,623,515 | 33 | 240,425,299 | 229.29 | 0.224 |
| energy_ml_models | 0 | 0 | 0 | 0 | 0 | 0.0 | 0.0 |
| energy_semantic | 1 | 0 | 5 | 1 | 4,468 | 0.0 | 0.0 |
| energy_silver | 16 | 0 | 99,687,762 | 139 | 7,535,936,869 | 7186.83 | 7.018 |
| energy_silver_reference | 12 | 0 | 24,109,630 | 46 | 827,438,562 | 789.11 | 0.771 |
| genomics_analytics | 6 | 0 | 3,916,305 | 17 | 50,726,210 | 48.38 | 0.047 |
| genomics_bronze | 85 | 0 | 322,522,486 | 308 | 10,188,043,577 | 9716.08 | 9.488 |
| genomics_gold | 26 | 0 | 626,598,181 | 465 | 47,516,174,813 | 45314.96 | 44.253 |
| genomics_ml_datasets | 3 | 0 | 25,411,678 | 24 | 2,065,750,025 | 1970.05 | 1.924 |
| genomics_silver | 27 | 0 | 571,844,029 | 401 | 25,411,672,798 | 24234.46 | 23.666 |
| genomics_silver_reference | 11 | 0 | 704,841,044 | 542 | 30,668,813,100 | 29248.06 | 28.563 |
| knowledge | 6 | 0 | 27,195,278 | 107 | 5,978,870,204 | 5701.89 | 5.568 |
| ml_datasets_shared | 7 | 0 | 243,364,938 | 143 | 7,090,884,267 | 6762.39 | 6.604 |
| ml_models | 19 | 0 | 4,930,612 | 151 | 76,733,668 | 73.18 | 0.071 |
| quality | 4 | 0 | 1,900,734 | 61 | 39,319,977 | 37.5 | 0.037 |
| shared_conformed | 11 | 0 | 5,883,384 | 13 | 230,068,833 | 219.41 | 0.214 |

## Per table

| schema | table | type | rows | files | size_bytes | size_mb | last_modified | error |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| bronze | dwd_device_instrument | MANAGED | 4,043 | 1 | 32,405 | 0.03 | 2026-11-23 21:54:44 |  |
| bronze | dwd_measurement | MANAGED | 177,440,391 | 10 | 446,144,191 | 425.48 | 2026-11-23 22:20:20 |  |
| bronze | dwd_missing_value_periods | MANAGED | 5,902,309 | 5 | 20,347,783 | 19.41 | 2026-11-23 21:54:56 |  |
| bronze | dwd_parameter_unit | MANAGED | 5,511 | 1 | 25,048 | 0.02 | 2026-11-23 21:54:48 |  |
| bronze | dwd_station_geography | MANAGED | 228 | 1 | 6,458 | 0.01 | 2026-11-23 21:54:35 |  |
| bronze | dwd_station_name_history | MANAGED | 106 | 1 | 3,067 | 0.0 | 2026-11-23 21:54:40 |  |
| bronze | energy_charts_physical_flows | MANAGED | 853,584 | 3 | 1,910,744 | 1.82 | 2026-11-12 12:18:49 |  |
| bronze | eurostat_retail_trade | MANAGED | 105 | 1 | 2,896 | 0.0 | 2026-11-23 21:55:00 |  |
| bronze | ga4_events | MANAGED | 779,485 | 16 | 27,084,269 | 25.83 | 2026-11-24 16:35:47 |  |
| bronze | honda_iot_meter | MANAGED | 20,507,677 | 4 | 221,410,704 | 211.15 | 2026-11-12 14:20:54 |  |
| bronze | honda_iot_weather | MANAGED | 3,419,507 | 3 | 41,263,292 | 39.35 | 2026-08-27 13:33:35 |  |
| bronze | mastr_anlagen_eeg | MANAGED | 9,244,933 | 10 | 276,585,387 | 263.77 | 2026-11-18 14:58:41 |  |
| bronze | mastr_anlagen_kwk | MANAGED | 92,514 | 1 | 2,726,455 | 2.6 | 2026-09-05 23:07:40 |  |
| bronze | mastr_anlagen_speicher | MANAGED | 2,774,446 | 2 | 66,845,962 | 63.75 | 2026-11-23 22:03:42 |  |
| bronze | mastr_bilanzierungsgebiete | MANAGED | 1,118 | 1 | 17,096 | 0.02 | 2026-09-05 23:09:11 |  |
| bronze | mastr_code_lookup | MANAGED | 1,789 | 5 | 31,440 | 0.03 | 2026-11-12 14:20:57 |  |
| bronze | mastr_einheiten | MANAGED | 9,407,161 | 11 | 568,467,643 | 542.13 | 2026-11-19 01:03:07 |  |
| bronze | mastr_einheiten_aenderung_netzbetreiberzuordnungen | MANAGED | 226,405 | 1 | 5,739,794 | 5.47 | 2026-09-05 23:09:28 |  |
| bronze | mastr_einheiten_genehmigung | MANAGED | 37,722 | 1 | 1,679,203 | 1.6 | 2026-09-05 23:09:15 |  |
| bronze | mastr_ertuechtigungen | MANAGED | 1,681 | 1 | 33,971 | 0.03 | 2026-09-05 23:09:31 |  |
| bronze | mastr_geloeschte_deaktivierte_einheiten | MANAGED | 244,295 | 1 | 2,923,502 | 2.79 | 2026-09-05 23:09:19 |  |
| bronze | mastr_geloeschte_deaktivierte_marktakteure | MANAGED | 285,315 | 1 | 3,242,833 | 3.09 | 2026-09-05 23:09:23 |  |
| bronze | mastr_katalogkategorien | MANAGED | 123 | 1 | 2,399 | 0.0 | 2026-09-05 23:09:37 |  |
| bronze | mastr_lokationen | MANAGED | 6,776,394 | 4 | 225,606,612 | 215.16 | 2026-11-12 19:32:00 |  |
| bronze | mastr_marktakteure | MANAGED | 5,616,357 | 2 | 120,078,875 | 114.52 | 2026-11-12 19:35:41 |  |
| bronze | mastr_marktakteure_und_rollen | MANAGED | 15,689 | 1 | 338,068 | 0.32 | 2026-09-05 23:08:25 |  |
| bronze | mastr_netzanschlusspunkte | MANAGED | 5,867,232 | 4 | 190,773,766 | 181.94 | 2026-11-12 19:36:05 |  |
| bronze | mastr_netze | MANAGED | 1,740 | 1 | 52,014 | 0.05 | 2026-09-05 23:08:50 |  |
| bronze | power_plant_capacity_additions | MANAGED | 17 | 1 | 3,093 | 0.0 | 2026-09-25 17:49:03 |  |
| bronze | power_plant_list | MANAGED | 2,613 | 1 | 98,131 | 0.09 | 2026-09-22 00:22:21 |  |
| bronze | redispatch_measures | MANAGED | 40,118 | 1 | 392,398 | 0.37 | 2026-09-05 11:15:45 |  |
| bronze | rees46_events | MANAGED | 109,950,743 | 29 | 1,913,065,542 | 1824.44 | 2026-09-06 20:43:12 |  |
| bronze | smard_balancing_energy | MANAGED | 71,136 | 1 | 354,586 | 0.34 | 2026-11-12 12:07:36 |  |
| bronze | smard_commercial_exchanges | MANAGED | 71,136 | 1 | 2,966,729 | 2.83 | 2026-11-12 12:07:42 |  |
| bronze | smard_energy_timeseries | MANAGED | 1,657,760 | 2 | 5,719,140 | 5.45 | 2026-11-12 12:07:30 |  |
| commerce_analytics | customer_journey_daily | MANAGED | 155 | 1 | 14,266 | 0.01 | 2026-11-19 21:34:45 |  |
| commerce_analytics | ga4_demand_daily | MANAGED | 7,408 | 1 | 38,813 | 0.04 | 2026-11-19 21:34:06 |  |
| commerce_analytics | product_demand_daily | MANAGED | 3,728 | 1 | 145,950 | 0.14 | 2026-11-19 21:35:28 |  |
| commerce_gold | commerce_event | MANAGED | 110,599,465 | 103 | 21,176,346,634 | 20195.34 | 2026-11-24 00:22:23 |  |
| commerce_gold | commerce_event_item | MANAGED | 3,982,732 | 4 | 208,511,549 | 198.85 | 2026-11-21 22:19:29 |  |
| commerce_gold | commerce_product | MANAGED | 208,268 | 1 | 21,376,501 | 20.39 | 2026-11-21 22:00:39 |  |
| commerce_gold | commerce_session | MANAGED | 23,186,564 | 104 | 3,335,908,916 | 3181.37 | 2026-11-22 22:53:10 |  |
| commerce_ml_datasets | dataset_lapse | MANAGED | 3,055,882 | 2 | 60,451,726 | 57.65 | 2026-11-23 14:29:18 |  |
| commerce_ml_datasets | dataset_next_item_sequence | MANAGED | 87,340,475 | 107 | 7,903,999,770 | 7537.84 | 2026-11-24 04:22:59 |  |
| commerce_ml_datasets | dataset_session_purchase | MANAGED | 6,717,647 | 6 | 536,095,864 | 511.26 | 2026-11-23 14:30:03 |  |
| commerce_ml_datasets | features_product_asof_ga4 | MANAGED | 51,188 | 1 | 196,965 | 0.19 | 2026-11-22 12:25:50 |  |
| commerce_ml_datasets | features_product_asof_rees46 | MANAGED | 5,001,492 | 2 | 12,682,941 | 12.1 | 2026-11-22 12:25:27 |  |
| commerce_ml_datasets | target_basket_ga4 | MANAGED | 4,446 | 1 | 144,103 | 0.14 | 2026-11-22 12:37:04 |  |
| commerce_ml_datasets | target_basket_rees46 | MANAGED | 1,402,758 | 2 | 43,553,583 | 41.54 | 2026-11-22 12:36:38 |  |
| commerce_semantic | metric_definition | MANAGED | 3 | 1 | 4,260 | 0.0 | 2026-09-28 15:25:22 |  |
| commerce_silver | commerce_event | MANAGED | 110,599,465 | 207 | 12,537,632,734 | 11956.82 | 2026-11-22 13:47:44 |  |
| commerce_silver | commerce_product | MANAGED | 208,268 | 1 | 20,923,969 | 19.95 | 2026-11-19 18:37:34 |  |
| commerce_silver | commerce_session | MANAGED | 23,186,564 | 79 | 3,181,747,916 | 3034.35 | 2026-11-19 18:16:49 |  |
| commerce_silver | commerce_user | MANAGED | 5,449,658 | 6 | 472,581,873 | 450.69 | 2026-11-19 18:18:07 |  |
| commerce_silver | ga4_event_item | MANAGED | 3,982,732 | 2 | 171,009,556 | 163.09 | 2026-11-22 13:47:28 |  |
| energy_analytics | capacity_margin_yearly | MANAGED | 12 | 1 | 3,852 | 0.0 | 2026-11-19 22:01:22 |  |
| energy_analytics | capacity_registry | MANAGED | 8,971,821 | 6 | 107,046,018 | 102.09 | 2026-11-19 22:00:24 |  |
| energy_analytics | capacity_utilisation_monthly | MANAGED | 3,369 | 1 | 52,006 | 0.05 | 2026-11-19 22:01:13 |  |
| energy_analytics | cross_border_trade | MANAGED | 1,066,971 | 1 | 13,186,940 | 12.58 | 2026-11-19 21:58:14 |  |
| energy_analytics | extreme_weather_stress_event | MANAGED | 96,757 | 6 | 549,718 | 0.52 | 2026-11-19 22:05:09 |  |
| energy_analytics | fleet_lifecycle_event | MANAGED | 10,252,521 | 19 | 503,894,839 | 480.55 | 2026-11-23 18:30:36 |  |
| energy_analytics | forecast_accuracy | MANAGED | 19,000 | 1 | 422,128 | 0.4 | 2026-11-19 21:58:28 |  |
| energy_analytics | generation_by_carrier_daily | MANAGED | 89,706 | 1 | 591,314 | 0.56 | 2026-11-19 21:57:46 |  |
| energy_analytics | grid_intervention_daily | MANAGED | 11,666 | 1 | 104,817 | 0.1 | 2026-11-19 21:58:23 |  |
| energy_analytics | market_price_daily | MANAGED | 14,582 | 1 | 676,008 | 0.64 | 2026-11-19 21:57:59 |  |
| energy_analytics | market_timeseries | MANAGED | 335,197 | 1 | 5,358,116 | 5.11 | 2026-11-19 21:57:53 |  |
| energy_analytics | negative_price_event | MANAGED | 2,116 | 1 | 48,275 | 0.05 | 2026-11-19 21:58:07 |  |
| energy_analytics | operator_concentration | MANAGED | 39,546,987 | 15 | 427,274,501 | 407.48 | 2026-11-19 22:02:08 |  |
| energy_analytics | site_energy_weather_daily | MANAGED | 50,952 | 4 | 769,047 | 0.73 | 2026-11-19 22:02:22 |  |
| energy_analytics | support_scheme_buildout | MANAGED | 5,695 | 1 | 49,917 | 0.05 | 2026-11-19 22:02:15 |  |
| energy_analytics | weather_energy_daily | MANAGED | 48,892 | 1 | 1,169,095 | 1.11 | 2026-11-19 22:04:03 |  |
| energy_gold | balancing_area | MANAGED | 1,118 | 1 | 60,454 | 0.06 | 2026-11-19 21:28:51 |  |
| energy_gold | capacity_plan | MANAGED | 52 | 1 | 6,559 | 0.01 | 2026-11-19 21:35:29 |  |
| energy_gold | channel_reading | MANAGED | 63,243,582 | 5 | 547,895,025 | 522.51 | 2026-11-23 15:50:53 |  |
| energy_gold | energy_component | MANAGED | 16,616,582 | 10 | 1,117,706,079 | 1065.93 | 2026-11-21 22:02:55 |  |
| energy_gold | entity_link | MANAGED | 73,354,195 | 21 | 2,868,651,157 | 2735.76 | 2026-11-23 15:51:22 |  |
| energy_gold | forecast_actual | MANAGED | 156,746 | 1 | 18,951,130 | 18.07 | 2026-11-21 22:03:10 |  |
| energy_gold | generation_unit | MANAGED | 9,407,161 | 22 | 1,146,330,939 | 1093.23 | 2026-11-23 01:30:43 |  |
| energy_gold | grid_connection_point | MANAGED | 5,867,232 | 7 | 357,104,077 | 340.56 | 2026-11-24 13:19:59 |  |
| energy_gold | grid_intervention_event | MANAGED | 39,550 | 1 | 3,938,551 | 3.76 | 2026-11-22 02:59:35 |  |
| energy_gold | grid_location | MANAGED | 6,776,394 | 2 | 249,974,041 | 238.39 | 2026-11-24 16:02:31 |  |
| energy_gold | grid_network | MANAGED | 1,740 | 1 | 69,840 | 0.07 | 2026-11-19 21:28:42 |  |
| energy_gold | iot_device | MANAGED | 198,164 | 1 | 18,116,378 | 17.28 | 2026-11-21 22:00:49 |  |
| energy_gold | market_actor | MANAGED | 5,616,357 | 3 | 144,743,529 | 138.04 | 2026-11-24 16:02:23 |  |
| energy_gold | market_actor_role | MANAGED | 15,689 | 1 | 1,530,710 | 1.46 | 2026-11-19 21:35:40 |  |
| energy_gold | market_price | MANAGED | 335,197 | 1 | 25,020,998 | 23.86 | 2026-11-21 22:02:11 |  |
| energy_gold | plant_sensor_observation | MANAGED | 9,568 | 1 | 740,896 | 0.71 | 2026-11-21 22:01:16 |  |
| energy_gold | power_plant | MANAGED | 2,613 | 1 | 474,731 | 0.45 | 2026-11-19 21:34:38 |  |
| energy_gold | storage_plant | MANAGED | 2,774,446 | 1 | 73,028,772 | 69.65 | 2026-11-19 21:18:44 |  |
| energy_gold | unit_event | MANAGED | 795,418 | 3 | 38,557,112 | 36.77 | 2026-11-23 18:30:26 |  |
| energy_gold | weather_daily | MANAGED | 2,144,456 | 3 | 150,132,921 | 143.18 | 2026-11-23 15:51:08 |  |
| energy_gold | weather_observation | MANAGED | 27,298,105 | 26 | 2,852,433,731 | 2720.29 | 2026-11-21 22:04:02 |  |
| energy_ml_datasets | dataset_honda | MANAGED | 867,297 | 2 | 49,034,076 | 46.76 | 2026-11-23 14:29:26 |  |
| energy_ml_datasets | dataset_market_daily | MANAGED | 37,229 | 3 | 4,390,254 | 4.19 | 2026-11-23 14:28:57 |  |
| energy_ml_datasets | dataset_market_quarter_hour | MANAGED | 69,216 | 1 | 8,230,163 | 7.85 | 2026-11-23 14:29:11 |  |
| energy_ml_datasets | dataset_redispatch | MANAGED | 50,622 | 2 | 2,855,307 | 2.72 | 2026-11-23 14:28:42 |  |
| energy_ml_datasets | dataset_self_supervised_rl | MANAGED | 2,172,086 | 3 | 31,689,281 | 30.22 | 2026-11-23 14:29:41 |  |
| energy_ml_datasets | dataset_unit | MANAGED | 9,485,531 | 6 | 136,515,150 | 130.19 | 2026-11-23 14:29:56 |  |
| energy_ml_datasets | features_calendar_market_area | MANAGED | 32,868 | 1 | 117,612 | 0.11 | 2026-11-22 12:25:28 |  |
| energy_ml_datasets | features_hub_height_fill | MANAGED | 35,266 | 8 | 295,113 | 0.28 | 2026-11-22 12:53:16 |  |
| energy_ml_datasets | features_pv_forecast_daily | MANAGED | 2,965 | 1 | 44,822 | 0.04 | 2026-11-22 12:26:30 |  |
| energy_ml_datasets | features_station_day_radiation | MANAGED | 43,383 | 1 | 202,537 | 0.19 | 2026-11-22 13:07:00 |  |
| energy_ml_datasets | features_station_day_wind | MANAGED | 80,331 | 1 | 564,637 | 0.54 | 2026-11-22 13:06:40 |  |
| energy_ml_datasets | target_honda_increment | MANAGED | 723,470 | 3 | 6,340,802 | 6.05 | 2026-11-22 12:53:50 |  |
| energy_ml_datasets | target_load | MANAGED | 23,251 | 1 | 145,545 | 0.14 | 2026-11-22 12:27:10 |  |
| energy_semantic | metric_definition | MANAGED | 5 | 1 | 4,468 | 0.0 | 2026-09-28 15:25:45 |  |
| energy_silver | device_telemetry_snapshot | MANAGED | 198,164 | 1 | 18,116,162 | 17.28 | 2026-11-22 14:47:14 |  |
| energy_silver | energy_component | MANAGED | 16,616,582 | 12 | 1,119,024,081 | 1067.18 | 2026-11-22 14:48:16 |  |
| energy_silver | energy_observation | MANAGED | 3,753,157 | 8 | 510,034,731 | 486.41 | 2026-11-22 14:50:09 |  |
| energy_silver | generation_unit | MANAGED | 9,407,161 | 17 | 879,745,357 | 838.99 | 2026-11-22 14:49:39 |  |
| energy_silver | grid_intervention_event | MANAGED | 39,550 | 1 | 3,938,314 | 3.76 | 2026-11-19 20:51:33 |  |
| energy_silver | plant_operating_sample | MANAGED | 9,568 | 1 | 740,683 | 0.71 | 2026-11-19 20:51:01 |  |
| energy_silver | power_plant_capacity_plan | MANAGED | 13 | 1 | 6,453 | 0.01 | 2026-11-19 20:49:59 |  |
| energy_silver | power_plant_register | MANAGED | 2,613 | 1 | 213,139 | 0.2 | 2026-11-19 20:49:35 |  |
| energy_silver | register_change_event | MANAGED | 756,015 | 3 | 36,615,772 | 34.92 | 2026-11-22 14:49:32 |  |
| energy_silver | register_link | MANAGED | 27,311,082 | 25 | 1,292,111,648 | 1232.25 | 2026-11-24 16:02:39 |  |
| energy_silver | storage_plant | MANAGED | 2,774,446 | 2 | 73,032,251 | 69.65 | 2026-11-19 20:19:00 |  |
| energy_silver | support_registration | MANAGED | 9,337,447 | 11 | 618,202,067 | 589.56 | 2026-11-21 00:31:39 |  |
| energy_silver | unit_authorisation | MANAGED | 37,722 | 1 | 1,833,895 | 1.75 | 2026-11-19 20:16:30 |  |
| energy_silver | unit_repowering | MANAGED | 1,681 | 1 | 35,975 | 0.03 | 2026-11-19 20:16:47 |  |
| energy_silver | weather_daily | MANAGED | 2,144,456 | 3 | 150,131,286 | 143.18 | 2026-11-22 14:47:04 |  |
| energy_silver | weather_observation | MANAGED | 27,298,105 | 51 | 2,832,155,055 | 2700.95 | 2026-11-23 09:54:47 |  |
| energy_silver_reference | balancing_area | MANAGED | 1,118 | 1 | 60,191 | 0.06 | 2026-11-19 20:19:37 |  |
| energy_silver_reference | energy_carrier_map | MANAGED | 105 | 8 | 36,852 | 0.04 | 2026-11-19 20:54:53 |  |
| energy_silver_reference | grid_connection_point | MANAGED | 5,867,232 | 4 | 202,269,521 | 192.9 | 2026-11-24 16:02:45 |  |
| energy_silver_reference | grid_location | MANAGED | 6,776,394 | 4 | 249,926,628 | 238.35 | 2026-11-24 16:02:07 |  |
| energy_silver_reference | grid_network | MANAGED | 1,740 | 1 | 69,589 | 0.07 | 2026-11-19 20:18:08 |  |
| energy_silver_reference | market_actor | MANAGED | 5,616,357 | 6 | 144,601,430 | 137.9 | 2026-11-24 16:02:52 |  |
| energy_silver_reference | market_actor_role | MANAGED | 15,689 | 1 | 435,995 | 0.42 | 2026-11-19 20:17:34 |  |
| energy_silver_reference | reference_code_list | MANAGED | 1,967 | 9 | 131,332 | 0.13 | 2026-11-19 19:52:00 |  |
| energy_silver_reference | weather_location | MANAGED | 80 | 4 | 30,840 | 0.03 | 2026-11-19 18:03:45 |  |
| energy_silver_reference | weather_missing_value_period | MANAGED | 5,817,858 | 3 | 229,391,427 | 218.76 | 2026-11-22 13:46:50 |  |
| energy_silver_reference | weather_missingness_reconciliation | MANAGED | 1,391 | 1 | 61,924 | 0.06 | 2026-11-19 18:16:52 |  |
| energy_silver_reference | weather_station_period | MANAGED | 9,699 | 4 | 422,833 | 0.4 | 2026-11-19 18:05:34 |  |
| genomics_analytics | classification_change_summary | MANAGED | 193 | 1 | 5,224 | 0.0 | 2026-11-19 22:13:02 |  |
| genomics_analytics | disease_overview | MANAGED | 266,194 | 1 | 14,433,942 | 13.77 | 2026-11-19 22:12:23 |  |
| genomics_analytics | gene_prioritization | MANAGED | 228,120 | 1 | 2,133,270 | 2.03 | 2026-11-19 22:12:15 |  |
| genomics_analytics | genetic_test_panel | MANAGED | 305,239 | 1 | 1,455,178 | 1.39 | 2026-11-19 22:12:29 |  |
| genomics_analytics | population_risk | MANAGED | 533,351 | 12 | 1,630,509 | 1.55 | 2026-11-19 22:12:57 |  |
| genomics_analytics | variant_review_queue | MANAGED | 2,583,208 | 1 | 31,068,087 | 29.63 | 2026-11-19 22:12:44 |  |
| genomics_bronze | aact_browse_terms | MANAGED | 7,015,864 | 6 | 16,179,060 | 15.43 | 2026-11-19 19:49:40 |  |
| genomics_bronze | aact_eligibilities | MANAGED | 611,552 | 6 | 291,790,276 | 278.27 | 2026-11-20 12:12:40 |  |
| genomics_bronze | aact_interventions | MANAGED | 1,036,427 | 1 | 15,121,614 | 14.42 | 2026-11-19 19:50:04 |  |
| genomics_bronze | aact_studies | MANAGED | 612,543 | 12 | 82,862,852 | 79.02 | 2026-11-21 01:51:14 |  |
| genomics_bronze | aact_study_terms | MANAGED | 2,765,665 | 2 | 24,357,906 | 23.23 | 2026-11-19 19:49:49 |  |
| genomics_bronze | acmg_secondary_findings_genes | MANAGED | 94 | 1 | 4,691 | 0.0 | 2026-11-12 14:09:02 |  |
| genomics_bronze | alphafold_residue_confidence | MANAGED | 15,088,388 | 7 | 30,155,041 | 28.76 | 2026-11-12 14:09:10 |  |
| genomics_bronze | alphamissense_variant_scores | MANAGED | 2,534,806 | 4 | 21,012,309 | 20.04 | 2026-11-04 22:34:22 |  |
| genomics_bronze | biogrid_interactions | MANAGED | 1,441,421 | 1 | 35,129,924 | 33.5 | 2026-11-13 12:13:03 |  |
| genomics_bronze | cadd_scores | MANAGED | 9,230,587 | 4 | 32,062,401 | 30.58 | 2026-11-04 20:34:09 |  |
| genomics_bronze | chembl_compounds | MANAGED | 2,897,819 | 5 | 264,141,214 | 251.9 | 2026-11-12 19:34:53 |  |
| genomics_bronze | chembl_uniprot_mapping | MANAGED | 17,257 | 1 | 253,378 | 0.24 | 2026-11-04 20:34:23 |  |
| genomics_bronze | clingen_dosage_sensitivity | MANAGED | 1,539 | 1 | 60,798 | 0.06 | 2026-11-04 20:34:29 |  |
| genomics_bronze | clingen_gene_validity | MANAGED | 3,717 | 1 | 200,789 | 0.19 | 2026-11-04 20:34:33 |  |
| genomics_bronze | clinvar_variant_position_grch37 | MANAGED | 4,696,816 | 3 | 30,264,819 | 28.86 | 2026-11-19 17:54:30 |  |
| genomics_bronze | clinvar_variant_summary | MANAGED | 23,468,593 | 21 | 1,371,309,896 | 1307.78 | 2026-11-19 14:58:29 |  |
| genomics_bronze | clinvar_variation_allele | MANAGED | 4,694,205 | 3 | 20,007,696 | 19.08 | 2026-11-12 14:10:10 |  |
| genomics_bronze | collectri_regulons | MANAGED | 43,175 | 1 | 556,897 | 0.53 | 2026-11-12 14:10:15 |  |
| genomics_bronze | cpic_pairs | MANAGED | 574 | 1 | 13,556 | 0.01 | 2026-11-04 20:35:21 |  |
| genomics_bronze | dbvar_variants | MANAGED | 222,568 | 2 | 10,821,942 | 10.32 | 2026-11-04 20:35:26 |  |
| genomics_bronze | ddinter_interactions | MANAGED | 222,383 | 1 | 928,085 | 0.89 | 2026-11-12 14:10:19 |  |
| genomics_bronze | depmap_gene_effect | MANAGED | 25,088,674 | 5 | 225,352,043 | 214.91 | 2026-11-19 16:11:05 |  |
| genomics_bronze | depmap_models | MANAGED | 2,554 | 1 | 136,572 | 0.13 | 2026-11-12 14:10:37 |  |
| genomics_bronze | encode_ccre_registry | MANAGED | 2,348,854 | 3 | 30,035,097 | 28.64 | 2026-11-12 14:10:43 |  |
| genomics_bronze | encode_enhancer_gene_links | MANAGED | 66,102,715 | 28 | 1,264,427,746 | 1205.85 | 2026-11-19 16:11:21 |  |
| genomics_bronze | ensembl_compara_homologues | MANAGED | 246,152 | 2 | 4,006,502 | 3.82 | 2026-11-12 14:11:31 |  |
| genomics_bronze | faers_case_code | MANAGED | 3,123,990 | 2 | 13,174,195 | 12.56 | 2026-11-23 22:04:19 |  |
| genomics_bronze | faers_demo | MANAGED | 4,056,792 | 7 | 89,069,244 | 84.94 | 2026-11-23 21:53:39 |  |
| genomics_bronze | faers_drug | MANAGED | 18,867,948 | 4 | 199,068,405 | 189.85 | 2026-11-23 22:20:28 |  |
| genomics_bronze | faers_indi | MANAGED | 11,856,495 | 6 | 36,450,223 | 34.76 | 2026-11-23 21:54:07 |  |
| genomics_bronze | faers_reac | MANAGED | 14,178,184 | 6 | 45,304,336 | 43.21 | 2026-11-23 21:54:25 |  |
| genomics_bronze | faers_ther | MANAGED | 4,961,879 | 4 | 29,004,322 | 27.66 | 2026-11-23 21:54:36 |  |
| genomics_bronze | fda_pgx_biomarkers | MANAGED | 692 | 1 | 11,451 | 0.01 | 2026-11-04 20:36:22 |  |
| genomics_bronze | gene_ages_consensus | MANAGED | 20,331 | 1 | 235,259 | 0.22 | 2026-11-12 19:16:00 |  |
| genomics_bronze | genome_annotation | MANAGED | 7,095,945 | 2 | 34,094,631 | 32.52 | 2026-11-23 22:04:15 |  |
| genomics_bronze | genome_sequence_chunk | MANAGED | 3,103 | 10 | 847,955,150 | 808.67 | 2026-11-24 16:36:56 |  |
| genomics_bronze | gnomad_mtdna_variants | MANAGED | 18,164 | 1 | 168,860 | 0.16 | 2026-11-12 14:11:35 |  |
| genomics_bronze | gnomad_variants | MANAGED | 2,723,660 | 2 | 90,630,659 | 86.43 | 2026-11-04 22:19:05 |  |
| genomics_bronze | gtex_gene_median_tpm | MANAGED | 59,033 | 1 | 14,032,223 | 13.38 | 2026-11-04 20:36:52 |  |
| genomics_bronze | gtr_test_condition_gene | MANAGED | 512,630 | 1 | 6,034,927 | 5.76 | 2026-11-04 20:36:58 |  |
| genomics_bronze | hpo_genes_to_phenotype | MANAGED | 333,983 | 1 | 1,823,754 | 1.74 | 2026-11-04 20:37:02 |  |
| genomics_bronze | hpo_phenotype_annotations | MANAGED | 286,651 | 1 | 1,543,954 | 1.47 | 2026-11-04 20:37:07 |  |
| genomics_bronze | hpo_term_relations | MANAGED | 24,436 | 1 | 132,252 | 0.13 | 2026-11-04 20:37:16 |  |
| genomics_bronze | hpo_terms | MANAGED | 20,482 | 1 | 880,437 | 0.84 | 2026-11-04 20:37:11 |  |
| genomics_bronze | iedb_epitopes | MANAGED | 2,910,311 | 13 | 66,999,420 | 63.9 | 2026-11-12 14:11:43 |  |
| genomics_bronze | iedb_mhc_ligands | MANAGED | 5,777,975 | 2 | 115,813,362 | 110.45 | 2026-11-19 16:11:13 |  |
| genomics_bronze | long_range_benchmark_causal_eqtl | MANAGED | 97,922 | 1 | 1,261,892 | 1.2 | 2026-11-04 20:37:21 |  |
| genomics_bronze | long_range_benchmark_pathogenic | MANAGED | 2,361,125 | 2 | 64,806,212 | 61.8 | 2026-11-12 14:21:04 |  |
| genomics_bronze | maps_embeddings_metadata | MANAGED | 132,565 | 13 | 707,381,343 | 674.61 | 2026-11-04 20:37:40 |  |
| genomics_bronze | medgen_concepts | MANAGED | 753,372 | 2 | 16,167,188 | 15.42 | 2026-11-04 20:37:46 |  |
| genomics_bronze | medgen_relations | MANAGED | 1,159,163 | 2 | 13,214,044 | 12.6 | 2026-11-04 20:37:52 |  |
| genomics_bronze | ncbi_gene_gene2refseq | MANAGED | 849,265 | 2 | 12,248,195 | 11.68 | 2026-11-04 20:37:58 |  |
| genomics_bronze | ncbi_gene_gene_info | MANAGED | 193,962 | 1 | 4,718,514 | 4.5 | 2026-11-04 20:38:03 |  |
| genomics_bronze | ncbi_gene_mim2gene_medgen | MANAGED | 28,709 | 1 | 213,466 | 0.2 | 2026-11-04 20:38:07 |  |
| genomics_bronze | opentargets_association_direct | MANAGED | 14,748,313 | 43 | 3,290,961,906 | 3138.51 | 2026-11-23 22:04:03 |  |
| genomics_bronze | opentargets_disease | MANAGED | 45,896 | 1 | 6,483,652 | 6.18 | 2026-11-04 20:38:31 |  |
| genomics_bronze | opentargets_drug_mechanism_of_action | MANAGED | 6,092 | 1 | 267,810 | 0.26 | 2026-11-04 20:38:35 |  |
| genomics_bronze | opentargets_drug_molecule | MANAGED | 19,170 | 1 | 6,969,746 | 6.65 | 2026-11-04 20:38:40 |  |
| genomics_bronze | opentargets_evidence_gwas_credible_sets | MANAGED | 3,445,546 | 4 | 220,180,404 | 209.98 | 2026-11-13 00:10:34 |  |
| genomics_bronze | opentargets_target | MANAGED | 78,733 | 6 | 28,025,565 | 26.73 | 2026-11-04 20:38:57 |  |
| genomics_bronze | pharmgkb_drugs | MANAGED | 3,770 | 1 | 631,590 | 0.6 | 2026-11-04 20:39:01 |  |
| genomics_bronze | pharmgkb_genes | MANAGED | 25,041 | 1 | 2,792,070 | 2.66 | 2026-11-04 20:39:06 |  |
| genomics_bronze | pharmgkb_relationships | MANAGED | 246,714 | 1 | 2,198,300 | 2.1 | 2026-11-04 20:39:10 |  |
| genomics_bronze | pharmgkb_variants | MANAGED | 7,692 | 1 | 961,390 | 0.92 | 2026-11-04 20:39:14 |  |
| genomics_bronze | proteingym_reference | MANAGED | 2,621 | 2 | 1,129,178 | 1.08 | 2026-11-12 14:21:14 |  |
| genomics_bronze | proteingym_substitutions | MANAGED | 548,557 | 2 | 4,871,643 | 4.65 | 2026-11-12 14:21:18 |  |
| genomics_bronze | reactome_pathway_mappings | MANAGED | 320,280 | 1 | 877,930 | 0.84 | 2026-11-04 20:39:43 |  |
| genomics_bronze | reactome_pathway_relations | MANAGED | 2,905 | 1 | 18,123 | 0.02 | 2026-11-04 20:39:47 |  |
| genomics_bronze | reactome_pathways | MANAGED | 2,889 | 1 | 50,087 | 0.05 | 2026-11-04 20:39:38 |  |
| genomics_bronze | reference_small | MANAGED | 38,255 | 3 | 1,006,409 | 0.96 | 2026-11-24 16:45:16 |  |
| genomics_bronze | spectralbio_variants | MANAGED | 867 | 1 | 14,103 | 0.01 | 2026-11-04 20:39:56 |  |
| genomics_bronze | string_protein_aliases | MANAGED | 3,889,207 | 4 | 15,118,314 | 14.42 | 2026-11-04 20:40:03 |  |
| genomics_bronze | string_protein_info | MANAGED | 19,699 | 1 | 1,747,819 | 1.67 | 2026-11-04 20:40:07 |  |
| genomics_bronze | string_protein_links | MANAGED | 10,826,054 | 1 | 64,706,278 | 61.71 | 2026-11-23 14:29:34 |  |
| genomics_bronze | tahoe_cell_line_metadata | MANAGED | 1,000 | 1 | 12,002 | 0.01 | 2026-11-04 20:40:21 |  |
| genomics_bronze | tahoe_drug_metadata | MANAGED | 379 | 1 | 25,237 | 0.02 | 2026-11-04 20:40:25 |  |
| genomics_bronze | tahoe_gene_metadata | MANAGED | 62,710 | 1 | 523,565 | 0.5 | 2026-11-04 20:40:29 |  |
| genomics_bronze | tahoe_pseudobulk_differential_expression | MANAGED | 10,772,362 | 3 | 143,116,168 | 136.49 | 2026-11-13 00:10:26 |  |
| genomics_bronze | tahoe_sample_metadata | MANAGED | 1,344 | 1 | 59,022 | 0.06 | 2026-11-04 20:40:47 |  |
| genomics_bronze | tcga_mc3_somatic_mutations | MANAGED | 3,600,963 | 2 | 108,634,227 | 103.6 | 2026-11-11 23:04:15 |  |
| genomics_bronze | thousand_genomes_variants | MANAGED | 544,361 | 2 | 9,622,124 | 9.18 | 2026-11-12 14:12:01 |  |
| genomics_bronze | ucsc_conservation_100way | MANAGED | 8,110,308 | 2 | 32,131,458 | 30.64 | 2026-11-12 14:21:22 |  |
| genomics_bronze | uniprot_entries | MANAGED | 20,429 | 1 | 6,059,916 | 5.78 | 2026-11-24 16:37:00 |  |
| genomics_bronze | uniprot_features | MANAGED | 284,172 | 1 | 4,226,153 | 4.03 | 2026-11-24 16:37:05 |  |
| genomics_bronze | variant_benchmark_sets | MANAGED | 12,042,448 | 2 | 50,990,366 | 48.63 | 2026-11-12 14:21:38 |  |
| genomics_gold | benchmark_variant | MANAGED | 14,697,654 | 11 | 693,688,786 | 661.55 | 2026-11-21 22:41:27 |  |
| genomics_gold | case_assay_record | MANAGED | 16,161,607 | 14 | 1,110,349,183 | 1058.91 | 2026-11-23 17:42:04 |  |
| genomics_gold | cell_line_profile | MANAGED | 3,450 | 1 | 317,490 | 0.3 | 2026-11-19 21:58:32 |  |
| genomics_gold | clinical_trial_profile | MANAGED | 612,543 | 6 | 375,966,670 | 358.55 | 2026-11-23 16:20:07 |  |
| genomics_gold | clinvar_classification_change | MANAGED | 519,421 | 4 | 3,883,813 | 3.7 | 2026-11-19 22:02:24 |  |
| genomics_gold | disease_profile | MANAGED | 266,194 | 1 | 14,075,890 | 13.42 | 2026-11-19 21:58:10 |  |
| genomics_gold | dms_assay | MANAGED | 2,621 | 1 | 1,230,944 | 1.17 | 2026-11-19 22:10:47 |  |
| genomics_gold | dms_assay_mutant | MANAGED | 548,557 | 1 | 24,362,506 | 23.23 | 2026-11-19 22:10:54 |  |
| genomics_gold | drug_profile | MANAGED | 2,903,856 | 11 | 363,136,018 | 346.31 | 2026-11-23 11:55:21 |  |
| genomics_gold | embedding_split | MANAGED | 2 | 1 | 27,866 | 0.03 | 2026-11-19 22:10:59 |  |
| genomics_gold | entity_identity_record | MANAGED | 136,458,663 | 56 | 10,798,496,265 | 10298.25 | 2026-11-19 22:06:17 |  |
| genomics_gold | epitope_profile | MANAGED | 2,910,311 | 6 | 108,610,792 | 103.58 | 2026-11-23 11:55:35 |  |
| genomics_gold | gene_disease_profile | MANAGED | 18,490,272 | 5 | 1,006,096,926 | 959.49 | 2026-11-23 11:55:49 |  |
| genomics_gold | gene_measurement | MANAGED | 261,402,088 | 222 | 21,204,242,279 | 20221.94 | 2026-11-23 17:47:47 |  |
| genomics_gold | gene_profile | MANAGED | 228,120 | 2 | 20,461,190 | 19.51 | 2026-11-23 02:49:22 |  |
| genomics_gold | genetic_test_profile | MANAGED | 512,638 | 1 | 19,831,969 | 18.91 | 2026-11-19 21:58:42 |  |
| genomics_gold | multi_value_attribute | MANAGED | 52 | 26 | 138,694 | 0.13 | 2026-11-19 21:33:52 |  |
| genomics_gold | multi_value_score | MANAGED | 60,138 | 16 | 2,738,935 | 2.61 | 2026-11-19 21:35:37 |  |
| genomics_gold | protein_profile | MANAGED | 461,200 | 1 | 18,386,448 | 17.53 | 2026-11-19 21:57:57 |  |
| genomics_gold | protein_variant_record | MANAGED | 132,805,706 | 50 | 10,273,963,583 | 9798.02 | 2026-11-19 22:05:01 |  |
| genomics_gold | somatic_mutation_profile | MANAGED | 3,600,920 | 5 | 260,156,759 | 248.1 | 2026-11-23 11:55:05 |  |
| genomics_gold | somatic_site_profile | MANAGED | 3,091,452 | 4 | 33,505,014 | 31.95 | 2026-11-19 22:02:11 |  |
| genomics_gold | structural_variant_profile | MANAGED | 7,974,145 | 2 | 126,543,734 | 120.68 | 2026-11-23 11:55:28 |  |
| genomics_gold | transcript_profile | MANAGED | 663,754 | 2 | 33,101,408 | 31.57 | 2026-11-19 21:57:45 |  |
| genomics_gold | tumor_sample_profile | MANAGED | 10,295 | 1 | 201,750 | 0.19 | 2026-11-19 21:59:23 |  |
| genomics_gold | variant_profile | MANAGED | 22,212,522 | 15 | 1,022,659,901 | 975.28 | 2026-11-24 01:39:24 |  |
| genomics_ml_datasets | dataset_benchmark | MANAGED | 14,697,654 | 12 | 851,219,751 | 811.79 | 2026-11-23 14:28:50 |  |
| genomics_ml_datasets | dataset_gene | MANAGED | 248,982 | 2 | 27,150,692 | 25.89 | 2026-11-23 14:29:48 |  |
| genomics_ml_datasets | dataset_variant | MANAGED | 10,465,042 | 10 | 1,187,379,582 | 1132.37 | 2026-11-24 04:17:11 |  |
| genomics_silver | benchmark_variant | MANAGED | 14,697,654 | 17 | 700,725,803 | 668.26 | 2026-11-23 03:49:36 |  |
| genomics_silver | clinical_trial | MANAGED | 612,543 | 8 | 354,026,964 | 337.63 | 2026-11-22 14:50:03 |  |
| genomics_silver | dms_assay | MANAGED | 2,621 | 1 | 1,230,940 | 1.17 | 2026-11-19 19:29:27 |  |
| genomics_silver | dms_assay_mutant | MANAGED | 548,557 | 1 | 24,362,437 | 23.23 | 2026-11-22 14:49:14 |  |
| genomics_silver | embedding_split | MANAGED | 2 | 1 | 27,856 | 0.03 | 2026-11-19 19:29:12 |  |
| genomics_silver | entity_relation | MANAGED | 43,525,722 | 37 | 2,104,380,955 | 2006.89 | 2026-11-23 03:49:45 |  |
| genomics_silver | epitope | MANAGED | 2,910,311 | 1 | 84,985,429 | 81.05 | 2026-11-22 14:47:28 |  |
| genomics_silver | epitope_mhc_assay | MANAGED | 5,777,975 | 2 | 121,376,475 | 115.75 | 2026-11-22 14:48:49 |  |
| genomics_silver | faers_case | MANAGED | 819,683 | 1 | 50,668,624 | 48.32 | 2026-11-20 12:11:36 |  |
| genomics_silver | faers_case_drug | MANAGED | 3,328,534 | 4 | 156,116,686 | 148.88 | 2026-11-22 14:49:45 |  |
| genomics_silver | faers_case_item | MANAGED | 6,235,415 | 2 | 259,535,973 | 247.51 | 2026-11-22 14:49:57 |  |
| genomics_silver | gene_dependency | MANAGED | 24,274,029 | 16 | 994,033,746 | 947.98 | 2026-11-22 14:48:10 |  |
| genomics_silver | gene_disease_score_year | MANAGED | 158,172,560 | 68 | 6,199,563,259 | 5912.36 | 2026-11-22 14:47:51 |  |
| genomics_silver | gene_expression_median | MANAGED | 4,014,244 | 1 | 150,248,348 | 143.29 | 2026-11-22 14:49:20 |  |
| genomics_silver | locus_score | MANAGED | 20,025,911 | 13 | 792,382,364 | 755.67 | 2026-11-22 14:47:36 |  |
| genomics_silver | perturbation_expression | MANAGED | 10,772,362 | 10 | 620,224,641 | 591.49 | 2026-11-22 14:49:26 |  |
| genomics_silver | perturbation_sample | MANAGED | 1,344 | 1 | 113,037 | 0.11 | 2026-11-19 20:04:12 |  |
| genomics_silver | protein_residue_score | MANAGED | 15,088,388 | 8 | 532,790,153 | 508.11 | 2026-11-22 14:49:07 |  |
| genomics_silver | regulatory_element_gene_link | MANAGED | 64,167,549 | 64 | 3,750,915,533 | 3577.15 | 2026-11-22 14:50:16 |  |
| genomics_silver | somatic_mutation | MANAGED | 3,600,920 | 6 | 257,997,389 | 246.05 | 2026-11-22 14:47:21 |  |
| genomics_silver | tumor_sample | MANAGED | 10,295 | 1 | 230,381 | 0.22 | 2026-11-19 20:02:59 |  |
| genomics_silver | variant | MANAGED | 22,212,522 | 9 | 631,121,336 | 601.88 | 2026-11-22 18:33:42 |  |
| genomics_silver | variant_classification | MANAGED | 14,214,237 | 15 | 633,207,723 | 603.87 | 2026-11-22 14:48:23 |  |
| genomics_silver | variant_condition | MANAGED | 29,039,994 | 27 | 1,350,193,230 | 1287.64 | 2026-11-22 14:49:52 |  |
| genomics_silver | variant_feature_overlap | MANAGED | 79,803,232 | 55 | 3,435,881,106 | 3276.71 | 2026-11-22 14:48:43 |  |
| genomics_silver | variant_hgvs | MANAGED | 7,832,977 | 8 | 459,449,174 | 438.16 | 2026-11-22 13:46:45 |  |
| genomics_silver | variant_population_frequency | MANAGED | 40,154,448 | 24 | 1,745,883,236 | 1665.0 | 2026-11-22 14:48:36 |  |
| genomics_silver_reference | cell_line | MANAGED | 3,450 | 1 | 322,657 | 0.31 | 2026-11-19 18:37:33 |  |
| genomics_silver_reference | entity_claim | MANAGED | 79,060,972 | 66 | 3,730,805,859 | 3557.97 | 2026-11-22 13:47:06 |  |
| genomics_silver_reference | entity_coordinate | MANAGED | 4,043,423 | 2 | 175,607,125 | 167.47 | 2026-11-22 13:47:33 |  |
| genomics_silver_reference | entity_derived_attribute | MANAGED | 562,302,059 | 410 | 23,850,528,641 | 22745.64 | 2026-11-23 03:49:54 |  |
| genomics_silver_reference | entity_id_crosswalk | MANAGED | 35,792,769 | 31 | 1,558,891,154 | 1486.67 | 2026-11-22 13:47:18 |  |
| genomics_silver_reference | entity_master | MANAGED | 4,523,119 | 14 | 466,381,071 | 444.78 | 2026-11-22 13:47:38 |  |
| genomics_silver_reference | entity_name | MANAGED | 15,446,406 | 13 | 705,442,350 | 672.76 | 2026-11-22 13:47:12 |  |
| genomics_silver_reference | genetic_test | MANAGED | 512,638 | 1 | 20,499,862 | 19.55 | 2026-11-22 13:47:23 |  |
| genomics_silver_reference | ontology_relation | MANAGED | 2,115,093 | 1 | 93,497,689 | 89.17 | 2026-11-21 16:06:25 |  |
| genomics_silver_reference | protein_feature | MANAGED | 550,016 | 2 | 47,168,601 | 44.98 | 2026-11-22 13:47:01 |  |
| genomics_silver_reference | protein_gene_link | MANAGED | 491,099 | 1 | 19,668,091 | 18.76 | 2026-11-22 13:46:56 |  |
| knowledge | rerank_scores | MANAGED | 5,453 | 5 | 210,864 | 0.2 | 2026-10-27 22:32:22 |  |
| knowledge | wikipedia_article | MANAGED | 4,428,568 | 86 | 5,140,253,845 | 4902.13 | 2026-11-15 13:51:19 |  |
| knowledge | wikipedia_chunks | MANAGED | 209,100 | 6 | 358,528,560 | 341.92 | 2026-10-13 23:27:05 |  |
| knowledge | wikipedia_clickstream | MANAGED | 22,509,883 | 8 | 416,598,429 | 397.3 | 2026-11-19 20:49:30 |  |
| knowledge | wikipedia_edges | MANAGED | 8,831 | 1 | 173,401 | 0.17 | 2026-10-13 18:37:21 |  |
| knowledge | wikipedia_selected | MANAGED | 33,443 | 1 | 63,105,105 | 60.18 | 2026-11-15 15:31:35 |  |
| ml_datasets_shared | dataset_manifest | MANAGED | 41 | 1 | 15,999 | 0.02 | 2026-11-22 20:45:52 |  |
| ml_datasets_shared | feature_contract | MANAGED | 3,215 | 7 | 35,267 | 0.03 | 2026-11-22 20:20:29 |  |
| ml_datasets_shared | gate_results | MANAGED | 641 | 6 | 24,242 | 0.02 | 2026-11-22 20:43:31 |  |
| ml_datasets_shared | imputer_parameters | MANAGED | 6,558 | 7 | 177,276 | 0.17 | 2026-11-22 13:19:08 |  |
| ml_datasets_shared | mask_specification | MANAGED | 14 | 1 | 4,115 | 0.0 | 2026-11-22 13:20:47 |  |
| ml_datasets_shared | null_class_registry | MANAGED | 2,893 | 7 | 54,744 | 0.05 | 2026-11-22 20:20:43 |  |
| ml_datasets_shared | partition_manifest | MANAGED | 243,351,576 | 114 | 7,090,572,624 | 6762.1 | 2026-11-22 20:20:39 |  |
| ml_models | approval_spec | MANAGED | 42 | 3 | 10,650 | 0.01 | 2026-11-22 17:56:10 |  |
| ml_models | candidate_results | MANAGED | 1,538 | 9 | 76,800 | 0.07 | 2026-11-22 22:48:21 |  |
| ml_models | candidate_selection | MANAGED | 108 | 3 | 17,354 | 0.02 | 2026-11-23 10:39:12 |  |
| ml_models | evaluation_flags | MANAGED | 26 | 3 | 11,477 | 0.01 | 2026-11-23 12:49:48 |  |
| ml_models | evaluation_results | MANAGED | 2,497 | 19 | 142,130 | 0.14 | 2026-11-23 12:19:02 |  |
| ml_models | evaluation_run_context | MANAGED | 228 | 11 | 66,155 | 0.06 | 2026-11-23 12:19:05 |  |
| ml_models | evaluation_spec | MANAGED | 55 | 6 | 15,220 | 0.01 | 2026-11-22 18:05:55 |  |
| ml_models | library_availability | MANAGED | 9 | 1 | 1,941 | 0.0 | 2026-11-22 18:03:23 |  |
| ml_models | model_approval | MANAGED | 108 | 3 | 36,061 | 0.03 | 2026-11-23 13:06:39 |  |
| ml_models | model_predictions | MANAGED | 4,909,978 | 2 | 75,751,694 | 72.24 | 2026-11-23 14:50:10 |  |
| ml_models | model_registry | MANAGED | 72 | 3 | 42,033 | 0.04 | 2026-11-23 13:16:05 |  |
| ml_models | monitoring_flags | MANAGED | 84 | 8 | 36,084 | 0.03 | 2026-11-23 14:31:23 |  |
| ml_models | monitoring_results | MANAGED | 12,638 | 15 | 118,961 | 0.11 | 2026-11-23 14:31:21 |  |
| ml_models | monitoring_spec | MANAGED | 18 | 3 | 7,897 | 0.01 | 2026-11-22 17:56:22 |  |
| ml_models | monitoring_triggers | MANAGED | 18 | 15 | 62,206 | 0.06 | 2026-11-23 14:31:42 |  |
| ml_models | reference_profile | MANAGED | 2,100 | 5 | 96,377 | 0.09 | 2026-11-23 14:01:39 |  |
| ml_models | registry_check | MANAGED | 72 | 3 | 22,461 | 0.02 | 2026-11-23 13:34:06 |  |
| ml_models | scoring_requests | MANAGED | 811 | 15 | 87,417 | 0.08 | 2026-11-23 14:39:39 |  |
| ml_models | task_run_context | MANAGED | 210 | 24 | 130,750 | 0.12 | 2026-11-22 22:48:25 |  |
| quality | field_class_registry | MANAGED | 4,371 | 5 | 48,259 | 0.05 | 2026-11-19 18:02:21 |  |
| quality | pipeline_watermarks | MANAGED | 2,647 | 9 | 70,961 | 0.07 | 2026-11-22 20:20:54 |  |
| quality | quality_audit_log | MANAGED | 8,329 | 28 | 187,026 | 0.18 | 2026-11-23 14:42:04 |  |
| quality | quarantine | MANAGED | 1,885,387 | 19 | 39,013,731 | 37.21 | 2026-11-19 20:51:48 |  |
| shared_conformed | dim_date | MANAGED | 50,768 | 1 | 13,747 | 0.01 | 2026-11-19 21:16:52 |  |
| shared_conformed | dim_geography | MANAGED | 17 | 1 | 3,302 | 0.0 | 2026-11-19 21:09:14 |  |
| shared_conformed | dim_place | MANAGED | 80 | 1 | 15,376 | 0.01 | 2026-11-19 21:23:01 |  |
| shared_conformed | dim_time | MANAGED | 1,440 | 2 | 5,387 | 0.01 | 2026-11-19 21:08:53 |  |
| shared_conformed | dim_weather_context | MANAGED | 0 | 0 | 0 | 0.0 | 2026-11-15 19:42:21 |  |
| shared_conformed | geo_plz_gemeinde_xref | MANAGED | 0 | 0 | 0 | 0.0 | 2026-11-15 19:42:20 |  |
| shared_conformed | metric_definition | MANAGED | 15 | 1 | 5,432 | 0.01 | 2026-11-19 21:23:58 |  |
| shared_conformed | ref_energy_carrier | MANAGED | 44 | 1 | 3,322 | 0.0 | 2026-11-19 21:12:07 |  |
| shared_conformed | ref_energy_carrier_map | MANAGED | 105 | 1 | 9,932 | 0.01 | 2026-11-19 21:11:22 |  |
| shared_conformed | ref_measure | MANAGED | 1,967 | 1 | 96,790 | 0.09 | 2026-11-19 21:14:50 |  |
| shared_conformed | series_coverage | MANAGED | 5,828,948 | 4 | 229,915,545 | 219.26 | 2026-11-24 16:02:15 |  |

## Volumes

| schema | volume | type |
| --- | --- | --- |
| bronze | landing | MANAGED |
| energy_ml_models | foundation_models | MANAGED |
| energy_ml_models | model_libraries | MANAGED |
| knowledge | models | MANAGED |
| knowledge | rerank | MANAGED |

## Errors (0)

None.
