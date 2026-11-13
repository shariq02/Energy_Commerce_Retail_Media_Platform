# Catalog inventory: energy_commerce_retail_media

Generated: 2026-11-13T00:31:27Z

Size is the data files of the current table version (older versions kept for time travel are not counted). A view has no size. `tables` counts tables and views; `views` is the part that are views.

## Guard lines

- WARNING schema genomics_bronze: 98 tables (guard 90)
- WARNING metastore: 866 tables and views in all catalogs (guard 400)

## Overall

| schemas | tables | views | rows | files | size_bytes | size_gb | volumes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 24 | 287 | 16 | 1,590,021,616 | 1,549 | 61,847,329,273 | 57.6 | 5 |

## Catalogs

| catalog | tables_and_views | error |
| --- | --- | --- |
| energy_commerce_retail_media | 287 |  |
| samples | 124 |  |
| system | 455 |  |

## Quota limits and usage

| quota | parent | parent_type | used | limit | used_percent |
| --- | --- | --- | --- | --- | --- |
| table-quota | energy_commerce_retail_media.genomics_bronze | SecurableType.SCHEMA | 98 | 100 | 98.0 |
| table-quota | energy_commerce_retail_media.energy_ml_datasets | SecurableType.SCHEMA | 91 | 100 | 91.0 |
| table-quota | 1a00c03f-d59a-4486-bdcc-33a6b812f863 | SecurableType.METASTORE | 432 | 500 | 86.4 |
| table-quota | energy_commerce_retail_media.bronze | SecurableType.SCHEMA | 62 | 100 | 62.0 |
| schema-quota | energy_commerce_retail_media | SecurableType.CATALOG | 24 | 50 | 48.0 |
| table-quota | energy_commerce_retail_media.commerce_ml_datasets | SecurableType.SCHEMA | 46 | 100 | 46.0 |
| table-quota | energy_commerce_retail_media.energy_gold | SecurableType.SCHEMA | 30 | 100 | 30.0 |
| table-quota | energy_commerce_retail_media.commerce_ml_models | SecurableType.SCHEMA | 27 | 100 | 27.0 |
| table-quota | energy_commerce_retail_media.ml_models | SecurableType.SCHEMA | 27 | 100 | 27.0 |
| table-quota | energy_commerce_retail_media.energy_ml_models | SecurableType.SCHEMA | 27 | 100 | 27.0 |
| table-quota | energy_commerce_retail_media.genomics_silver | SecurableType.SCHEMA | 20 | 100 | 20.0 |
| volume-quota | 1a00c03f-d59a-4486-bdcc-33a6b812f863 | SecurableType.METASTORE | 8 | 50 | 16.0 |
| table-quota | energy_commerce_retail_media.energy_silver | SecurableType.SCHEMA | 15 | 100 | 15.0 |
| table-quota | energy_commerce_retail_media.shared_conformed | SecurableType.SCHEMA | 14 | 100 | 14.0 |
| table-quota | energy_commerce_retail_media.energy_silver_reference | SecurableType.SCHEMA | 11 | 100 | 11.0 |
| table-quota | energy_commerce_retail_media.genomics_silver_reference | SecurableType.SCHEMA | 11 | 100 | 11.0 |
| table-quota | energy_commerce_retail_media.commerce_gold | SecurableType.SCHEMA | 10 | 100 | 10.0 |
| table-quota | energy_commerce_retail_media.ml_datasets_shared | SecurableType.SCHEMA | 9 | 100 | 9.0 |
| catalog-quota | 1a00c03f-d59a-4486-bdcc-33a6b812f863 | SecurableType.METASTORE | 3 | 50 | 6.0 |
| table-quota | energy_commerce_retail_media.knowledge | SecurableType.SCHEMA | 6 | 100 | 6.0 |
| table-quota | energy_commerce_retail_media.energy_analytics | SecurableType.SCHEMA | 5 | 100 | 5.0 |
| table-quota | energy_commerce_retail_media.quality | SecurableType.SCHEMA | 5 | 100 | 5.0 |
| privilege-quota | 1a00c03f-d59a-4486-bdcc-33a6b812f863 | SecurableType.METASTORE | 9 | 200 | 4.5 |
| schema-quota | system | SecurableType.CATALOG | 2 | 50 | 4.0 |
| volume-quota | energy_commerce_retail_media.knowledge | SecurableType.SCHEMA | 2 | 50 | 4.0 |
| volume-quota | energy_commerce_retail_media.energy_ml_models | SecurableType.SCHEMA | 2 | 50 | 4.0 |
| privilege-quota | energy_commerce_retail_media | SecurableType.CATALOG | 6 | 200 | 3.0 |
| privilege-quota | samples | SecurableType.CATALOG | 5 | 200 | 2.5 |
| provider-quota | 1a00c03f-d59a-4486-bdcc-33a6b812f863 | SecurableType.METASTORE | 1 | 50 | 2.0 |
| volume-quota | samples.sec | SecurableType.SCHEMA | 1 | 50 | 2.0 |
| table-quota | energy_commerce_retail_media.commerce_silver | SecurableType.SCHEMA | 2 | 100 | 2.0 |
| privilege-quota | system.ai | SecurableType.SCHEMA | 4 | 200 | 2.0 |
| table-quota | energy_commerce_retail_media.commerce_analytics | SecurableType.SCHEMA | 2 | 100 | 2.0 |
| privilege-quota | energy_commerce_retail_media.energy_analytics | SecurableType.SCHEMA | 4 | 200 | 2.0 |
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
| bronze | 62 | 0 | 538,241,195 | 170 | 3,946,781,287 | 3763.94 | 3.676 |
| commerce_analytics | 2 | 0 | 9,881 | 2 | 69,223 | 0.07 | 0.0 |
| commerce_gold | 10 | 3 | 32,832,009 | 41 | 1,407,419,136 | 1342.22 | 1.311 |
| commerce_ml_datasets | 0 | 0 | 0 | 0 | 0 | 0.0 | 0.0 |
| commerce_ml_models | 0 | 0 | 0 | 0 | 0 | 0.0 | 0.0 |
| commerce_semantic | 1 | 0 | 3 | 1 | 4,260 | 0.0 | 0.0 |
| commerce_silver | 2 | 0 | 114,582,197 | 209 | 12,708,724,198 | 12119.98 | 11.836 |
| commerce_silver_reference | 0 | 0 | 0 | 0 | 0 | 0.0 | 0.0 |
| default | 0 | 0 | 0 | 0 | 0 | 0.0 | 0.0 |
| energy_analytics | 5 | 0 | 138,871 | 5 | 946,395 | 0.9 | 0.001 |
| energy_gold | 30 | 12 | 169,346,817 | 211 | 10,268,599,394 | 9792.9 | 9.563 |
| energy_ml_datasets | 0 | 0 | 0 | 0 | 0 | 0.0 | 0.0 |
| energy_ml_models | 0 | 0 | 0 | 0 | 0 | 0.0 | 0.0 |
| energy_semantic | 1 | 0 | 5 | 1 | 4,468 | 0.0 | 0.0 |
| energy_silver | 15 | 0 | 66,546,182 | 102 | 5,410,381,545 | 5159.74 | 5.039 |
| energy_silver_reference | 11 | 0 | 24,109,525 | 33 | 817,020,527 | 779.17 | 0.761 |
| genomics_bronze | 98 | 0 | 276,066,967 | 312 | 6,313,989,665 | 6021.49 | 5.88 |
| genomics_silver | 16 | 0 | 145,386,040 | 106 | 6,224,903,388 | 5936.53 | 5.797 |
| genomics_silver_reference | 10 | 0 | 187,330,830 | 182 | 8,500,775,045 | 8106.97 | 7.917 |
| knowledge | 6 | 0 | 27,195,278 | 107 | 5,978,870,204 | 5701.89 | 5.568 |
| ml_datasets_shared | 0 | 0 | 0 | 0 | 0 | 0.0 | 0.0 |
| ml_models | 0 | 0 | 0 | 0 | 0 | 0.0 | 0.0 |
| quality | 4 | 0 | 1,891,810 | 45 | 39,195,767 | 37.38 | 0.037 |
| shared_conformed | 14 | 1 | 6,344,006 | 22 | 229,644,771 | 219.01 | 0.214 |

## Per table

| schema | table | type | rows | files | size_bytes | size_mb | last_modified | error |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| bronze | dwd_air_temperature | MANAGED | 17,336,388 | 1 | 42,040,783 | 40.09 | 2026-09-05 12:43:38 |  |
| bronze | dwd_cloud_type | MANAGED | 13,101,463 | 1 | 48,541,877 | 46.29 | 2026-09-05 13:09:11 |  |
| bronze | dwd_cloudiness | MANAGED | 12,973,316 | 8 | 10,905,182 | 10.4 | 2026-09-05 10:44:08 |  |
| bronze | dwd_device_instrument | MANAGED | 4,008 | 1 | 32,485 | 0.03 | 2026-09-05 10:46:54 |  |
| bronze | dwd_dew_point | MANAGED | 13,524,783 | 1 | 37,493,076 | 35.76 | 2026-09-05 13:08:45 |  |
| bronze | dwd_extreme_wind | MANAGED | 6,480,621 | 4 | 8,735,436 | 8.33 | 2026-09-05 10:46:18 |  |
| bronze | dwd_measurement | MANAGED | 177,413,542 | 9 | 453,628,845 | 432.61 | 2026-11-12 20:09:14 |  |
| bronze | dwd_missing_value_periods | MANAGED | 5,897,409 | 7 | 21,224,003 | 20.24 | 2026-09-05 10:47:07 |  |
| bronze | dwd_moisture | MANAGED | 12,449,858 | 2 | 99,508,690 | 94.9 | 2026-09-05 11:34:35 |  |
| bronze | dwd_parameter_unit | MANAGED | 5,418 | 1 | 24,771 | 0.02 | 2026-09-05 10:46:59 |  |
| bronze | dwd_precipitation | MANAGED | 7,154,301 | 5 | 5,527,700 | 5.27 | 2026-09-05 10:44:32 |  |
| bronze | dwd_pressure | MANAGED | 13,227,922 | 1 | 34,549,225 | 32.95 | 2026-09-06 12:38:50 |  |
| bronze | dwd_soil_temperature | MANAGED | 7,183,226 | 8 | 30,237,188 | 28.84 | 2026-09-05 10:45:29 |  |
| bronze | dwd_solar | MANAGED | 5,809,249 | 8 | 24,926,282 | 23.77 | 2026-09-05 10:46:41 |  |
| bronze | dwd_station_geography | MANAGED | 222 | 1 | 6,526 | 0.01 | 2026-09-05 10:46:45 |  |
| bronze | dwd_station_name_history | MANAGED | 106 | 1 | 3,067 | 0.0 | 2026-09-05 10:46:49 |  |
| bronze | dwd_sun | MANAGED | 12,650,564 | 8 | 10,055,692 | 9.59 | 2026-09-05 10:44:52 |  |
| bronze | dwd_visibility | MANAGED | 13,124,582 | 1 | 20,084,903 | 19.15 | 2026-09-05 13:08:52 |  |
| bronze | dwd_weather_phenomena | MANAGED | 12,915,288 | 1 | 17,912,130 | 17.08 | 2026-09-05 13:08:59 |  |
| bronze | dwd_wind | MANAGED | 15,957,198 | 1 | 31,203,087 | 29.76 | 2026-09-05 12:43:28 |  |
| bronze | dwd_wind_synop | MANAGED | 13,524,783 | 1 | 29,537,612 | 28.17 | 2026-09-05 13:08:36 |  |
| bronze | energy_charts_physical_flows | MANAGED | 853,584 | 3 | 1,910,744 | 1.82 | 2026-11-12 12:18:49 |  |
| bronze | ga4_events | MANAGED | 779,485 | 1 | 25,902,201 | 24.7 | 2026-09-15 15:58:48 |  |
| bronze | honda_iot_cooling | MANAGED | 6,835,855 | 2 | 50,501,340 | 48.16 | 2026-09-21 13:54:28 |  |
| bronze | honda_iot_electricity | MANAGED | 6,835,916 | 2 | 120,150,032 | 114.58 | 2026-09-21 13:54:16 |  |
| bronze | honda_iot_heating | MANAGED | 6,835,906 | 2 | 51,072,493 | 48.71 | 2026-09-21 13:54:22 |  |
| bronze | honda_iot_meter | MANAGED | 20,507,677 | 4 | 221,410,704 | 211.15 | 2026-11-12 14:20:54 |  |
| bronze | honda_iot_weather | MANAGED | 3,419,507 | 3 | 41,263,292 | 39.35 | 2026-08-27 13:33:35 |  |
| bronze | mastr_anlagen_eeg | MANAGED | 66,718 | 4 | 2,563,630 | 2.44 | 2026-11-12 14:20:45 |  |
| bronze | mastr_anlagen_eeg_biomasse | MANAGED | 15,644 | 1 | 747,895 | 0.71 | 2026-09-05 23:07:28 |  |
| bronze | mastr_anlagen_eeg_geothermie_gsgk | MANAGED | 132 | 1 | 10,498 | 0.01 | 2026-09-05 23:07:35 |  |
| bronze | mastr_anlagen_eeg_wasser | MANAGED | 7,485 | 1 | 303,236 | 0.29 | 2026-09-05 23:07:31 |  |
| bronze | mastr_anlagen_eeg_wind | MANAGED | 43,457 | 1 | 1,483,125 | 1.41 | 2026-09-05 23:07:23 |  |
| bronze | mastr_anlagen_kwk | MANAGED | 92,514 | 1 | 2,726,455 | 2.6 | 2026-09-05 23:07:40 |  |
| bronze | mastr_bilanzierungsgebiete | MANAGED | 1,118 | 1 | 17,096 | 0.02 | 2026-09-05 23:09:11 |  |
| bronze | mastr_code_lookup | MANAGED | 1,789 | 5 | 31,440 | 0.03 | 2026-11-12 14:20:57 |  |
| bronze | mastr_einheiten | MANAGED | 170,974 | 6 | 13,736,216 | 13.1 | 2026-11-12 14:20:40 |  |
| bronze | mastr_einheiten_aenderung_netzbetreiberzuordnungen | MANAGED | 226,405 | 1 | 5,739,794 | 5.47 | 2026-09-05 23:09:28 |  |
| bronze | mastr_einheiten_biomasse | MANAGED | 24,334 | 1 | 2,302,674 | 2.2 | 2026-09-07 02:35:28 |  |
| bronze | mastr_einheiten_genehmigung | MANAGED | 37,722 | 1 | 1,679,203 | 1.6 | 2026-09-05 23:09:15 |  |
| bronze | mastr_einheiten_geothermie_gsgk | MANAGED | 336 | 1 | 48,501 | 0.05 | 2026-09-17 13:42:15 |  |
| bronze | mastr_einheiten_kernkraft | MANAGED | 6 | 1 | 13,789 | 0.01 | 2026-09-07 00:19:17 |  |
| bronze | mastr_einheiten_verbrennung | MANAGED | 94,027 | 1 | 6,779,898 | 6.47 | 2026-09-17 12:45:45 |  |
| bronze | mastr_einheiten_wasser | MANAGED | 8,814 | 1 | 798,504 | 0.76 | 2026-09-17 12:11:05 |  |
| bronze | mastr_einheiten_wind | MANAGED | 43,457 | 1 | 3,733,949 | 3.56 | 2026-09-07 00:19:08 |  |
| bronze | mastr_ertuechtigungen | MANAGED | 1,681 | 1 | 33,971 | 0.03 | 2026-09-05 23:09:31 |  |
| bronze | mastr_geloeschte_deaktivierte_einheiten | MANAGED | 244,295 | 1 | 2,923,502 | 2.79 | 2026-09-05 23:09:19 |  |
| bronze | mastr_geloeschte_deaktivierte_marktakteure | MANAGED | 285,315 | 1 | 3,242,833 | 3.09 | 2026-09-05 23:09:23 |  |
| bronze | mastr_katalogkategorien | MANAGED | 123 | 1 | 2,399 | 0.0 | 2026-09-05 23:09:37 |  |
| bronze | mastr_katalogwerte | MANAGED | 1,737 | 1 | 24,359 | 0.02 | 2026-09-05 23:09:40 |  |
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
| commerce_analytics | ga4_demand_daily | MANAGED | 7,389 | 1 | 39,713 | 0.04 | 2026-09-28 15:24:36 |  |
| commerce_analytics | rees46_demand_daily | MANAGED | 2,492 | 1 | 29,510 | 0.03 | 2026-09-28 15:24:56 |  |
| commerce_gold | ga4_event | VIEW |  |  |  |  |  | count: [UC_DEPENDENCY_DOES_NOT_EXIST] Dependency does not exist in Unity Catalog:  Table 'energy_commerce_retail_media.commerce_gold.ga4_event' is invalid because one of the underlying resources does not exi |
| commerce_gold | ga4_event_item | VIEW | 3,982,732 |  |  |  |  |  |
| commerce_gold | ga4_product | MANAGED | 1,393 | 1 | 53,914 | 0.05 | 2026-09-28 10:57:48 |  |
| commerce_gold | ga4_purchase | MANAGED | 4,786 | 1 | 452,322 | 0.43 | 2026-09-28 11:03:50 |  |
| commerce_gold | ga4_session | MANAGED | 168,963 | 1 | 6,735,536 | 6.42 | 2026-09-28 10:49:50 |  |
| commerce_gold | ga4_user | MANAGED | 133,010 | 1 | 2,937,909 | 2.8 | 2026-09-28 10:51:21 |  |
| commerce_gold | rees46_event | VIEW |  |  |  |  |  | count: [UC_DEPENDENCY_DOES_NOT_EXIST] Dependency does not exist in Unity Catalog:  Table 'energy_commerce_retail_media.commerce_gold.rees46_event' is invalid because one of the underlying resources does not  |
| commerce_gold | rees46_product | MANAGED | 206,876 | 11 | 7,031,106 | 6.71 | 2026-09-28 11:07:38 |  |
| commerce_gold | rees46_session | MANAGED | 23,017,601 | 21 | 1,299,758,367 | 1239.55 | 2026-11-12 19:35:07 |  |
| commerce_gold | rees46_user | MANAGED | 5,316,648 | 5 | 90,449,982 | 86.26 | 2026-10-05 23:06:14 |  |
| commerce_semantic | metric_definition | MANAGED | 3 | 1 | 4,260 | 0.0 | 2026-09-28 15:25:22 |  |
| commerce_silver | commerce_event | MANAGED | 110,599,465 | 207 | 12,537,698,486 | 11956.88 | 2026-11-12 19:34:56 |  |
| commerce_silver | ga4_event_item | MANAGED | 3,982,732 | 2 | 171,025,712 | 163.1 | 2026-11-12 19:36:22 |  |
| energy_analytics | generation_by_carrier_daily | MANAGED | 99,847 | 1 | 472,922 | 0.45 | 2026-09-28 15:22:24 |  |
| energy_analytics | market_price_daily | MANAGED | 2,897 | 1 | 45,855 | 0.04 | 2026-09-28 15:22:24 |  |
| energy_analytics | site_energy_daily | MANAGED | 16,984 | 1 | 126,308 | 0.12 | 2026-09-28 15:22:59 |  |
| energy_analytics | site_energy_weather_daily | MANAGED | 16,984 | 1 | 247,528 | 0.24 | 2026-09-28 15:24:02 |  |
| energy_analytics | site_weather_daily | MANAGED | 2,159 | 1 | 53,782 | 0.05 | 2026-09-28 15:23:30 |  |
| energy_gold | balancing_area | VIEW | 1,118 |  |  |  |  |  |
| energy_gold | capacity_plan | MANAGED | 13 | 1 | 6,702 | 0.01 | 2026-09-28 10:51:27 |  |
| energy_gold | channel_reading | MANAGED | 63,243,582 | 41 | 2,278,291,230 | 2172.75 | 2026-11-12 19:33:37 |  |
| energy_gold | energy_balance_component | MANAGED | 13,973,351 | 3 | 221,784,893 | 211.51 | 2026-09-30 12:31:35 |  |
| energy_gold | energy_channel | MANAGED | 8 | 1 | 3,018 | 0.0 | 2026-09-28 11:06:40 |  |
| energy_gold | entity_link | MANAGED | 33,963,083 | 45 | 999,891,239 | 953.57 | 2026-11-12 19:37:55 |  |
| energy_gold | forecast_actual | MANAGED | 17,142 | 4 | 541,644 | 0.52 | 2026-09-28 11:06:37 |  |
| energy_gold | generation_forecast | VIEW |  |  |  |  |  | count: [UC_DEPENDENCY_DOES_NOT_EXIST] Dependency does not exist in Unity Catalog:  Table 'energy_commerce_retail_media.energy_gold.generation_forecast' is invalid because one of the underlying resources does |
| energy_gold | generation_unit | MANAGED | 170,974 | 1 | 19,177,378 | 18.29 | 2026-10-02 13:23:01 |  |
| energy_gold | grid_connection_point | VIEW | 5,867,232 |  |  |  |  |  |
| energy_gold | grid_intervention_event | VIEW |  |  |  |  |  | count: [INCOMPATIBLE_VIEW_SCHEMA_CHANGE] The SQL query of view `energy_commerce_retail_media`.`energy_gold`.`grid_intervention_event` has an incompatible schema change and column requesting_market_area_codes |
| energy_gold | grid_location | MANAGED | 6,776,394 | 2 | 249,467,624 | 237.91 | 2026-11-12 19:33:12 |  |
| energy_gold | grid_location_coordinate_conflict | VIEW |  |  |  |  |  | count: [UC_DEPENDENCY_DOES_NOT_EXIST] Dependency does not exist in Unity Catalog:  Table 'energy_commerce_retail_media.energy_gold.grid_location_coordinate_conflict' is invalid because one of the underlying  |
| energy_gold | grid_network | MANAGED | 1,740 | 1 | 65,665 | 0.06 | 2026-09-28 10:51:22 |  |
| energy_gold | iot_device | MANAGED | 198,164 | 1 | 11,739,516 | 11.2 | 2026-09-28 11:05:53 |  |
| energy_gold | iot_device_observation | MANAGED | 198,164 | 1 | 13,817,628 | 13.18 | 2026-09-28 10:59:09 |  |
| energy_gold | market_actor | MANAGED | 5,616,357 | 2 | 137,302,016 | 130.94 | 2026-10-05 23:05:53 |  |
| energy_gold | market_actor_role | VIEW |  |  |  |  |  | count: [INCOMPATIBLE_VIEW_SCHEMA_CHANGE] The SQL query of view `energy_commerce_retail_media`.`energy_gold`.`market_actor_role` has an incompatible schema change and column BundesnetzagenturBetriebsnummer_nv |
| energy_gold | market_price | VIEW |  |  |  |  |  | count: [UC_DEPENDENCY_DOES_NOT_EXIST] Dependency does not exist in Unity Catalog:  Table 'energy_commerce_retail_media.energy_gold.market_price' is invalid because one of the underlying resources does not ex |
| energy_gold | place_instrument_period | MANAGED | 3,965 | 1 | 178,016 | 0.17 | 2026-09-28 11:22:09 |  |
| energy_gold | place_parameter_period | MANAGED | 5,412 | 1 | 206,979 | 0.2 | 2026-09-28 11:22:16 |  |
| energy_gold | plant_sensor_observation | VIEW |  |  |  |  |  | count: [INCOMPATIBLE_VIEW_SCHEMA_CHANGE] The SQL query of view `energy_commerce_retail_media`.`energy_gold`.`plant_sensor_observation` has an incompatible schema change and column quality_flags cannot be res |
| energy_gold | power_plant | MANAGED | 2,613 | 1 | 213,474 | 0.2 | 2026-09-28 10:57:48 |  |
| energy_gold | registry_change_event | MANAGED | 756,015 | 1 | 23,290,821 | 22.21 | 2026-09-28 10:59:11 |  |
| energy_gold | support_registration | VIEW |  |  |  |  |  | count: [INCOMPATIBLE_VIEW_SCHEMA_CHANGE] The SQL query of view `energy_commerce_retail_media`.`energy_gold`.`support_registration` has an incompatible schema change and column AnlagenkennzifferAnlagenregiste |
| energy_gold | unit_authorisation | VIEW |  |  |  |  |  | count: [INCOMPATIBLE_VIEW_SCHEMA_CHANGE] The SQL query of view `energy_commerce_retail_media`.`energy_gold`.`unit_authorisation` has an incompatible schema change and column Frist_nv cannot be resolved. Expe |
| energy_gold | unit_repowering | VIEW | 1,681 |  |  |  |  |  |
| energy_gold | weather_daily | VIEW | 2,144,456 |  |  |  |  |  |
| energy_gold | weather_observation | MANAGED | 27,251,632 | 88 | 5,537,479,343 | 5280.95 | 2026-11-12 19:35:24 |  |
| energy_gold | weather_observation_radiation | MANAGED | 9,153,721 | 16 | 775,142,208 | 739.23 | 2026-11-12 19:31:47 |  |
| energy_semantic | metric_definition | MANAGED | 5 | 1 | 4,468 | 0.0 | 2026-09-28 15:25:45 |  |
| energy_silver | device_telemetry_snapshot | MANAGED | 198,164 | 1 | 18,116,163 | 17.28 | 2026-11-12 21:41:37 |  |
| energy_silver | energy_component | MANAGED | 16,616,582 | 12 | 1,119,060,616 | 1067.22 | 2026-11-12 22:08:18 |  |
| energy_silver | energy_observation | MANAGED | 3,753,157 | 8 | 510,034,299 | 486.41 | 2026-11-12 22:07:20 |  |
| energy_silver | generation_unit | MANAGED | 170,974 | 6 | 19,424,620 | 18.52 | 2026-11-12 21:29:17 |  |
| energy_silver | grid_intervention_event | MANAGED | 39,550 | 1 | 3,932,690 | 3.75 | 2026-11-12 21:41:18 |  |
| energy_silver | plant_operating_sample | MANAGED | 9,568 | 1 | 740,684 | 0.71 | 2026-11-12 21:40:56 |  |
| energy_silver | power_plant_capacity_plan | MANAGED | 13 | 1 | 5,899 | 0.01 | 2026-11-12 21:40:36 |  |
| energy_silver | power_plant_register | MANAGED | 2,613 | 1 | 210,916 | 0.2 | 2026-11-12 21:40:22 |  |
| energy_silver | register_change_event | MANAGED | 756,015 | 3 | 36,615,652 | 34.92 | 2026-11-12 21:36:07 |  |
| energy_silver | register_link | MANAGED | 15,358,350 | 6 | 706,612,686 | 673.88 | 2026-11-12 21:36:39 |  |
| energy_silver | support_registration | MANAGED | 159,232 | 5 | 11,028,886 | 10.52 | 2026-11-12 21:31:23 |  |
| energy_silver | unit_authorisation | MANAGED | 37,722 | 1 | 1,833,896 | 1.75 | 2026-11-12 21:32:28 |  |
| energy_silver | unit_repowering | MANAGED | 1,681 | 1 | 35,975 | 0.03 | 2026-11-12 21:33:07 |  |
| energy_silver | weather_daily | MANAGED | 2,144,456 | 4 | 150,495,426 | 143.52 | 2026-11-12 20:25:54 |  |
| energy_silver | weather_observation | MANAGED | 27,298,105 | 51 | 2,832,233,137 | 2701.03 | 2026-11-12 22:04:39 |  |
| energy_silver_reference | balancing_area | MANAGED | 1,118 | 1 | 56,529 | 0.05 | 2026-11-12 21:35:44 |  |
| energy_silver_reference | grid_connection_point | MANAGED | 5,867,232 | 3 | 202,318,052 | 192.95 | 2026-11-12 21:34:32 |  |
| energy_silver_reference | grid_location | MANAGED | 6,776,394 | 4 | 247,026,941 | 235.58 | 2026-11-12 21:35:30 |  |
| energy_silver_reference | grid_network | MANAGED | 1,740 | 1 | 61,176 | 0.06 | 2026-11-12 21:35:02 |  |
| energy_silver_reference | market_actor | MANAGED | 5,616,357 | 2 | 137,122,041 | 130.77 | 2026-11-12 21:33:50 |  |
| energy_silver_reference | market_actor_role | MANAGED | 15,689 | 1 | 435,995 | 0.42 | 2026-11-12 21:34:07 |  |
| energy_silver_reference | reference_code_list | MANAGED | 1,967 | 9 | 131,340 | 0.13 | 2026-11-12 21:16:18 |  |
| energy_silver_reference | weather_location | MANAGED | 80 | 4 | 30,844 | 0.03 | 2026-11-12 19:26:43 |  |
| energy_silver_reference | weather_missing_value_period | MANAGED | 5,817,858 | 3 | 229,352,847 | 218.73 | 2026-11-12 19:36:08 |  |
| energy_silver_reference | weather_missingness_reconciliation | MANAGED | 1,391 | 1 | 61,925 | 0.06 | 2026-11-12 19:38:23 |  |
| energy_silver_reference | weather_station_period | MANAGED | 9,699 | 4 | 422,837 | 0.4 | 2026-11-12 19:29:10 |  |
| genomics_bronze | aact_browse_terms | MANAGED | 7,003,546 | 12 | 32,237,197 | 30.74 | 2026-11-12 14:08:24 |  |
| genomics_bronze | aact_eligibilities | MANAGED | 610,240 | 12 | 274,464,306 | 261.75 | 2026-11-12 14:08:43 |  |
| genomics_bronze | aact_interventions | MANAGED | 1,034,143 | 4 | 70,770,522 | 67.49 | 2026-11-12 14:08:49 |  |
| genomics_bronze | aact_studies | MANAGED | 611,233 | 12 | 82,715,478 | 78.88 | 2026-11-12 14:08:57 |  |
| genomics_bronze | aact_study_terms | MANAGED | 2,757,633 | 4 | 44,009,529 | 41.97 | 2026-11-12 14:08:33 |  |
| genomics_bronze | acmg_secondary_findings_genes | MANAGED | 94 | 1 | 4,691 | 0.0 | 2026-11-12 14:09:02 |  |
| genomics_bronze | alphafold_residue_confidence | MANAGED | 15,088,388 | 7 | 30,155,041 | 28.76 | 2026-11-12 14:09:10 |  |
| genomics_bronze | alphamissense_variant_scores | MANAGED | 2,534,806 | 4 | 21,012,309 | 20.04 | 2026-11-04 22:34:22 |  |
| genomics_bronze | biogrid_interactions | MANAGED | 1,441,421 | 11 | 40,620,759 | 38.74 | 2026-11-04 20:33:57 |  |
| genomics_bronze | cadd_scores | MANAGED | 9,230,587 | 4 | 32,062,401 | 30.58 | 2026-11-04 20:34:09 |  |
| genomics_bronze | chembl_compounds | MANAGED | 2,897,819 | 5 | 264,141,214 | 251.9 | 2026-11-12 19:34:53 |  |
| genomics_bronze | chembl_uniprot_mapping | MANAGED | 17,257 | 1 | 253,378 | 0.24 | 2026-11-04 20:34:23 |  |
| genomics_bronze | clingen_dosage_sensitivity | MANAGED | 1,539 | 1 | 60,798 | 0.06 | 2026-11-04 20:34:29 |  |
| genomics_bronze | clingen_gene_validity | MANAGED | 3,717 | 1 | 200,789 | 0.19 | 2026-11-04 20:34:33 |  |
| genomics_bronze | clinvar_variant_summary | MANAGED | 23,468,593 | 21 | 1,371,309,896 | 1307.78 | 2026-11-12 16:57:10 |  |
| genomics_bronze | clinvar_variation_allele | MANAGED | 4,694,205 | 3 | 20,007,696 | 19.08 | 2026-11-12 14:10:10 |  |
| genomics_bronze | clinvar_vs_benign_variants | MANAGED | 50,164 | 1 | 1,134,360 | 1.08 | 2026-11-04 20:35:16 |  |
| genomics_bronze | collectri_regulons | MANAGED | 43,175 | 1 | 556,897 | 0.53 | 2026-11-12 14:10:15 |  |
| genomics_bronze | cpic_pairs | MANAGED | 574 | 1 | 13,556 | 0.01 | 2026-11-04 20:35:21 |  |
| genomics_bronze | dbvar_variants | MANAGED | 222,568 | 2 | 10,821,942 | 10.32 | 2026-11-04 20:35:26 |  |
| genomics_bronze | ddinter_interactions | MANAGED | 222,383 | 1 | 928,085 | 0.89 | 2026-11-12 14:10:19 |  |
| genomics_bronze | depmap_gene_effect | MANAGED | 25,088,674 | 5 | 225,352,043 | 214.91 | 2026-11-12 16:05:13 |  |
| genomics_bronze | depmap_models | MANAGED | 2,554 | 1 | 136,572 | 0.13 | 2026-11-12 14:10:37 |  |
| genomics_bronze | encode_ccre_registry | MANAGED | 2,348,854 | 3 | 30,035,097 | 28.64 | 2026-11-12 14:10:43 |  |
| genomics_bronze | encode_enhancer_gene_links | MANAGED | 66,102,715 | 28 | 1,264,427,746 | 1205.85 | 2026-11-12 15:57:24 |  |
| genomics_bronze | ensembl_compara_homologues | MANAGED | 246,152 | 2 | 4,006,502 | 3.82 | 2026-11-12 14:11:31 |  |
| genomics_bronze | faers_case_code | MANAGED | 618,006 | 2 | 2,965,999 | 2.83 | 2026-11-12 14:21:30 |  |
| genomics_bronze | faers_demo | MANAGED | 819,683 | 3 | 16,258,571 | 15.51 | 2026-11-04 20:35:41 |  |
| genomics_bronze | faers_drug | MANAGED | 3,330,435 | 6 | 38,126,836 | 36.36 | 2026-11-04 20:35:48 |  |
| genomics_bronze | faers_indi | MANAGED | 2,264,273 | 3 | 7,771,809 | 7.41 | 2026-11-04 20:35:55 |  |
| genomics_bronze | faers_outc | MANAGED | 595,285 | 1 | 2,860,143 | 2.73 | 2026-11-04 20:36:00 |  |
| genomics_bronze | faers_reac | MANAGED | 2,725,426 | 3 | 10,041,343 | 9.58 | 2026-11-04 20:36:09 |  |
| genomics_bronze | faers_rpsr | MANAGED | 22,721 | 1 | 104,813 | 0.1 | 2026-11-04 20:36:13 |  |
| genomics_bronze | faers_ther | MANAGED | 730,973 | 1 | 4,967,308 | 4.74 | 2026-11-04 20:36:18 |  |
| genomics_bronze | fda_pgx_biomarkers | MANAGED | 692 | 1 | 11,451 | 0.01 | 2026-11-04 20:36:22 |  |
| genomics_bronze | gencode_annotation | MANAGED | 425,844 | 4 | 17,131,847 | 16.34 | 2026-11-04 20:36:29 |  |
| genomics_bronze | gene_ages_consensus | MANAGED | 20,331 | 1 | 235,259 | 0.22 | 2026-11-12 19:16:00 |  |
| genomics_bronze | genome_annotation | MANAGED | 633,133 | 2 | 21,693,604 | 20.69 | 2026-11-12 14:21:27 |  |
| genomics_bronze | gnomad_balanced_variants | MANAGED | 11,992,284 | 6 | 49,856,905 | 47.55 | 2026-11-04 20:36:46 |  |
| genomics_bronze | gnomad_mtdna_variants | MANAGED | 18,164 | 1 | 168,860 | 0.16 | 2026-11-12 14:11:35 |  |
| genomics_bronze | gnomad_variants | MANAGED | 2,723,660 | 2 | 90,630,659 | 86.43 | 2026-11-04 22:19:05 |  |
| genomics_bronze | gtex_gene_median_tpm | MANAGED | 59,033 | 1 | 14,032,223 | 13.38 | 2026-11-04 20:36:52 |  |
| genomics_bronze | gtr_test_condition_gene | MANAGED | 512,630 | 1 | 6,034,927 | 5.76 | 2026-11-04 20:36:58 |  |
| genomics_bronze | hpo_genes_to_phenotype | MANAGED | 333,983 | 1 | 1,823,754 | 1.74 | 2026-11-04 20:37:02 |  |
| genomics_bronze | hpo_phenotype_annotations | MANAGED | 286,651 | 1 | 1,543,954 | 1.47 | 2026-11-04 20:37:07 |  |
| genomics_bronze | hpo_term_relations | MANAGED | 24,436 | 1 | 132,252 | 0.13 | 2026-11-04 20:37:16 |  |
| genomics_bronze | hpo_terms | MANAGED | 20,482 | 1 | 880,437 | 0.84 | 2026-11-04 20:37:11 |  |
| genomics_bronze | iedb_epitopes | MANAGED | 2,910,311 | 13 | 66,999,420 | 63.9 | 2026-11-12 14:11:43 |  |
| genomics_bronze | iedb_mhc_ligands | MANAGED | 5,777,975 | 2 | 115,813,362 | 110.45 | 2026-11-12 16:05:22 |  |
| genomics_bronze | long_range_benchmark_causal_eqtl | MANAGED | 97,922 | 1 | 1,261,892 | 1.2 | 2026-11-04 20:37:21 |  |
| genomics_bronze | long_range_benchmark_pathogenic | MANAGED | 2,361,125 | 2 | 64,806,212 | 61.8 | 2026-11-12 14:21:04 |  |
| genomics_bronze | long_range_benchmark_pathogenic_coding | MANAGED | 39,652 | 1 | 737,065 | 0.7 | 2026-11-04 20:37:25 |  |
| genomics_bronze | long_range_benchmark_pathogenic_non_coding | MANAGED | 2,321,473 | 8 | 65,713,590 | 62.67 | 2026-11-04 20:37:31 |  |
| genomics_bronze | maps_embeddings_metadata | MANAGED | 132,565 | 13 | 707,381,343 | 674.61 | 2026-11-04 20:37:40 |  |
| genomics_bronze | medgen_concepts | MANAGED | 753,372 | 2 | 16,167,188 | 15.42 | 2026-11-04 20:37:46 |  |
| genomics_bronze | medgen_relations | MANAGED | 1,159,163 | 2 | 13,214,044 | 12.6 | 2026-11-04 20:37:52 |  |
| genomics_bronze | ncbi_gene_gene2refseq | MANAGED | 849,265 | 2 | 12,248,195 | 11.68 | 2026-11-04 20:37:58 |  |
| genomics_bronze | ncbi_gene_gene_info | MANAGED | 193,962 | 1 | 4,718,514 | 4.5 | 2026-11-04 20:38:03 |  |
| genomics_bronze | ncbi_gene_mim2gene_medgen | MANAGED | 28,709 | 1 | 213,466 | 0.2 | 2026-11-04 20:38:07 |  |
| genomics_bronze | opentargets_association_by_datasource_direct | MANAGED | 440,000 | 2 | 93,665,363 | 89.33 | 2026-11-04 22:18:59 |  |
| genomics_bronze | opentargets_association_direct | MANAGED | 880,000 | 4 | 195,240,545 | 186.2 | 2026-11-12 14:21:10 |  |
| genomics_bronze | opentargets_association_overall_direct | MANAGED | 440,000 | 2 | 96,109,928 | 91.66 | 2026-11-04 22:19:11 |  |
| genomics_bronze | opentargets_disease | MANAGED | 45,896 | 1 | 6,483,652 | 6.18 | 2026-11-04 20:38:31 |  |
| genomics_bronze | opentargets_drug_mechanism_of_action | MANAGED | 6,092 | 1 | 267,810 | 0.26 | 2026-11-04 20:38:35 |  |
| genomics_bronze | opentargets_drug_molecule | MANAGED | 19,170 | 1 | 6,969,746 | 6.65 | 2026-11-04 20:38:40 |  |
| genomics_bronze | opentargets_evidence_gwas_credible_sets | MANAGED | 3,445,546 | 4 | 220,180,404 | 209.98 | 2026-11-13 00:10:34 |  |
| genomics_bronze | opentargets_target | MANAGED | 78,733 | 6 | 28,025,565 | 26.73 | 2026-11-04 20:38:57 |  |
| genomics_bronze | pharmgkb_drugs | MANAGED | 3,770 | 1 | 631,590 | 0.6 | 2026-11-04 20:39:01 |  |
| genomics_bronze | pharmgkb_genes | MANAGED | 25,041 | 1 | 2,792,070 | 2.66 | 2026-11-04 20:39:06 |  |
| genomics_bronze | pharmgkb_relationships | MANAGED | 246,714 | 1 | 2,198,300 | 2.1 | 2026-11-04 20:39:10 |  |
| genomics_bronze | pharmgkb_variants | MANAGED | 7,692 | 1 | 961,390 | 0.92 | 2026-11-04 20:39:14 |  |
| genomics_bronze | proteingym_clinical_reference | MANAGED | 2,525 | 1 | 1,068,180 | 1.02 | 2026-11-04 20:39:20 |  |
| genomics_bronze | proteingym_clinical_substitutions | MANAGED | 62,727 | 1 | 308,074 | 0.29 | 2026-11-04 20:39:24 |  |
| genomics_bronze | proteingym_dms_reference | MANAGED | 96 | 1 | 52,100 | 0.05 | 2026-11-04 20:39:28 |  |
| genomics_bronze | proteingym_dms_substitutions | MANAGED | 485,830 | 1 | 4,562,048 | 4.35 | 2026-11-04 20:39:33 |  |
| genomics_bronze | proteingym_reference | MANAGED | 2,621 | 2 | 1,129,178 | 1.08 | 2026-11-12 14:21:14 |  |
| genomics_bronze | proteingym_substitutions | MANAGED | 548,557 | 2 | 4,871,643 | 4.65 | 2026-11-12 14:21:18 |  |
| genomics_bronze | reactome_pathway_mappings | MANAGED | 320,280 | 1 | 877,930 | 0.84 | 2026-11-04 20:39:43 |  |
| genomics_bronze | reactome_pathway_relations | MANAGED | 2,905 | 1 | 18,123 | 0.02 | 2026-11-04 20:39:47 |  |
| genomics_bronze | reactome_pathways | MANAGED | 2,889 | 1 | 50,087 | 0.05 | 2026-11-04 20:39:38 |  |
| genomics_bronze | refseq_annotation | MANAGED | 207,289 | 1 | 5,143,190 | 4.9 | 2026-11-04 20:39:52 |  |
| genomics_bronze | spectralbio_variants | MANAGED | 867 | 1 | 14,103 | 0.01 | 2026-11-04 20:39:56 |  |
| genomics_bronze | string_protein_aliases | MANAGED | 3,889,207 | 4 | 15,118,314 | 14.42 | 2026-11-04 20:40:03 |  |
| genomics_bronze | string_protein_info | MANAGED | 19,699 | 1 | 1,747,819 | 1.67 | 2026-11-04 20:40:07 |  |
| genomics_bronze | string_protein_links | MANAGED | 10,826,054 | 9 | 65,640,604 | 62.6 | 2026-11-04 20:40:17 |  |
| genomics_bronze | tahoe_cell_line_metadata | MANAGED | 1,000 | 1 | 12,002 | 0.01 | 2026-11-04 20:40:21 |  |
| genomics_bronze | tahoe_drug_metadata | MANAGED | 379 | 1 | 25,237 | 0.02 | 2026-11-04 20:40:25 |  |
| genomics_bronze | tahoe_gene_metadata | MANAGED | 62,710 | 1 | 523,565 | 0.5 | 2026-11-04 20:40:29 |  |
| genomics_bronze | tahoe_pseudobulk_differential_expression | MANAGED | 10,772,362 | 3 | 143,116,168 | 136.49 | 2026-11-13 00:10:26 |  |
| genomics_bronze | tahoe_sample_metadata | MANAGED | 1,344 | 1 | 59,022 | 0.06 | 2026-11-04 20:40:47 |  |
| genomics_bronze | tcga_mc3_somatic_mutations | MANAGED | 3,600,963 | 2 | 108,634,227 | 103.6 | 2026-11-11 23:04:15 |  |
| genomics_bronze | thousand_genomes_variants | MANAGED | 544,361 | 2 | 9,622,124 | 9.18 | 2026-11-12 14:12:01 |  |
| genomics_bronze | ucsc_conservation_100way | MANAGED | 8,110,308 | 2 | 32,131,458 | 30.64 | 2026-11-12 14:21:22 |  |
| genomics_bronze | ucsc_conservation_phastcons_100way | MANAGED | 4,055,154 | 2 | 13,755,146 | 13.12 | 2026-11-04 20:41:05 |  |
| genomics_bronze | ucsc_conservation_phylop_100way | MANAGED | 4,055,154 | 2 | 18,424,566 | 17.57 | 2026-11-04 20:41:12 |  |
| genomics_bronze | uniprot_entries | MANAGED | 20,431 | 1 | 372,643 | 0.36 | 2026-11-04 20:41:17 |  |
| genomics_bronze | uniprot_features | MANAGED | 283,500 | 1 | 4,209,336 | 4.01 | 2026-11-04 20:41:21 |  |
| genomics_bronze | variant_benchmark_sets | MANAGED | 12,042,448 | 2 | 50,990,366 | 48.63 | 2026-11-12 14:21:38 |  |
| genomics_silver | benchmark_variant | MANAGED | 14,697,654 | 16 | 709,453,939 | 676.59 | 2026-11-11 21:24:40 |  |
| genomics_silver | dms_assay | MANAGED | 2,621 | 1 | 1,225,252 | 1.17 | 2026-11-11 21:25:26 |  |
| genomics_silver | dms_assay_mutant | MANAGED | 548,557 | 1 | 24,350,739 | 23.22 | 2026-11-11 21:25:51 |  |
| genomics_silver | embedding_split | MANAGED | 2 | 1 | 27,855 | 0.03 | 2026-11-11 21:25:06 |  |
| genomics_silver | entity_relation | MANAGED | 18,351,838 | 16 | 914,175,828 | 871.83 | 2026-11-11 21:58:01 |  |
| genomics_silver | faers_case | MANAGED | 819,683 | 1 | 47,339,748 | 45.15 | 2026-11-11 21:32:18 |  |
| genomics_silver | faers_case_drug | MANAGED | 3,328,534 | 3 | 153,723,267 | 146.6 | 2026-11-11 21:32:39 |  |
| genomics_silver | faers_case_item | MANAGED | 6,235,415 | 2 | 259,535,971 | 247.51 | 2026-11-11 21:33:07 |  |
| genomics_silver | gene_expression_median | MANAGED | 4,014,244 | 1 | 150,305,278 | 143.34 | 2026-11-11 21:28:48 |  |
| genomics_silver | locus_score | MANAGED | 20,025,911 | 14 | 809,202,206 | 771.72 | 2026-11-12 22:49:13 |  |
| genomics_silver | perturbation_expression | MANAGED | 10,772,362 | 10 | 615,485,499 | 586.97 | 2026-11-11 21:31:51 |  |
| genomics_silver | perturbation_sample | MANAGED | 1,344 | 1 | 113,051 | 0.11 | 2026-11-11 21:31:14 |  |
| genomics_silver | somatic_mutation | MANAGED | 3,600,920 | 8 | 267,204,959 | 254.83 | 2026-11-11 21:30:55 |  |
| genomics_silver | tumor_sample | MANAGED | 10,295 | 1 | 230,380 | 0.22 | 2026-11-11 21:29:25 |  |
| genomics_silver | variant | MANAGED | 22,822,212 | 7 | 526,558,021 | 502.16 | 2026-11-12 22:23:00 |  |
| genomics_silver | variant_population_frequency | MANAGED | 40,154,448 | 23 | 1,745,971,395 | 1665.09 | 2026-11-12 22:50:19 |  |
| genomics_silver_reference | cell_line | MANAGED | 3,450 | 1 | 322,656 | 0.31 | 2026-11-12 22:19:23 |  |
| genomics_silver_reference | entity_coordinate | MANAGED | 4,043,423 | 2 | 175,664,597 | 167.53 | 2026-11-12 22:17:04 |  |
| genomics_silver_reference | entity_derived_attribute | MANAGED | 121,645,426 | 113 | 5,317,598,324 | 5071.26 | 2026-11-11 21:41:18 |  |
| genomics_silver_reference | entity_id_crosswalk | MANAGED | 37,444,747 | 34 | 1,630,527,625 | 1554.99 | 2026-11-12 22:02:49 |  |
| genomics_silver_reference | entity_master | MANAGED | 4,531,185 | 14 | 469,105,170 | 447.37 | 2026-11-12 22:18:12 |  |
| genomics_silver_reference | entity_name | MANAGED | 15,993,643 | 13 | 726,534,335 | 692.88 | 2026-11-12 22:04:52 |  |
| genomics_silver_reference | genetic_test | MANAGED | 512,638 | 1 | 20,611,485 | 19.66 | 2026-11-12 22:19:49 |  |
| genomics_silver_reference | ontology_relation | MANAGED | 2,115,093 | 1 | 93,497,688 | 89.17 | 2026-11-12 22:20:13 |  |
| genomics_silver_reference | protein_feature | MANAGED | 550,016 | 2 | 47,256,601 | 45.07 | 2026-11-12 22:19:04 |  |
| genomics_silver_reference | protein_gene_link | MANAGED | 491,209 | 1 | 19,656,564 | 18.75 | 2026-11-12 22:18:38 |  |
| knowledge | rerank_scores | MANAGED | 5,453 | 5 | 210,864 | 0.2 | 2026-10-27 22:32:22 |  |
| knowledge | wikipedia_article | MANAGED | 4,428,568 | 86 | 5,140,253,845 | 4902.13 | 2026-10-26 21:16:59 |  |
| knowledge | wikipedia_chunks | MANAGED | 209,100 | 6 | 358,528,560 | 341.92 | 2026-10-13 23:27:05 |  |
| knowledge | wikipedia_clickstream | MANAGED | 22,509,883 | 8 | 416,598,429 | 397.3 | 2026-11-12 23:03:30 |  |
| knowledge | wikipedia_edges | MANAGED | 8,831 | 1 | 173,401 | 0.17 | 2026-10-13 18:37:21 |  |
| knowledge | wikipedia_selected | MANAGED | 33,443 | 1 | 63,105,105 | 60.18 | 2026-10-16 14:31:27 |  |
| quality | field_class_registry | MANAGED | 3,764 | 4 | 40,848 | 0.04 | 2026-11-12 19:24:45 |  |
| quality | pipeline_watermarks | MANAGED | 1,471 | 14 | 71,085 | 0.07 | 2026-11-12 23:29:01 |  |
| quality | quality_audit_log | MANAGED | 4,492 | 12 | 96,077 | 0.09 | 2026-11-12 23:29:03 |  |
| quality | quarantine | MANAGED | 1,882,083 | 15 | 38,987,757 | 37.18 | 2026-11-12 22:06:33 |  |
| shared_conformed | data_gap_period | MANAGED | 5,817,858 | 3 | 229,350,234 | 218.73 | 2026-11-12 19:32:37 |  |
| shared_conformed | dim_date | MANAGED | 50,768 | 8 | 70,358 | 0.07 | 2026-09-28 10:47:24 |  |
| shared_conformed | dim_geography | MANAGED | 17 | 1 | 3,300 | 0.0 | 2026-09-28 10:45:45 |  |
| shared_conformed | dim_place | MANAGED | 80 | 1 | 15,623 | 0.01 | 2026-09-28 10:48:10 |  |
| shared_conformed | dim_place_validity | MANAGED | 322 | 2 | 30,168 | 0.03 | 2026-09-28 10:48:19 |  |
| shared_conformed | dim_time | MANAGED | 1,440 | 2 | 5,383 | 0.01 | 2026-09-28 10:45:41 |  |
| shared_conformed | dim_weather_context | MANAGED | 0 | 0 | 0 | 0.0 | 2026-09-27 17:23:55 |  |
| shared_conformed | geo_plz_gemeinde_xref | MANAGED | 0 | 0 | 0 | 0.0 | 2026-09-27 17:23:53 |  |
| shared_conformed | quality_exception | VIEW | 469,984 |  |  |  |  |  |
| shared_conformed | ref_energy_carrier | MANAGED | 84 | 1 | 4,200 | 0.0 | 2026-09-28 12:27:48 |  |
| shared_conformed | ref_energy_carrier_map | MANAGED | 95 | 1 | 5,573 | 0.01 | 2026-09-28 12:27:56 |  |
| shared_conformed | ref_measure | MANAGED | 55 | 1 | 6,210 | 0.01 | 2026-09-28 10:46:44 |  |
| shared_conformed | ref_vocabulary | MANAGED | 1,912 | 1 | 92,600 | 0.09 | 2026-09-28 10:46:43 |  |
| shared_conformed | series_coverage | MANAGED | 1,391 | 1 | 61,122 | 0.06 | 2026-09-28 12:38:51 |  |

## Volumes

| schema | volume | type |
| --- | --- | --- |
| bronze | landing | MANAGED |
| energy_ml_models | foundation_models | MANAGED |
| energy_ml_models | model_libraries | MANAGED |
| knowledge | models | MANAGED |
| knowledge | rerank | MANAGED |

## Errors (10)

| schema | table | error |
| --- | --- | --- |
| commerce_gold | ga4_event | count: [UC_DEPENDENCY_DOES_NOT_EXIST] Dependency does not exist in Unity Catalog:  Table 'energy_commerce_retail_media.commerce_gold.ga4_event' is invalid because one of the underlying resources does not exi |
| commerce_gold | rees46_event | count: [UC_DEPENDENCY_DOES_NOT_EXIST] Dependency does not exist in Unity Catalog:  Table 'energy_commerce_retail_media.commerce_gold.rees46_event' is invalid because one of the underlying resources does not  |
| energy_gold | generation_forecast | count: [UC_DEPENDENCY_DOES_NOT_EXIST] Dependency does not exist in Unity Catalog:  Table 'energy_commerce_retail_media.energy_gold.generation_forecast' is invalid because one of the underlying resources does |
| energy_gold | grid_intervention_event | count: [INCOMPATIBLE_VIEW_SCHEMA_CHANGE] The SQL query of view `energy_commerce_retail_media`.`energy_gold`.`grid_intervention_event` has an incompatible schema change and column requesting_market_area_codes |
| energy_gold | grid_location_coordinate_conflict | count: [UC_DEPENDENCY_DOES_NOT_EXIST] Dependency does not exist in Unity Catalog:  Table 'energy_commerce_retail_media.energy_gold.grid_location_coordinate_conflict' is invalid because one of the underlying  |
| energy_gold | market_actor_role | count: [INCOMPATIBLE_VIEW_SCHEMA_CHANGE] The SQL query of view `energy_commerce_retail_media`.`energy_gold`.`market_actor_role` has an incompatible schema change and column BundesnetzagenturBetriebsnummer_nv |
| energy_gold | market_price | count: [UC_DEPENDENCY_DOES_NOT_EXIST] Dependency does not exist in Unity Catalog:  Table 'energy_commerce_retail_media.energy_gold.market_price' is invalid because one of the underlying resources does not ex |
| energy_gold | plant_sensor_observation | count: [INCOMPATIBLE_VIEW_SCHEMA_CHANGE] The SQL query of view `energy_commerce_retail_media`.`energy_gold`.`plant_sensor_observation` has an incompatible schema change and column quality_flags cannot be res |
| energy_gold | support_registration | count: [INCOMPATIBLE_VIEW_SCHEMA_CHANGE] The SQL query of view `energy_commerce_retail_media`.`energy_gold`.`support_registration` has an incompatible schema change and column AnlagenkennzifferAnlagenregiste |
| energy_gold | unit_authorisation | count: [INCOMPATIBLE_VIEW_SCHEMA_CHANGE] The SQL query of view `energy_commerce_retail_media`.`energy_gold`.`unit_authorisation` has an incompatible schema change and column Frist_nv cannot be resolved. Expe |
