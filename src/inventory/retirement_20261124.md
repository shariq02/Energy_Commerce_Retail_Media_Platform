# Layer retirement plan

_Generated: 2026-11-24T16:46Z_

Switches: tables and views True, Volumes False, schemas False

## Schemas to drop

| layer | schema | tables | views | volumes | decision |
| --- | --- | --- | --- | --- | --- |

## Successor check

| layer | successor schema | tables | problem | passes |
| --- | --- | --- | --- | --- |

## Bronze tables merged into another table

| merged table | value | part table | part rows | merged rows | decision |
| --- | --- | --- | --- | --- | --- |
| dwd_measurement | air_temperature | dwd_air_temperature |  | 17,336,388 | gone |
| dwd_measurement | cloud_type | dwd_cloud_type |  | 13,101,113 | gone |
| dwd_measurement | cloudiness | dwd_cloudiness |  | 12,972,965 | gone |
| dwd_measurement | dew_point | dwd_dew_point |  | 13,524,783 | gone |
| dwd_measurement | extreme_wind | dwd_extreme_wind |  | 6,480,839 | gone |
| dwd_measurement | moisture | dwd_moisture |  | 12,450,186 | gone |
| dwd_measurement | precipitation | dwd_precipitation |  | 7,154,301 | gone |
| dwd_measurement | pressure | dwd_pressure |  | 13,227,922 | gone |
| dwd_measurement | soil_temperature | dwd_soil_temperature |  | 7,182,993 | gone |
| dwd_measurement | solar | dwd_solar |  | 5,846,785 | gone |
| dwd_measurement | sun | dwd_sun |  | 12,642,243 | gone |
| dwd_measurement | visibility | dwd_visibility |  | 13,123,637 | gone |
| dwd_measurement | weather_phenomena | dwd_weather_phenomena |  | 12,914,405 | gone |
| dwd_measurement | wind | dwd_wind |  | 15,957,048 | gone |
| dwd_measurement | wind_synop | dwd_wind_synop |  | 13,524,783 | gone |
| mastr_einheiten | biomasse | mastr_einheiten_biomasse |  | 24,334 | gone |
| mastr_einheiten | electricity_storage | mastr_einheiten_stromspeicher |  | 2,774,472 | gone |
| mastr_einheiten | gas_consumer | mastr_einheiten_gasverbraucher |  | 902 | gone |
| mastr_einheiten | gas_producer | mastr_einheiten_gaserzeuger |  | 398 | gone |
| mastr_einheiten | gas_storage | mastr_einheiten_gasspeicher |  | 56 | gone |
| mastr_einheiten | geothermie_gsgk | mastr_einheiten_geothermie_gsgk |  | 336 | gone |
| mastr_einheiten | kernkraft | mastr_einheiten_kernkraft |  | 6 | gone |
| mastr_einheiten | solar | mastr_einheiten_solar |  | 6,460,359 | gone |
| mastr_einheiten | verbrennung | mastr_einheiten_verbrennung |  | 94,027 | gone |
| mastr_einheiten | wasser | mastr_einheiten_wasser |  | 8,814 | gone |
| mastr_einheiten | wind | mastr_einheiten_wind |  | 43,457 | gone |
| mastr_anlagen_eeg | biomasse | mastr_anlagen_eeg_biomasse |  | 15,644 | gone |
| mastr_anlagen_eeg | geothermie_gsgk | mastr_anlagen_eeg_geothermie_gsgk |  | 132 | gone |
| mastr_anlagen_eeg | solar | mastr_anlagen_eeg_solar |  | 6,460,359 | gone |
| mastr_anlagen_eeg | storage | mastr_anlagen_eeg_speicher |  | 2,717,856 | gone |
| mastr_anlagen_eeg | wasser | mastr_anlagen_eeg_wasser |  | 7,485 | gone |
| mastr_anlagen_eeg | wind | mastr_anlagen_eeg_wind |  | 43,457 | gone |
| mastr_anlagen_speicher | electricity | mastr_anlagen_stromspeicher |  | 2,774,391 | gone |
| mastr_anlagen_speicher | gas | mastr_anlagen_gasspeicher |  | 55 | gone |
| honda_iot_meter | cooling | honda_iot_cooling |  | 6,835,855 | gone |
| honda_iot_meter | electricity | honda_iot_electricity |  | 6,835,916 | gone |
| honda_iot_meter | heating | honda_iot_heating |  | 6,835,906 | gone |
| mastr_code_lookup | katalogwerte | mastr_katalogwerte |  | 1,737 | gone |
| long_range_benchmark_pathogenic | coding | long_range_benchmark_pathogenic_coding |  | 39,652 | gone |
| long_range_benchmark_pathogenic | non_coding | long_range_benchmark_pathogenic_non_coding |  | 2,321,473 | gone |
| opentargets_association_direct | datasource | opentargets_association_by_datasource_direct |  | 7,522,466 | gone |
| opentargets_association_direct | overall | opentargets_association_overall_direct |  | 7,225,847 | gone |
| proteingym_reference | clinical | proteingym_clinical_reference |  | 2,525 | gone |
| proteingym_reference | dms | proteingym_dms_reference |  | 96 | gone |
| proteingym_substitutions | clinical | proteingym_clinical_substitutions |  | 62,727 | gone |
| proteingym_substitutions | dms | proteingym_dms_substitutions |  | 485,830 | gone |
| ucsc_conservation_100way | phastcons | ucsc_conservation_phastcons_100way |  | 4,055,154 | gone |
| ucsc_conservation_100way | phylop | ucsc_conservation_phylop_100way |  | 4,055,154 | gone |
| genome_annotation | gencode | gencode_annotation |  | 6,888,656 | gone |
| genome_annotation | refseq | refseq_annotation |  | 207,289 | gone |
| faers_case_code | outcome | faers_outc |  | 3,011,718 | gone |
| faers_case_code | report_source | faers_rpsr |  | 112,272 | gone |
| reference_small | gnomad_constraint | gnomad_constraint_metrics |  | 36,849 | gone |
| reference_small | tcga_site_code | tcga_site_codes |  | 830 | gone |
| reference_small | blosum62 | blosum62_scores | 576 | 576 | drop |
| variant_benchmark_sets | clinvar_vs_benign | clinvar_vs_benign_variants |  | 50,164 | gone |
| variant_benchmark_sets | gnomad_balanced | gnomad_balanced_variants |  | 11,992,284 | gone |
