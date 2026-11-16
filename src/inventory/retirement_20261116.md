# Layer retirement plan

_Generated: 2026-11-16T18:20Z_

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
| dwd_measurement | air_temperature | dwd_air_temperature | 17,336,388 | 17,336,388 | drop |
| dwd_measurement | cloud_type | dwd_cloud_type | 13,101,463 | 13,101,463 | drop |
| dwd_measurement | cloudiness | dwd_cloudiness | 12,973,316 | 12,973,316 | drop |
| dwd_measurement | dew_point | dwd_dew_point | 13,524,783 | 13,524,783 | drop |
| dwd_measurement | extreme_wind | dwd_extreme_wind | 6,480,621 | 6,480,621 | drop |
| dwd_measurement | moisture | dwd_moisture | 12,449,858 | 12,449,858 | drop |
| dwd_measurement | precipitation | dwd_precipitation | 7,154,301 | 7,154,301 | drop |
| dwd_measurement | pressure | dwd_pressure | 13,227,922 | 13,227,922 | drop |
| dwd_measurement | soil_temperature | dwd_soil_temperature | 7,183,226 | 7,183,226 | drop |
| dwd_measurement | solar | dwd_solar | 5,809,249 | 5,809,249 | drop |
| dwd_measurement | sun | dwd_sun | 12,650,564 | 12,650,564 | drop |
| dwd_measurement | visibility | dwd_visibility | 13,124,582 | 13,124,582 | drop |
| dwd_measurement | weather_phenomena | dwd_weather_phenomena | 12,915,288 | 12,915,288 | drop |
| dwd_measurement | wind | dwd_wind | 15,957,198 | 15,957,198 | drop |
| dwd_measurement | wind_synop | dwd_wind_synop | 13,524,783 | 13,524,783 | drop |
| mastr_einheiten | biomasse | mastr_einheiten_biomasse | 24,334 | 24,334 | drop |
| mastr_einheiten | geothermie_gsgk | mastr_einheiten_geothermie_gsgk | 336 | 336 | drop |
| mastr_einheiten | kernkraft | mastr_einheiten_kernkraft | 6 | 6 | drop |
| mastr_einheiten | verbrennung | mastr_einheiten_verbrennung | 94,027 | 94,027 | drop |
| mastr_einheiten | wasser | mastr_einheiten_wasser | 8,814 | 8,814 | drop |
| mastr_einheiten | wind | mastr_einheiten_wind | 43,457 | 43,457 | drop |
| mastr_anlagen_eeg | biomasse | mastr_anlagen_eeg_biomasse | 15,644 | 15,644 | drop |
| mastr_anlagen_eeg | geothermie_gsgk | mastr_anlagen_eeg_geothermie_gsgk | 132 | 132 | drop |
| mastr_anlagen_eeg | wasser | mastr_anlagen_eeg_wasser | 7,485 | 7,485 | drop |
| mastr_anlagen_eeg | wind | mastr_anlagen_eeg_wind | 43,457 | 43,457 | drop |
| honda_iot_meter | cooling | honda_iot_cooling | 6,835,855 | 6,835,855 | drop |
| honda_iot_meter | electricity | honda_iot_electricity | 6,835,916 | 6,835,916 | drop |
| honda_iot_meter | heating | honda_iot_heating | 6,835,906 | 6,835,906 | drop |
| mastr_code_lookup | katalogwerte | mastr_katalogwerte | 1,737 | 1,737 | drop |
| long_range_benchmark_pathogenic | coding | long_range_benchmark_pathogenic_coding | 39,652 | 39,652 | drop |
| long_range_benchmark_pathogenic | non_coding | long_range_benchmark_pathogenic_non_coding | 2,321,473 | 2,321,473 | drop |
| opentargets_association_direct | datasource | opentargets_association_by_datasource_direct | 440,000 | 440,000 | drop |
| opentargets_association_direct | overall | opentargets_association_overall_direct | 440,000 | 440,000 | drop |
| proteingym_reference | clinical | proteingym_clinical_reference | 2,525 | 2,525 | drop |
| proteingym_reference | dms | proteingym_dms_reference | 96 | 96 | drop |
| proteingym_substitutions | clinical | proteingym_clinical_substitutions | 62,727 | 62,727 | drop |
| proteingym_substitutions | dms | proteingym_dms_substitutions | 485,830 | 485,830 | drop |
| ucsc_conservation_100way | phastcons | ucsc_conservation_phastcons_100way | 4,055,154 | 4,055,154 | drop |
| ucsc_conservation_100way | phylop | ucsc_conservation_phylop_100way | 4,055,154 | 4,055,154 | drop |
| genome_annotation | gencode | gencode_annotation | 425,844 | 425,844 | drop |
| genome_annotation | refseq | refseq_annotation | 207,289 | 207,289 | drop |
| faers_case_code | outcome | faers_outc | 595,285 | 595,285 | drop |
| faers_case_code | report_source | faers_rpsr | 22,721 | 22,721 | drop |
| variant_benchmark_sets | clinvar_vs_benign | clinvar_vs_benign_variants | 50,164 | 50,164 | drop |
| variant_benchmark_sets | gnomad_balanced | gnomad_balanced_variants | 11,992,284 | 11,992,284 | drop |
