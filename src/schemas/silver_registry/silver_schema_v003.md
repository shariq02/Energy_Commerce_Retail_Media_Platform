# Silver schema snapshot v003

<!-- silver-schema-hash: d02c9a838b8322198bfc50eb8ddcc74a0e05fff932a97ca2912fc743fc14d0d3 -->

**Captured:** 2026-09-26T14:45:30Z
**Catalog / schemas:** `energy_commerce_retail_media.{energy_silver, energy_silver_reference, commerce_silver, commerce_silver_reference}`
**Tables:** 52  |  **Columns:** 1228

## Change summary

Compared with v002.

**Tables added:** (none)
**Tables removed:** (none)

**commerce_silver.ga4_event**
- column added: `event_timestamp_native` (string, nullable)
- column added: `event_timestamp_project` (timestamp, nullable)
- column added: `event_timestamp_utc` (timestamp, nullable)
- column removed: `event_ts_native`
- column removed: `event_ts_project`
- column removed: `event_ts_utc`

**commerce_silver.ga4_event_item**
- column added: `event_timestamp_utc` (timestamp, nullable)
- column removed: `event_ts_utc`

**commerce_silver.rees46_event**
- column added: `event_timestamp_native` (string, nullable)
- column added: `event_timestamp_project` (timestamp, nullable)
- column added: `event_timestamp_utc` (timestamp, nullable)
- column removed: `event_ts_native`
- column removed: `event_ts_project`
- column removed: `event_ts_utc`

**energy_silver.device_telemetry_snapshot**
- column added: `observation_timestamp_native` (string, nullable)
- column added: `observation_timestamp_utc` (timestamp, nullable)
- column removed: `observation_ts_native`
- column removed: `observation_ts_utc`

**energy_silver.electricity_balance**
- column added: `observation_timestamp_native` (string, nullable)
- column added: `observation_timestamp_project` (timestamp, nullable)
- column added: `observation_timestamp_utc` (timestamp, nullable)
- column removed: `observation_ts_native`
- column removed: `observation_ts_project`
- column removed: `observation_ts_utc`

**energy_silver.electricity_generation_forecast**
- column added: `forecast_issue_timestamp` (timestamp, nullable)
- column added: `observation_timestamp_native` (string, nullable)
- column added: `observation_timestamp_project` (timestamp, nullable)
- column added: `observation_timestamp_utc` (timestamp, nullable)
- column removed: `forecast_issue_ts`
- column removed: `observation_ts_native`
- column removed: `observation_ts_project`
- column removed: `observation_ts_utc`

**energy_silver.electricity_price**
- column added: `observation_timestamp_native` (string, nullable)
- column added: `observation_timestamp_project` (timestamp, nullable)
- column added: `observation_timestamp_utc` (timestamp, nullable)
- column removed: `observation_ts_native`
- column removed: `observation_ts_project`
- column removed: `observation_ts_utc`

**energy_silver.energy_meter_reading**
- column added: `cooling_electricity_increment_kwh` (double, nullable)
- column added: `cooling_electricity_kwh` (double, nullable)
- column added: `electricity_combined_heat_and_power_increment_kwh` (double, nullable)
- column added: `electricity_combined_heat_and_power_kwh` (double, nullable)
- column added: `electricity_solar_photovoltaic_increment_kwh` (double, nullable)
- column added: `electricity_solar_photovoltaic_kwh` (double, nullable)
- column added: `heating_combined_heat_and_power_electricity_increment_kwh` (double, nullable)
- column added: `heating_combined_heat_and_power_electricity_kwh` (double, nullable)
- column added: `heating_combined_heat_and_power_heat_increment_kwh` (double, nullable)
- column added: `heating_combined_heat_and_power_heat_kwh` (double, nullable)
- column added: `observation_timestamp_native` (string, nullable)
- column added: `observation_timestamp_project` (timestamp, nullable)
- column added: `observation_timestamp_utc` (timestamp, nullable)
- column removed: `cooling_cool_elec_increment_kwh`
- column removed: `cooling_cool_elec_kwh`
- column removed: `electricity_chp_increment_kwh`
- column removed: `electricity_chp_kwh`
- column removed: `electricity_pv_increment_kwh`
- column removed: `electricity_pv_kwh`
- column removed: `heating_chp_elec_increment_kwh`
- column removed: `heating_chp_elec_kwh`
- column removed: `heating_chp_heat_increment_kwh`
- column removed: `heating_chp_heat_kwh`
- column removed: `observation_ts_native`
- column removed: `observation_ts_project`
- column removed: `observation_ts_utc`

**energy_silver.generation_unit**
- column added: `additional_fuels` (string, nullable)
- column added: `address_supplement` (string, nullable)
- column added: `assigned_to_capacity_reserve` (string, nullable)
- column added: `assigned_to_grid_reserve` (string, nullable)
- column added: `baltic_sea_area_development_plan_zone` (string, nullable)
- column added: `cadastral_district` (string, nullable)
- column added: `capacity_reserve_date` (timestamp, nullable)
- column added: `citizen_energy` (string, nullable)
- column added: `combined_heat_and_power_support_id` (string, nullable)
- column added: `combined_operation_net_rated_capacity_increase` (string, nullable)
- column added: `combined_operation_unit_ids` (string, nullable)
- column added: `connected_to_extra_high_or_high_voltage` (string, nullable)
- column added: `construction_start_date` (timestamp, nullable)
- column added: `curtailment_condition_animal_protection` (string, nullable)
- column added: `curtailment_condition_ice_throw` (string, nullable)
- column added: `curtailment_condition_noise_protection_day` (string, nullable)
- column added: `curtailment_condition_noise_protection_night` (string, nullable)
- column added: `curtailment_condition_other` (string, nullable)
- column added: `curtailment_condition_power_limit` (string, nullable)
- column added: `curtailment_condition_shadow_flicker` (string, nullable)
- column added: `deployment_location` (string, nullable)
- column added: `deployment_location_code` (string, nullable)
- column added: `deployment_location_label_de` (string, nullable)
- column added: `electricity_generation_reduction` (string, nullable)
- column added: `emergency_generator` (string, nullable)
- column added: `exclusive_use_in_combined_operation` (string, nullable)
- column added: `grid_operator_check_date` (timestamp, nullable)
- column added: `grid_operator_check_status` (string, nullable)
- column added: `grid_reserve_date` (timestamp, nullable)
- column added: `house_number` (string, nullable)
- column added: `house_number_not_found` (string, nullable)
- column added: `land_area_used` (string, nullable)
- column added: `land_parcel_numbers` (string, nullable)
- column added: `night_marking` (string, nullable)
- column added: `north_sea_area_development_plan_zone` (string, nullable)
- column added: `official_municipality_key` (string, nullable)
- column added: `official_municipality_key_level` (string, nullable)
- column added: `official_municipality_key_method` (string, nullable)
- column added: `operationally_responsible_party` (string, nullable)
- column added: `operator_change_date` (timestamp, nullable)
- column added: `operator_change_registration_date` (timestamp, nullable)
- column added: `part_of_cross_border_plant` (string, nullable)
- column added: `power_plant_number` (string, nullable)
- column added: `predominant_land_use_before_construction` (string, nullable)
- column added: `previous_land_use_category` (string, nullable)
- column added: `renewable_energy_act_support_id` (string, nullable)
- column added: `resource_energy_identification_code` (string, nullable)
- column added: `resource_energy_identification_code_display_name` (string, nullable)
- column added: `rotor_blade_deicing_system` (string, nullable)
- column added: `security_standby_start_date` (timestamp, nullable)
- column added: `source_municipality_key` (string, nullable)
- column added: `street` (string, nullable)
- column added: `street_not_found` (string, nullable)
- column added: `unit_is_in_combined_operation` (string, nullable)
- column removed: `Adresszusatz`
- column removed: `AnlageIstImKombibetrieb`
- column removed: `AnschlussAnHoechstOderHochSpannung`
- column removed: `AuflageAbschaltungLeistungsbegrenzung`
- column removed: `AuflagenAbschaltungEiswurf`
- column removed: `AuflagenAbschaltungSchallimmissionsschutzNachts`
- column removed: `AuflagenAbschaltungSchallimmissionsschutzTagsueber`
- column removed: `AuflagenAbschaltungSchattenwurf`
- column removed: `AuflagenAbschaltungSonstige`
- column removed: `AuflagenAbschaltungTierschutz`
- column removed: `AusschliesslicheVerwendungImKombibetrieb`
- column removed: `BestandteilGrenzkraftwerk`
- column removed: `Buergerenergie`
- column removed: `DatumBaubeginn`
- column removed: `DatumDesBetreiberwechsels`
- column removed: `DatumKapazitaetsreserve`
- column removed: `DatumNetzreserve`
- column removed: `DatumRegistrierungDesBetreiberwechsels`
- column removed: `Einsatzort`
- column removed: `Einsatzort_code`
- column removed: `Einsatzort_label_de`
- column removed: `Einsatzverantwortlicher`
- column removed: `FlurFlurstuecknummern`
- column removed: `GebietNachDemFlaechenentwicklungsplanNordsee`
- column removed: `GebietNachDemFlaechenentwicklungsplanOstsee`
- column removed: `Gemarkung`
- column removed: `GroesseDerInAnspruchGenommenenFlaeche`
- column removed: `Hausnummer`
- column removed: `HausnummerNichtGefunden`
- column removed: `KapazitaetsreserveZugeordnet`
- column removed: `Kraftwerksnummer`
- column removed: `MastrNummernKombibetrieb`
- column removed: `MinderungStromerzeugung`
- column removed: `Nachtkennzeichnung`
- column removed: `NetzbetreiberpruefungDatum`
- column removed: `NetzbetreiberpruefungStatus`
- column removed: `NetzreserveZugeordnet`
- column removed: `Notstromaggregat`
- column removed: `Rotorblattenteisungssystem`
- column removed: `SicherheitsbereitschaftAbDatum`
- column removed: `SteigerungNettonennleistungKombibetrieb`
- column removed: `Strasse`
- column removed: `StrasseNichtGefunden`
- column removed: `UeberwiegendeNutzungsartDerFlaecheVorErrichtung`
- column removed: `VorherigerNutzungsartenbereichDerFlaeche`
- column removed: `Weic`
- column removed: `WeicDisplayName`
- column removed: `WeitereBrennstoffe`
- column removed: `ags_code`
- column removed: `ags_level`
- column removed: `ags_method`
- column removed: `eeg_support_id`
- column removed: `kwk_support_id`
- column removed: `municipality_key_ags`
- 3 column(s) changed ordinal position

**energy_silver.grid_intervention_event**
- column added: `instructing_transmission_system_operator_native` (string, nullable)
- column added: `requesting_transmission_system_operator_native` (string, nullable)
- column removed: `instructing_tso_native`
- column removed: `requesting_tso_native`

**energy_silver.grid_operator_change_event**
- column added: `_source_id_disambiguated` (boolean, nullable)
- column added: `_source_id_ordinal` (int, nullable)
- column removed: `_src_id_disambiguated`
- column removed: `_src_id_ord`

**energy_silver.plant_operating_sample**
- column added: `exhaust_vacuum_cm_of_mercury` (double, nullable)
- column removed: `exhaust_vacuum_cm_hg`

**energy_silver.power_plant_capacity_plan**
- column added: `_source_id_disambiguated` (boolean, nullable)
- column added: `_source_id_ordinal` (int, nullable)
- column removed: `_src_id_disambiguated`
- column removed: `_src_id_ord`

**energy_silver.power_plant_register**
- column added: `_source_id_disambiguated` (boolean, nullable)
- column added: `_source_id_ordinal` (int, nullable)
- column added: `official_municipality_key` (string, nullable)
- column added: `official_municipality_key_level` (string, nullable)
- column added: `official_municipality_key_method` (string, nullable)
- column removed: `_src_id_disambiguated`
- column removed: `_src_id_ord`
- column removed: `ags_code`
- column removed: `ags_level`
- column removed: `ags_method`

**energy_silver.support_registration**
- column added: `biogas_capacity_increase_date` (timestamp, nullable)
- column added: `biogas_capacity_increase_extent` (string, nullable)
- column added: `biogas_capacity_increased` (string, nullable)
- column added: `biogas_flexibility_premium_claim_date` (timestamp, nullable)
- column added: `biogas_flexibility_premium_claimed` (string, nullable)
- column added: `biogas_gas_production_capacity` (string, nullable)
- column added: `biogas_maximum_rated_capacity` (string, nullable)
- column added: `biomethane_first_use` (string, nullable)
- column added: `combined_heat_and_power_support_id` (string, nullable)
- column added: `electrical_combined_heat_and_power_capacity_kw` (double, nullable)
- column added: `exclusive_use_of_biomass` (string, nullable)
- column added: `installation_register_identifier` (string, nullable)
- column added: `reference_yield_to_actual_yield_ratio_10_years` (string, nullable)
- column added: `reference_yield_to_actual_yield_ratio_15_years` (string, nullable)
- column added: `reference_yield_to_actual_yield_ratio_5_years` (string, nullable)
- column added: `renewable_energy_act_commissioning_date` (timestamp, nullable)
- column added: `renewable_energy_act_installation_key` (string, nullable)
- column added: `renewable_energy_act_support_id` (string, nullable)
- column added: `retrofit_ids` (string, nullable)
- column added: `yield_estimate_to_reference_yield_ratio` (string, nullable)
- column removed: `AnlagenkennzifferAnlagenregister`
- column removed: `AnlagenschluesselEeg`
- column removed: `AusschliesslicheVerwendungBiomasse`
- column removed: `BiogasDatumInanspruchnahmeFlexiPraemie`
- column removed: `BiogasDatumLeistungserhoehung`
- column removed: `BiogasGaserzeugungskapazitaet`
- column removed: `BiogasHoechstbemessungsleistung`
- column removed: `BiogasInanspruchnahmeFlexiPraemie`
- column removed: `BiogasLeistungserhoehung`
- column removed: `BiogasUmfangLeistungserhoehung`
- column removed: `BiomethanErstmaligerEinsatz`
- column removed: `ErtuechtigungIds`
- column removed: `VerhaeltnisErtragsschaetzungReferenzertrag`
- column removed: `VerhaeltnisReferenzertragErtrag10Jahre`
- column removed: `VerhaeltnisReferenzertragErtrag15Jahre`
- column removed: `VerhaeltnisReferenzertragErtrag5Jahre`
- column removed: `eeg_commissioning_date`
- column removed: `eeg_support_id`
- column removed: `electrical_chp_capacity_kw`
- column removed: `kwk_support_id`

**energy_silver.thermal_energy**
- column added: `heating_combined_heat_and_power_heat_w` (double, nullable)
- column added: `observation_timestamp_native` (string, nullable)
- column added: `observation_timestamp_project` (timestamp, nullable)
- column added: `observation_timestamp_utc` (timestamp, nullable)
- column removed: `heating_chp_heat_w`
- column removed: `observation_ts_native`
- column removed: `observation_ts_project`
- column removed: `observation_ts_utc`

**energy_silver.unit_repowering**
- column added: `renewable_energy_act_support_id` (string, nullable)
- column added: `repowering_id` (string, nullable)
- column removed: `Id`
- column removed: `eeg_support_id`

**energy_silver.weather_cloud**
- column added: `cloud_cover_total_alternate` (array<struct<value:double,native_value:double,native_unit:string,source_dataset:string,quality_code:string>>, nullable)
- column added: `observation_timestamp_native` (string, nullable)
- column added: `observation_timestamp_project` (timestamp, nullable)
- column added: `observation_timestamp_utc` (timestamp, nullable)
- column removed: `cloud_cover_total_alt`
- column removed: `observation_ts_native`
- column removed: `observation_ts_project`
- column removed: `observation_ts_utc`

**energy_silver.weather_daily**
- column added: `observation_count` (int, nullable)
- column removed: `n_observations`

**energy_silver.weather_humidity**
- column added: `observation_timestamp_native` (string, nullable)
- column added: `observation_timestamp_project` (timestamp, nullable)
- column added: `observation_timestamp_utc` (timestamp, nullable)
- column added: `relative_humidity_alternate` (array<struct<value:double,native_value:double,native_unit:string,source_dataset:string,quality_code:string>>, nullable)
- column removed: `observation_ts_native`
- column removed: `observation_ts_project`
- column removed: `observation_ts_utc`
- column removed: `relative_humidity_alt`

**energy_silver.weather_longwave_radiation**
- column added: `observation_timestamp_native` (string, nullable)
- column added: `observation_timestamp_project` (timestamp, nullable)
- column added: `observation_timestamp_utc` (timestamp, nullable)
- column removed: `observation_ts_native`
- column removed: `observation_ts_project`
- column removed: `observation_ts_utc`

**energy_silver.weather_precipitation**
- column added: `observation_timestamp_native` (string, nullable)
- column added: `observation_timestamp_project` (timestamp, nullable)
- column added: `observation_timestamp_utc` (timestamp, nullable)
- column removed: `observation_ts_native`
- column removed: `observation_ts_project`
- column removed: `observation_ts_utc`

**energy_silver.weather_present_weather**
- column added: `observation_timestamp_native` (string, nullable)
- column added: `observation_timestamp_project` (timestamp, nullable)
- column added: `observation_timestamp_utc` (timestamp, nullable)
- column removed: `observation_ts_native`
- column removed: `observation_ts_project`
- column removed: `observation_ts_utc`

**energy_silver.weather_pressure**
- column added: `observation_timestamp_native` (string, nullable)
- column added: `observation_timestamp_project` (timestamp, nullable)
- column added: `observation_timestamp_utc` (timestamp, nullable)
- column added: `pressure_station_alternate` (array<struct<value:double,native_value:double,native_unit:string,source_dataset:string,quality_code:string>>, nullable)
- column removed: `observation_ts_native`
- column removed: `observation_ts_project`
- column removed: `observation_ts_utc`
- column removed: `pressure_station_alt`

**energy_silver.weather_soil_temperature**
- column added: `observation_timestamp_native` (string, nullable)
- column added: `observation_timestamp_project` (timestamp, nullable)
- column added: `observation_timestamp_utc` (timestamp, nullable)
- column removed: `observation_ts_native`
- column removed: `observation_ts_project`
- column removed: `observation_ts_utc`

**energy_silver.weather_solar_geometry**
- column added: `observation_timestamp_native` (string, nullable)
- column added: `observation_timestamp_project` (timestamp, nullable)
- column added: `observation_timestamp_utc` (timestamp, nullable)
- column removed: `observation_ts_native`
- column removed: `observation_ts_project`
- column removed: `observation_ts_utc`

**energy_silver.weather_solar_radiation**
- column added: `observation_timestamp_native` (string, nullable)
- column added: `observation_timestamp_project` (timestamp, nullable)
- column added: `observation_timestamp_utc` (timestamp, nullable)
- column removed: `observation_ts_native`
- column removed: `observation_ts_project`
- column removed: `observation_ts_utc`

**energy_silver.weather_sunshine_duration**
- column added: `observation_timestamp_native` (string, nullable)
- column added: `observation_timestamp_project` (timestamp, nullable)
- column added: `observation_timestamp_utc` (timestamp, nullable)
- column removed: `observation_ts_native`
- column removed: `observation_ts_project`
- column removed: `observation_ts_utc`

**energy_silver.weather_temperature**
- column added: `air_temperature_alternate` (array<struct<value:double,native_value:double,native_unit:string,source_dataset:string,quality_code:string>>, nullable)
- column added: `dew_point_temperature_alternate` (array<struct<value:double,native_value:double,native_unit:string,source_dataset:string,quality_code:string>>, nullable)
- column added: `observation_timestamp_native` (string, nullable)
- column added: `observation_timestamp_project` (timestamp, nullable)
- column added: `observation_timestamp_utc` (timestamp, nullable)
- column removed: `air_temperature_alt`
- column removed: `dew_point_temperature_alt`
- column removed: `observation_ts_native`
- column removed: `observation_ts_project`
- column removed: `observation_ts_utc`

**energy_silver.weather_uv_index**
- column added: `observation_timestamp_native` (string, nullable)
- column added: `observation_timestamp_project` (timestamp, nullable)
- column added: `observation_timestamp_utc` (timestamp, nullable)
- column removed: `observation_ts_native`
- column removed: `observation_ts_project`
- column removed: `observation_ts_utc`

**energy_silver.weather_visibility**
- column added: `observation_timestamp_native` (string, nullable)
- column added: `observation_timestamp_project` (timestamp, nullable)
- column added: `observation_timestamp_utc` (timestamp, nullable)
- column removed: `observation_ts_native`
- column removed: `observation_ts_project`
- column removed: `observation_ts_utc`

**energy_silver.weather_wind**
- column added: `observation_timestamp_native` (string, nullable)
- column added: `observation_timestamp_project` (timestamp, nullable)
- column added: `observation_timestamp_utc` (timestamp, nullable)
- column removed: `observation_ts_native`
- column removed: `observation_ts_project`
- column removed: `observation_ts_utc`

**energy_silver_reference.balancing_area**
- column added: `_source_id_disambiguated` (boolean, nullable)
- column added: `_source_id_ordinal` (int, nullable)
- column added: `area_energy_identification_code` (string, nullable)
- column added: `balancing_area_connection_point` (string, nullable)
- column added: `balancing_area_id` (string, nullable)
- column removed: `BilanzierungsgebietNetzanschlusspunkt`
- column removed: `Id`
- column removed: `Yeic`
- column removed: `_src_id_disambiguated`
- column removed: `_src_id_ord`

**energy_silver_reference.grid_connection_point**
- column added: `balancing_area_connection_point_id` (string, nullable)
- column added: `connection_point_name` (string, nullable)
- column added: `gas_quality` (string, nullable)
- column added: `last_changed_at` (timestamp, nullable)
- column added: `metering_location` (string, nullable)
- column added: `technical_location_name` (string, nullable)
- column removed: `BilanzierungsgebietNetzanschlusspunktId`
- column removed: `Gasqualitaet`
- column removed: `LetzteAenderung`
- column removed: `Messlokation`
- column removed: `NameDerTechnischenLokation`
- column removed: `NetzanschlusspunktBezeichnung`

**energy_silver_reference.grid_location**
- column added: `connection_point_ids` (string, nullable)
- column added: `location_id` (string, nullable)
- column added: `technical_location_name` (string, nullable)
- column removed: `MastrNummer`
- column removed: `NameDerTechnischenLokation`
- column removed: `NetzanschlusspunkteMaStRNummern`

**energy_silver_reference.grid_network**
- column added: `balancing_areas` (string, nullable)
- column added: `grid_id` (string, nullable)
- column added: `market_area` (string, nullable)
- column removed: `Bilanzierungsgebiete`
- column removed: `Marktgebiet`
- column removed: `MastrNummer`

**energy_silver_reference.market_actor**
- column added: `address_supplement` (string, nullable)
- column added: `delivery_address_city` (string, nullable)
- column added: `delivery_address_country` (string, nullable)
- column added: `delivery_address_house_number` (string, nullable)
- column added: `delivery_address_postcode` (string, nullable)
- column added: `delivery_address_street` (string, nullable)
- column added: `delivery_address_supplement` (string, nullable)
- column added: `direct_marketing_company` (string, nullable)
- column added: `electricity_wholesaler` (string, nullable)
- column added: `email` (string, nullable)
- column added: `energy_regulators_agency_code` (string, nullable)
- column added: `european_statistical_region_level_2` (string, nullable)
- column added: `fax` (string, nullable)
- column added: `federal_network_agency_operating_number` (string, nullable)
- column added: `foreign_registry_court` (string, nullable)
- column added: `foreign_registry_number` (string, nullable)
- column added: `gas_wholesaler` (string, nullable)
- column added: `grid` (string, nullable)
- column added: `grid_operator_web_portal` (string, nullable)
- column added: `house_number` (string, nullable)
- column added: `is_small_or_medium_enterprise` (string, nullable)
- column added: `main_economic_sector_division` (string, nullable)
- column added: `main_economic_sector_group` (string, nullable)
- column added: `market_actor_first_name` (string, nullable)
- column added: `market_actor_id` (string, nullable)
- column added: `market_actor_last_name` (string, nullable)
- column added: `market_actor_registration_date` (timestamp, nullable)
- column added: `market_actor_salutation` (string, nullable)
- column added: `other_legal_form` (string, nullable)
- column added: `region` (string, nullable)
- column added: `registry_court` (string, nullable)
- column added: `registry_number` (string, nullable)
- column added: `registry_number_prefix` (string, nullable)
- column added: `street` (string, nullable)
- column added: `supplies_end_consumers_electricity` (string, nullable)
- column added: `supplies_end_consumers_gas` (string, nullable)
- column added: `supplies_household_customers_electricity` (string, nullable)
- column added: `supplies_household_customers_gas` (string, nullable)
- column added: `telephone` (string, nullable)
- column added: `value_added_tax_identification_number` (string, nullable)
- column added: `website` (string, nullable)
- column removed: `AcerCode`
- column removed: `Adresszusatz`
- column removed: `AdresszusatzAnZustelladresse`
- column removed: `BelieferungHaushaltskundenGas`
- column removed: `BelieferungHaushaltskundenStrom`
- column removed: `BelieferungVonLetztverbrauchernGas`
- column removed: `BelieferungVonLetztverbrauchernStrom`
- column removed: `BundesnetzagenturBetriebsnummer`
- column removed: `Direktvermarktungsunternehmen`
- column removed: `Email`
- column removed: `Fax`
- column removed: `Gasgrosshaendler`
- column removed: `HauptwirtdschaftszweigAbteilung`
- column removed: `HauptwirtdschaftszweigGruppe`
- column removed: `Hausnummer`
- column removed: `HausnummerAnZustelladresse`
- column removed: `LandAnZustelladresse`
- column removed: `MarktakteurAnrede`
- column removed: `MarktakteurNachname`
- column removed: `MarktakteurVorname`
- column removed: `MastrNummer`
- column removed: `Netz`
- column removed: `OrtAnZustelladresse`
- column removed: `PostleitzahlAnZustelladresse`
- column removed: `Region`
- column removed: `Registergericht`
- column removed: `RegistergerichtAusland`
- column removed: `Registernummer`
- column removed: `RegisternummerAusland`
- column removed: `RegisternummerPraefix`
- column removed: `RegistrierungsdatumMarktakteur`
- column removed: `SonstigeRechtsform`
- column removed: `Strasse`
- column removed: `StrasseAnZustelladresse`
- column removed: `Stromgrosshaendler`
- column removed: `Telefon`
- column removed: `Umsatzsteueridentifikationsnummer`
- column removed: `WebportalDesNetzbetreibers`
- column removed: `Webseite`
- column removed: `is_sme`
- column removed: `nuts2_region`

**energy_silver_reference.market_actor_role**
- column added: `federal_network_agency_operating_number` (string, nullable)
- column added: `market_actor_role_id` (string, nullable)
- column added: `market_partner_identification_number` (string, nullable)
- column added: `market_role_contact_details` (string, nullable)
- column removed: `BundesnetzagenturBetriebsnummer`
- column removed: `KontaktdatenMarktrolle`
- column removed: `Marktpartneridentifikationsnummer`
- column removed: `MastrNummer`

**energy_silver_reference.weather_location**
- column added: `official_municipality_key` (string, nullable)
- column removed: `ags_code`

**energy_silver_reference.weather_missing_value_period**
- column added: `gap_end_timestamp` (timestamp, nullable)
- column added: `gap_start_timestamp` (timestamp, nullable)
- column removed: `gap_end_ts`
- column removed: `gap_start_ts`

## Schema

| schema | table | # | column | type | nullable |
|---|---|---|---|---|---|
| commerce_silver | ga4_event | 1 | event_key | string | true |
| commerce_silver | ga4_event | 2 | event_date_native | string | true |
| commerce_silver | ga4_event | 3 | event_timestamp_native | string | true |
| commerce_silver | ga4_event | 4 | event_timestamp_utc | timestamp | true |
| commerce_silver | ga4_event | 5 | event_timestamp_project | timestamp | true |
| commerce_silver | ga4_event | 6 | local_date | date | true |
| commerce_silver | ga4_event | 7 | event_name | string | true |
| commerce_silver | ga4_event | 8 | user_pseudo_id | string | true |
| commerce_silver | ga4_event | 9 | session_id | bigint | true |
| commerce_silver | ga4_event | 10 | session_number | bigint | true |
| commerce_silver | ga4_event | 11 | page_location | string | true |
| commerce_silver | ga4_event | 12 | page_title | string | true |
| commerce_silver | ga4_event | 13 | search_term | string | true |
| commerce_silver | ga4_event | 14 | unique_search_term | string | true |
| commerce_silver | ga4_event | 15 | geo_country | string | true |
| commerce_silver | ga4_event | 16 | transaction_id | string | true |
| commerce_silver | ga4_event | 17 | purchase_revenue | double | true |
| commerce_silver | ga4_event | 18 | unique_items | bigint | true |
| commerce_silver | ga4_event | 19 | total_item_quantity | bigint | true |
| commerce_silver | ga4_event | 20 | quality_flags | array<string> | true |
| commerce_silver | ga4_event | 21 | measurement_basis | string | true |
| commerce_silver | ga4_event | 22 | source_system | string | true |
| commerce_silver | ga4_event | 23 | source_dataset | string | true |
| commerce_silver | ga4_event | 24 | source_record_id | string | true |
| commerce_silver | ga4_event | 25 | _silver_loaded_at | timestamp | true |
| commerce_silver | ga4_event | 26 | _silver_run_id | string | true |
| commerce_silver | ga4_event_item | 1 | event_key | string | true |
| commerce_silver | ga4_event_item | 2 | item_ordinal | int | true |
| commerce_silver | ga4_event_item | 3 | event_name | string | true |
| commerce_silver | ga4_event_item | 4 | event_timestamp_utc | timestamp | true |
| commerce_silver | ga4_event_item | 5 | user_pseudo_id | string | true |
| commerce_silver | ga4_event_item | 6 | item_context | string | true |
| commerce_silver | ga4_event_item | 7 | item_id | string | true |
| commerce_silver | ga4_event_item | 8 | item_name | string | true |
| commerce_silver | ga4_event_item | 9 | item_category | string | true |
| commerce_silver | ga4_event_item | 10 | price | double | true |
| commerce_silver | ga4_event_item | 11 | quantity | bigint | true |
| commerce_silver | ga4_event_item | 12 | item_revenue | double | true |
| commerce_silver | ga4_event_item | 13 | source_system | string | true |
| commerce_silver | ga4_event_item | 14 | source_dataset | string | true |
| commerce_silver | ga4_event_item | 15 | source_record_id | string | true |
| commerce_silver | ga4_event_item | 16 | _silver_loaded_at | timestamp | true |
| commerce_silver | ga4_event_item | 17 | _silver_run_id | string | true |
| commerce_silver | rees46_event | 1 | event_key | string | true |
| commerce_silver | rees46_event | 2 | event_timestamp_native | string | true |
| commerce_silver | rees46_event | 3 | event_timestamp_utc | timestamp | true |
| commerce_silver | rees46_event | 4 | event_timestamp_project | timestamp | true |
| commerce_silver | rees46_event | 5 | local_date | date | true |
| commerce_silver | rees46_event | 6 | event_type | string | true |
| commerce_silver | rees46_event | 7 | user_id | string | true |
| commerce_silver | rees46_event | 8 | user_session | string | true |
| commerce_silver | rees46_event | 9 | product_id | string | true |
| commerce_silver | rees46_event | 10 | category_id | string | true |
| commerce_silver | rees46_event | 11 | category_code | string | true |
| commerce_silver | rees46_event | 12 | category_l1 | string | true |
| commerce_silver | rees46_event | 13 | category_l2 | string | true |
| commerce_silver | rees46_event | 14 | category_l3 | string | true |
| commerce_silver | rees46_event | 15 | brand | string | true |
| commerce_silver | rees46_event | 16 | price | double | true |
| commerce_silver | rees46_event | 17 | currency_unknown | boolean | true |
| commerce_silver | rees46_event | 18 | quality_flags | array<string> | true |
| commerce_silver | rees46_event | 19 | measurement_basis | string | true |
| commerce_silver | rees46_event | 20 | source_system | string | true |
| commerce_silver | rees46_event | 21 | source_dataset | string | true |
| commerce_silver | rees46_event | 22 | source_record_id | string | true |
| commerce_silver | rees46_event | 23 | _silver_loaded_at | timestamp | true |
| commerce_silver | rees46_event | 24 | _silver_run_id | string | true |
| energy_silver | actor_deletion_event | 1 | market_actor_id | string | true |
| energy_silver | actor_deletion_event | 2 | last_updated_at | timestamp | true |
| energy_silver | actor_deletion_event | 3 | actor_status_code | string | true |
| energy_silver | actor_deletion_event | 4 | actor_status_label_de | string | true |
| energy_silver | actor_deletion_event | 5 | actor_status | string | true |
| energy_silver | actor_deletion_event | 6 | _unmatched_code_columns | string | true |
| energy_silver | actor_deletion_event | 7 | source_record_id | string | true |
| energy_silver | actor_deletion_event | 8 | source_dataset | string | true |
| energy_silver | actor_deletion_event | 9 | source_system | string | true |
| energy_silver | actor_deletion_event | 10 | _silver_loaded_at | timestamp | true |
| energy_silver | actor_deletion_event | 11 | _silver_run_id | string | true |
| energy_silver | device_telemetry_snapshot | 1 | device_key | string | true |
| energy_silver | device_telemetry_snapshot | 2 | source_device_id | string | true |
| energy_silver | device_telemetry_snapshot | 3 | device_name | string | true |
| energy_silver | device_telemetry_snapshot | 4 | ip_address | string | true |
| energy_silver | device_telemetry_snapshot | 5 | latitude | double | true |
| energy_silver | device_telemetry_snapshot | 6 | longitude | double | true |
| energy_silver | device_telemetry_snapshot | 7 | country_code | string | true |
| energy_silver | device_telemetry_snapshot | 8 | country_code_alpha3 | string | true |
| energy_silver | device_telemetry_snapshot | 9 | country_name | string | true |
| energy_silver | device_telemetry_snapshot | 10 | observation_timestamp_native | string | true |
| energy_silver | device_telemetry_snapshot | 11 | observation_timestamp_utc | timestamp | true |
| energy_silver | device_telemetry_snapshot | 12 | temperature_degc | double | true |
| energy_silver | device_telemetry_snapshot | 13 | humidity_percent | double | true |
| energy_silver | device_telemetry_snapshot | 14 | co2_level | double | true |
| energy_silver | device_telemetry_snapshot | 15 | battery_level | double | true |
| energy_silver | device_telemetry_snapshot | 16 | lcd_label | string | true |
| energy_silver | device_telemetry_snapshot | 17 | data_origin | string | true |
| energy_silver | device_telemetry_snapshot | 18 | measurement_basis | string | true |
| energy_silver | device_telemetry_snapshot | 19 | source_system | string | true |
| energy_silver | device_telemetry_snapshot | 20 | source_dataset | string | true |
| energy_silver | device_telemetry_snapshot | 21 | source_record_id | string | true |
| energy_silver | device_telemetry_snapshot | 22 | _silver_loaded_at | timestamp | true |
| energy_silver | device_telemetry_snapshot | 23 | _silver_run_id | string | true |
| energy_silver | electricity_balance | 1 | observation_key | string | true |
| energy_silver | electricity_balance | 2 | location_key | string | true |
| energy_silver | electricity_balance | 3 | source_location_id | string | true |
| energy_silver | electricity_balance | 4 | market_area_code | string | true |
| energy_silver | electricity_balance | 5 | observation_timestamp_native | string | true |
| energy_silver | electricity_balance | 6 | time_basis | string | true |
| energy_silver | electricity_balance | 7 | utc_offset_hours | double | true |
| energy_silver | electricity_balance | 8 | observation_timestamp_utc | timestamp | true |
| energy_silver | electricity_balance | 9 | observation_timestamp_project | timestamp | true |
| energy_silver | electricity_balance | 10 | local_date | date | true |
| energy_silver | electricity_balance | 11 | interval_seconds | int | true |
| energy_silver | electricity_balance | 12 | interval_reference | string | true |
| energy_silver | electricity_balance | 13 | components | array<struct<component_kind:string,carrier_code:string,native_label:string,energy_mwh:double,power_w:double,source_series:string>> | true |
| energy_silver | electricity_balance | 14 | sign_convention | string | true |
| energy_silver | electricity_balance | 15 | quality_flags | array<string> | true |
| energy_silver | electricity_balance | 16 | measurement_basis | string | true |
| energy_silver | electricity_balance | 17 | source_system | string | true |
| energy_silver | electricity_balance | 18 | source_dataset | string | true |
| energy_silver | electricity_balance | 19 | source_record_id | string | true |
| energy_silver | electricity_balance | 20 | _silver_loaded_at | timestamp | true |
| energy_silver | electricity_balance | 21 | _silver_run_id | string | true |
| energy_silver | electricity_generation_forecast | 1 | observation_key | string | true |
| energy_silver | electricity_generation_forecast | 2 | location_key | string | true |
| energy_silver | electricity_generation_forecast | 3 | source_location_id | string | true |
| energy_silver | electricity_generation_forecast | 4 | market_area_code | string | true |
| energy_silver | electricity_generation_forecast | 5 | observation_timestamp_native | string | true |
| energy_silver | electricity_generation_forecast | 6 | time_basis | string | true |
| energy_silver | electricity_generation_forecast | 7 | utc_offset_hours | double | true |
| energy_silver | electricity_generation_forecast | 8 | observation_timestamp_utc | timestamp | true |
| energy_silver | electricity_generation_forecast | 9 | observation_timestamp_project | timestamp | true |
| energy_silver | electricity_generation_forecast | 10 | local_date | date | true |
| energy_silver | electricity_generation_forecast | 11 | interval_seconds | int | true |
| energy_silver | electricity_generation_forecast | 12 | interval_reference | string | true |
| energy_silver | electricity_generation_forecast | 13 | forecast_issue_timestamp | timestamp | true |
| energy_silver | electricity_generation_forecast | 14 | components | array<struct<forecast_scope:string,native_label:string,energy_mwh:double,semantic_status:string,semantic_issue_ref:string,source_series:string>> | true |
| energy_silver | electricity_generation_forecast | 15 | sign_convention | string | true |
| energy_silver | electricity_generation_forecast | 16 | quality_flags | array<string> | true |
| energy_silver | electricity_generation_forecast | 17 | measurement_basis | string | true |
| energy_silver | electricity_generation_forecast | 18 | source_system | string | true |
| energy_silver | electricity_generation_forecast | 19 | source_dataset | string | true |
| energy_silver | electricity_generation_forecast | 20 | source_record_id | string | true |
| energy_silver | electricity_generation_forecast | 21 | _silver_loaded_at | timestamp | true |
| energy_silver | electricity_generation_forecast | 22 | _silver_run_id | string | true |
| energy_silver | electricity_price | 1 | observation_key | string | true |
| energy_silver | electricity_price | 2 | location_key | string | true |
| energy_silver | electricity_price | 3 | source_location_id | string | true |
| energy_silver | electricity_price | 4 | market_area_code | string | true |
| energy_silver | electricity_price | 5 | observation_timestamp_native | string | true |
| energy_silver | electricity_price | 6 | time_basis | string | true |
| energy_silver | electricity_price | 7 | utc_offset_hours | double | true |
| energy_silver | electricity_price | 8 | observation_timestamp_utc | timestamp | true |
| energy_silver | electricity_price | 9 | observation_timestamp_project | timestamp | true |
| energy_silver | electricity_price | 10 | local_date | date | true |
| energy_silver | electricity_price | 11 | interval_seconds | int | true |
| energy_silver | electricity_price | 12 | interval_reference | string | true |
| energy_silver | electricity_price | 13 | price_eur_per_mwh | double | true |
| energy_silver | electricity_price | 14 | sign_convention | string | true |
| energy_silver | electricity_price | 15 | quality_flags | array<string> | true |
| energy_silver | electricity_price | 16 | measurement_basis | string | true |
| energy_silver | electricity_price | 17 | source_system | string | true |
| energy_silver | electricity_price | 18 | source_dataset | string | true |
| energy_silver | electricity_price | 19 | source_record_id | string | true |
| energy_silver | electricity_price | 20 | _silver_loaded_at | timestamp | true |
| energy_silver | electricity_price | 21 | _silver_run_id | string | true |
| energy_silver | energy_meter_reading | 1 | observation_key | string | true |
| energy_silver | energy_meter_reading | 2 | location_key | string | true |
| energy_silver | energy_meter_reading | 3 | source_location_id | string | true |
| energy_silver | energy_meter_reading | 4 | market_area_code | string | true |
| energy_silver | energy_meter_reading | 5 | observation_timestamp_native | string | true |
| energy_silver | energy_meter_reading | 6 | time_basis | string | true |
| energy_silver | energy_meter_reading | 7 | utc_offset_hours | double | true |
| energy_silver | energy_meter_reading | 8 | observation_timestamp_utc | timestamp | true |
| energy_silver | energy_meter_reading | 9 | observation_timestamp_project | timestamp | true |
| energy_silver | energy_meter_reading | 10 | local_date | date | true |
| energy_silver | energy_meter_reading | 11 | interval_seconds | int | true |
| energy_silver | energy_meter_reading | 12 | interval_reference | string | true |
| energy_silver | energy_meter_reading | 13 | electricity_total_kwh | double | true |
| energy_silver | energy_meter_reading | 14 | electricity_total_increment_kwh | double | true |
| energy_silver | energy_meter_reading | 15 | electricity_solar_photovoltaic_kwh | double | true |
| energy_silver | energy_meter_reading | 16 | electricity_solar_photovoltaic_increment_kwh | double | true |
| energy_silver | energy_meter_reading | 17 | electricity_combined_heat_and_power_kwh | double | true |
| energy_silver | energy_meter_reading | 18 | electricity_combined_heat_and_power_increment_kwh | double | true |
| energy_silver | energy_meter_reading | 19 | heating_total_kwh | double | true |
| energy_silver | energy_meter_reading | 20 | heating_total_increment_kwh | double | true |
| energy_silver | energy_meter_reading | 21 | heating_combined_heat_and_power_heat_kwh | double | true |
| energy_silver | energy_meter_reading | 22 | heating_combined_heat_and_power_heat_increment_kwh | double | true |
| energy_silver | energy_meter_reading | 23 | heating_combined_heat_and_power_electricity_kwh | double | true |
| energy_silver | energy_meter_reading | 24 | heating_combined_heat_and_power_electricity_increment_kwh | double | true |
| energy_silver | energy_meter_reading | 25 | cooling_total_kwh | double | true |
| energy_silver | energy_meter_reading | 26 | cooling_total_increment_kwh | double | true |
| energy_silver | energy_meter_reading | 27 | cooling_electricity_kwh | double | true |
| energy_silver | energy_meter_reading | 28 | cooling_electricity_increment_kwh | double | true |
| energy_silver | energy_meter_reading | 29 | increment_derivation_rule | string | true |
| energy_silver | energy_meter_reading | 30 | sign_convention | string | true |
| energy_silver | energy_meter_reading | 31 | quality_flags | array<string> | true |
| energy_silver | energy_meter_reading | 32 | measurement_basis | string | true |
| energy_silver | energy_meter_reading | 33 | source_system | string | true |
| energy_silver | energy_meter_reading | 34 | source_dataset | string | true |
| energy_silver | energy_meter_reading | 35 | source_record_id | string | true |
| energy_silver | energy_meter_reading | 36 | _silver_loaded_at | timestamp | true |
| energy_silver | energy_meter_reading | 37 | _silver_run_id | string | true |
| energy_silver | generation_unit | 1 | unit_id | string | true |
| energy_silver | generation_unit | 2 | last_updated_at | timestamp | true |
| energy_silver | generation_unit | 3 | location_id | string | true |
| energy_silver | generation_unit | 4 | grid_operator_check_status | string | true |
| energy_silver | generation_unit | 5 | grid_operator_check_date | timestamp | true |
| energy_silver | generation_unit | 6 | operator_id | string | true |
| energy_silver | generation_unit | 7 | district | string | true |
| energy_silver | generation_unit | 8 | municipality | string | true |
| energy_silver | generation_unit | 9 | source_municipality_key | string | true |
| energy_silver | generation_unit | 10 | postcode | string | true |
| energy_silver | generation_unit | 11 | cadastral_district | string | true |
| energy_silver | generation_unit | 12 | land_parcel_numbers | string | true |
| energy_silver | generation_unit | 13 | street_not_found | string | true |
| energy_silver | generation_unit | 14 | Hausnummer_nv | string | true |
| energy_silver | generation_unit | 15 | house_number_not_found | string | true |
| energy_silver | generation_unit | 16 | locality | string | true |
| energy_silver | generation_unit | 17 | longitude | double | true |
| energy_silver | generation_unit | 18 | latitude | double | true |
| energy_silver | generation_unit | 19 | registration_date | timestamp | true |
| energy_silver | generation_unit | 20 | commissioning_date | timestamp | true |
| energy_silver | generation_unit | 21 | migration_absence_flag | string | true |
| energy_silver | generation_unit | 22 | unit_name | string | true |
| energy_silver | generation_unit | 23 | Weic_nv | string | true |
| energy_silver | generation_unit | 24 | Kraftwerksnummer_nv | string | true |
| energy_silver | generation_unit | 25 | capacity_gross_kw | double | true |
| energy_silver | generation_unit | 26 | capacity_net_kw | double | true |
| energy_silver | generation_unit | 27 | remote_control_by_grid_operator | boolean | true |
| energy_silver | generation_unit | 28 | remote_control_by_direct_marketer | boolean | true |
| energy_silver | generation_unit | 29 | authorisation_id | string | true |
| energy_silver | generation_unit | 30 | wind_farm_name | string | true |
| energy_silver | generation_unit | 31 | manufacturer | string | true |
| energy_silver | generation_unit | 32 | model_designation | string | true |
| energy_silver | generation_unit | 33 | hub_height_m | double | true |
| energy_silver | generation_unit | 34 | rotor_diameter_m | double | true |
| energy_silver | generation_unit | 35 | rotor_blade_deicing_system | string | true |
| energy_silver | generation_unit | 36 | curtailment_condition_power_limit | string | true |
| energy_silver | generation_unit | 37 | curtailment_condition_noise_protection_night | string | true |
| energy_silver | generation_unit | 38 | curtailment_condition_noise_protection_day | string | true |
| energy_silver | generation_unit | 39 | curtailment_condition_shadow_flicker | string | true |
| energy_silver | generation_unit | 40 | curtailment_condition_animal_protection | string | true |
| energy_silver | generation_unit | 41 | curtailment_condition_ice_throw | string | true |
| energy_silver | generation_unit | 42 | curtailment_condition_other | string | true |
| energy_silver | generation_unit | 43 | night_marking | string | true |
| energy_silver | generation_unit | 44 | citizen_energy | string | true |
| energy_silver | generation_unit | 45 | renewable_energy_act_support_id | string | true |
| energy_silver | generation_unit | 46 | connected_to_extra_high_or_high_voltage | string | true |
| energy_silver | generation_unit | 47 | street | string | true |
| energy_silver | generation_unit | 48 | house_number | string | true |
| energy_silver | generation_unit | 49 | land_area_used | string | true |
| energy_silver | generation_unit | 50 | predominant_land_use_before_construction | string | true |
| energy_silver | generation_unit | 51 | previous_land_use_category | string | true |
| energy_silver | generation_unit | 52 | planned_commissioning_date | timestamp | true |
| energy_silver | generation_unit | 53 | address_supplement | string | true |
| energy_silver | generation_unit | 54 | previous_operator_id | string | true |
| energy_silver | generation_unit | 55 | operator_change_date | timestamp | true |
| energy_silver | generation_unit | 56 | operator_change_registration_date | timestamp | true |
| energy_silver | generation_unit | 57 | final_decommissioning_date | timestamp | true |
| energy_silver | generation_unit | 58 | provisional_shutdown_start_date | timestamp | true |
| energy_silver | generation_unit | 59 | resource_energy_identification_code | string | true |
| energy_silver | generation_unit | 60 | resource_energy_identification_code_display_name | string | true |
| energy_silver | generation_unit | 61 | north_sea_area_development_plan_zone | string | true |
| energy_silver | generation_unit | 62 | water_depth_m | double | true |
| energy_silver | generation_unit | 63 | distance_to_coast_km | double | true |
| energy_silver | generation_unit | 64 | baltic_sea_area_development_plan_zone | string | true |
| energy_silver | generation_unit | 65 | recommissioning_date | timestamp | true |
| energy_silver | generation_unit | 66 | operationally_responsible_party | string | true |
| energy_silver | generation_unit | 67 | federal_state_code | string | true |
| energy_silver | generation_unit | 68 | federal_state_label_de | string | true |
| energy_silver | generation_unit | 69 | federal_state | string | true |
| energy_silver | generation_unit | 70 | wind_onshore_or_offshore_code | string | true |
| energy_silver | generation_unit | 71 | wind_onshore_or_offshore_label_de | string | true |
| energy_silver | generation_unit | 72 | wind_onshore_or_offshore | string | true |
| energy_silver | generation_unit | 73 | operating_status_code | string | true |
| energy_silver | generation_unit | 74 | operating_status_label_de | string | true |
| energy_silver | generation_unit | 75 | operating_status | string | true |
| energy_silver | generation_unit | 76 | system_status_code | string | true |
| energy_silver | generation_unit | 77 | system_status_label_de | string | true |
| energy_silver | generation_unit | 78 | system_status | string | true |
| energy_silver | generation_unit | 79 | feed_in_type_code | string | true |
| energy_silver | generation_unit | 80 | feed_in_type_label_de | string | true |
| energy_silver | generation_unit | 81 | feed_in_type | string | true |
| energy_silver | generation_unit | 82 | generation_technology_code | string | true |
| energy_silver | generation_unit | 83 | generation_technology_label_de | string | true |
| energy_silver | generation_unit | 84 | generation_technology | string | true |
| energy_silver | generation_unit | 85 | offshore_sea_area_code | string | true |
| energy_silver | generation_unit | 86 | offshore_sea_area_label_de | string | true |
| energy_silver | generation_unit | 87 | offshore_sea_area | string | true |
| energy_silver | generation_unit | 88 | _unmatched_code_columns | string | true |
| energy_silver | generation_unit | 89 | official_municipality_key | string | true |
| energy_silver | generation_unit | 90 | official_municipality_key_level | string | true |
| energy_silver | generation_unit | 91 | official_municipality_key_method | string | true |
| energy_silver | generation_unit | 92 | unit_type | string | true |
| energy_silver | generation_unit | 93 | source_dataset | string | true |
| energy_silver | generation_unit | 94 | biomass_type | string | true |
| energy_silver | generation_unit | 95 | combined_heat_and_power_support_id | string | true |
| energy_silver | generation_unit | 96 | assigned_to_grid_reserve | string | true |
| energy_silver | generation_unit | 97 | assigned_to_capacity_reserve | string | true |
| energy_silver | generation_unit | 98 | power_plant_number | string | true |
| energy_silver | generation_unit | 99 | commissioning_date_current_site | timestamp | true |
| energy_silver | generation_unit | 100 | main_fuel_code | string | true |
| energy_silver | generation_unit | 101 | main_fuel_label_de | string | true |
| energy_silver | generation_unit | 102 | main_fuel | string | true |
| energy_silver | generation_unit | 103 | plant_name | string | true |
| energy_silver | generation_unit | 104 | electricity_generation_reduction | string | true |
| energy_silver | generation_unit | 105 | hydro_inflow_type | string | true |
| energy_silver | generation_unit | 106 | part_of_cross_border_plant | string | true |
| energy_silver | generation_unit | 107 | capacity_net_germany_kw | double | true |
| energy_silver | generation_unit | 108 | hydro_plant_type_code | string | true |
| energy_silver | generation_unit | 109 | hydro_plant_type_label_de | string | true |
| energy_silver | generation_unit | 110 | hydro_plant_type | string | true |
| energy_silver | generation_unit | 111 | plant_block_name | string | true |
| energy_silver | generation_unit | 112 | unit_is_in_combined_operation | string | true |
| energy_silver | generation_unit | 113 | emergency_generator | string | true |
| energy_silver | generation_unit | 114 | combined_operation_net_rated_capacity_increase | string | true |
| energy_silver | generation_unit | 115 | combined_operation_unit_ids | string | true |
| energy_silver | generation_unit | 116 | additional_fuels | string | true |
| energy_silver | generation_unit | 117 | grid_reserve_date | timestamp | true |
| energy_silver | generation_unit | 118 | exclusive_use_in_combined_operation | string | true |
| energy_silver | generation_unit | 119 | security_standby_start_date | timestamp | true |
| energy_silver | generation_unit | 120 | capacity_reserve_date | timestamp | true |
| energy_silver | generation_unit | 121 | construction_start_date | timestamp | true |
| energy_silver | generation_unit | 122 | additional_main_fuel_code | string | true |
| energy_silver | generation_unit | 123 | additional_main_fuel_label_de | string | true |
| energy_silver | generation_unit | 124 | additional_main_fuel | string | true |
| energy_silver | generation_unit | 125 | deployment_location_code | string | true |
| energy_silver | generation_unit | 126 | deployment_location_label_de | string | true |
| energy_silver | generation_unit | 127 | deployment_location | string | true |
| energy_silver | generation_unit | 128 | source_record_id | string | true |
| energy_silver | generation_unit | 129 | source_system | string | true |
| energy_silver | generation_unit | 130 | _silver_loaded_at | timestamp | true |
| energy_silver | generation_unit | 131 | _silver_run_id | string | true |
| energy_silver | grid_intervention_event | 1 | event_key | string | true |
| energy_silver | grid_intervention_event | 2 | event_start_native | string | true |
| energy_silver | grid_intervention_event | 3 | event_end_native | string | true |
| energy_silver | grid_intervention_event | 4 | time_basis | string | true |
| energy_silver | grid_intervention_event | 5 | event_start_utc | timestamp | true |
| energy_silver | grid_intervention_event | 6 | event_end_utc | timestamp | true |
| energy_silver | grid_intervention_event | 7 | event_start_project | timestamp | true |
| energy_silver | grid_intervention_event | 8 | event_end_project | timestamp | true |
| energy_silver | grid_intervention_event | 9 | duration_hours | double | true |
| energy_silver | grid_intervention_event | 10 | reason_native | string | true |
| energy_silver | grid_intervention_event | 11 | reason | string | true |
| energy_silver | grid_intervention_event | 12 | direction_native | string | true |
| energy_silver | grid_intervention_event | 13 | direction | string | true |
| energy_silver | grid_intervention_event | 14 | mean_power_mw | double | true |
| energy_silver | grid_intervention_event | 15 | max_power_mw | double | true |
| energy_silver | grid_intervention_event | 16 | energy_mwh | double | true |
| energy_silver | grid_intervention_event | 17 | instructing_transmission_system_operator_native | string | true |
| energy_silver | grid_intervention_event | 18 | instructing_market_area_code | string | true |
| energy_silver | grid_intervention_event | 19 | requesting_transmission_system_operator_native | string | true |
| energy_silver | grid_intervention_event | 20 | requesting_market_area_codes | array<string> | true |
| energy_silver | grid_intervention_event | 21 | affected_asset_text | string | true |
| energy_silver | grid_intervention_event | 22 | affected_unit_match_name | string | true |
| energy_silver | grid_intervention_event | 23 | affected_unit_match_confidence | string | true |
| energy_silver | grid_intervention_event | 24 | primary_energy_type_native | string | true |
| energy_silver | grid_intervention_event | 25 | primary_energy_type | string | true |
| energy_silver | grid_intervention_event | 26 | quality_flags | array<string> | true |
| energy_silver | grid_intervention_event | 27 | measurement_basis | string | true |
| energy_silver | grid_intervention_event | 28 | source_system | string | true |
| energy_silver | grid_intervention_event | 29 | source_dataset | string | true |
| energy_silver | grid_intervention_event | 30 | source_record_id | string | true |
| energy_silver | grid_intervention_event | 31 | _silver_loaded_at | timestamp | true |
| energy_silver | grid_intervention_event | 32 | _silver_run_id | string | true |
| energy_silver | grid_location_coordinate_conflict | 1 | location_id | string | true |
| energy_silver | grid_location_coordinate_conflict | 2 | distinct_coords | bigint | true |
| energy_silver | grid_location_coordinate_conflict | 3 | _coordinate_conflict | boolean | true |
| energy_silver | grid_location_coordinate_conflict | 4 | source_record_id | string | true |
| energy_silver | grid_location_coordinate_conflict | 5 | source_dataset | string | true |
| energy_silver | grid_location_coordinate_conflict | 6 | source_system | string | true |
| energy_silver | grid_location_coordinate_conflict | 7 | _silver_loaded_at | timestamp | true |
| energy_silver | grid_location_coordinate_conflict | 8 | _silver_run_id | string | true |
| energy_silver | grid_operator_change_event | 1 | unit_id | string | true |
| energy_silver | grid_operator_change_event | 2 | location_id | string | true |
| energy_silver | grid_operator_change_event | 3 | previous_grid_operator_id | string | true |
| energy_silver | grid_operator_change_event | 4 | new_grid_operator_id | string | true |
| energy_silver | grid_operator_change_event | 5 | change_type | string | true |
| energy_silver | grid_operator_change_event | 6 | grid_operator_change_registered_date | timestamp | true |
| energy_silver | grid_operator_change_event | 7 | grid_operator_change_effective_date | timestamp | true |
| energy_silver | grid_operator_change_event | 8 | connection_point_id | string | true |
| energy_silver | grid_operator_change_event | 9 | _unmatched_code_columns | string | true |
| energy_silver | grid_operator_change_event | 10 | _source_id_ordinal | int | true |
| energy_silver | grid_operator_change_event | 11 | _source_id_disambiguated | boolean | true |
| energy_silver | grid_operator_change_event | 12 | _date_order_violation_registered_before_effective | boolean | true |
| energy_silver | grid_operator_change_event | 13 | _date_order_violation_commissioning_after_change | boolean | true |
| energy_silver | grid_operator_change_event | 14 | source_record_id | string | true |
| energy_silver | grid_operator_change_event | 15 | source_dataset | string | true |
| energy_silver | grid_operator_change_event | 16 | source_system | string | true |
| energy_silver | grid_operator_change_event | 17 | _silver_loaded_at | timestamp | true |
| energy_silver | grid_operator_change_event | 18 | _silver_run_id | string | true |
| energy_silver | plant_operating_sample | 1 | sample_key | string | true |
| energy_silver | plant_operating_sample | 2 | ambient_temperature_degc | double | true |
| energy_silver | plant_operating_sample | 3 | exhaust_vacuum_cm_of_mercury | double | true |
| energy_silver | plant_operating_sample | 4 | ambient_pressure_mbar | double | true |
| energy_silver | plant_operating_sample | 5 | relative_humidity_percent | double | true |
| energy_silver | plant_operating_sample | 6 | net_electrical_output_mw | double | true |
| energy_silver | plant_operating_sample | 7 | repeat_index | int | true |
| energy_silver | plant_operating_sample | 8 | quality_flags | array<string> | true |
| energy_silver | plant_operating_sample | 9 | measurement_basis | string | true |
| energy_silver | plant_operating_sample | 10 | source_system | string | true |
| energy_silver | plant_operating_sample | 11 | source_dataset | string | true |
| energy_silver | plant_operating_sample | 12 | source_record_id | string | true |
| energy_silver | plant_operating_sample | 13 | _silver_loaded_at | timestamp | true |
| energy_silver | plant_operating_sample | 14 | _silver_run_id | string | true |
| energy_silver | power_plant_capacity_plan | 1 | energy_carrier | string | true |
| energy_silver | power_plant_capacity_plan | 2 | 2026 | double | true |
| energy_silver | power_plant_capacity_plan | 3 | 2027 | double | true |
| energy_silver | power_plant_capacity_plan | 4 | 2028 | double | true |
| energy_silver | power_plant_capacity_plan | 5 | 2029 | double | true |
| energy_silver | power_plant_capacity_plan | 6 | retiring_capacity_total_2026_2029_mw | double | true |
| energy_silver | power_plant_capacity_plan | 7 | capacity_section | string | true |
| energy_silver | power_plant_capacity_plan | 8 | is_total | boolean | true |
| energy_silver | power_plant_capacity_plan | 9 | _source_id_ordinal | int | true |
| energy_silver | power_plant_capacity_plan | 10 | _source_id_disambiguated | boolean | true |
| energy_silver | power_plant_capacity_plan | 11 | source_record_id | string | true |
| energy_silver | power_plant_capacity_plan | 12 | source_dataset | string | true |
| energy_silver | power_plant_capacity_plan | 13 | source_system | string | true |
| energy_silver | power_plant_capacity_plan | 14 | _silver_loaded_at | timestamp | true |
| energy_silver | power_plant_capacity_plan | 15 | _silver_run_id | string | true |
| energy_silver | power_plant_register | 1 | mastr_unit_id | string | true |
| energy_silver | power_plant_register | 2 | operator_name | string | true |
| energy_silver | power_plant_register | 3 | plant_name | string | true |
| energy_silver | power_plant_register | 4 | postcode | string | true |
| energy_silver | power_plant_register | 5 | municipality | string | true |
| energy_silver | power_plant_register | 6 | street | string | true |
| energy_silver | power_plant_register | 7 | house_number | string | true |
| energy_silver | power_plant_register | 8 | federal_state | string | true |
| energy_silver | power_plant_register | 9 | country | string | true |
| energy_silver | power_plant_register | 10 | commissioning_year | int | true |
| energy_silver | power_plant_register | 11 | decommissioning_year | int | true |
| energy_silver | power_plant_register | 12 | main_fuel | string | true |
| energy_silver | power_plant_register | 13 | is_combined_heat_and_power | boolean | true |
| energy_silver | power_plant_register | 14 | is_renewable_carrier | boolean | true |
| energy_silver | power_plant_register | 15 | capacity_gross_mw | double | true |
| energy_silver | power_plant_register | 16 | capacity_net_mw | double | true |
| energy_silver | power_plant_register | 17 | generation_technology | string | true |
| energy_silver | power_plant_register | 18 | grid_voltage_level | string | true |
| energy_silver | power_plant_register | 19 | connecting_grid_operator | string | true |
| energy_silver | power_plant_register | 20 | is_border_plant | boolean | true |
| energy_silver | power_plant_register | 21 | border_plant_net_capacity_mw | double | true |
| energy_silver | power_plant_register | 22 | record_type_code | string | true |
| energy_silver | power_plant_register | 23 | record_type_label_de | string | true |
| energy_silver | power_plant_register | 24 | record_type | string | true |
| energy_silver | power_plant_register | 25 | operating_status_code | string | true |
| energy_silver | power_plant_register | 26 | operating_status_label_de | string | true |
| energy_silver | power_plant_register | 27 | operating_status | string | true |
| energy_silver | power_plant_register | 28 | energy_carrier_code | string | true |
| energy_silver | power_plant_register | 29 | energy_carrier_label_de | string | true |
| energy_silver | power_plant_register | 30 | energy_carrier | string | true |
| energy_silver | power_plant_register | 31 | feed_in_type_code | string | true |
| energy_silver | power_plant_register | 32 | feed_in_type_label_de | string | true |
| energy_silver | power_plant_register | 33 | feed_in_type | string | true |
| energy_silver | power_plant_register | 34 | official_municipality_key | string | true |
| energy_silver | power_plant_register | 35 | official_municipality_key_level | string | true |
| energy_silver | power_plant_register | 36 | official_municipality_key_method | string | true |
| energy_silver | power_plant_register | 37 | _bundesland_out_of_set | boolean | true |
| energy_silver | power_plant_register | 38 | energy_carrier_mastr_matched | boolean | true |
| energy_silver | power_plant_register | 39 | _source_id_ordinal | int | true |
| energy_silver | power_plant_register | 40 | _source_id_disambiguated | boolean | true |
| energy_silver | power_plant_register | 41 | source_record_id | string | true |
| energy_silver | power_plant_register | 42 | source_dataset | string | true |
| energy_silver | power_plant_register | 43 | source_system | string | true |
| energy_silver | power_plant_register | 44 | _silver_loaded_at | timestamp | true |
| energy_silver | power_plant_register | 45 | _silver_run_id | string | true |
| energy_silver | register_link | 1 | relationship_type | string | true |
| energy_silver | register_link | 2 | parent_type | string | true |
| energy_silver | register_link | 3 | parent_id | string | true |
| energy_silver | register_link | 4 | linked_type | string | true |
| energy_silver | register_link | 5 | linked_id | string | true |
| energy_silver | register_link | 6 | source_system | string | true |
| energy_silver | register_link | 7 | source_dataset | string | true |
| energy_silver | register_link | 8 | source_record_id | string | true |
| energy_silver | register_link | 9 | _silver_loaded_at | timestamp | true |
| energy_silver | register_link | 10 | _silver_run_id | string | true |
| energy_silver | support_registration | 1 | registration_date | timestamp | true |
| energy_silver | support_registration | 2 | last_updated_at | timestamp | true |
| energy_silver | support_registration | 3 | renewable_energy_act_commissioning_date | timestamp | true |
| energy_silver | support_registration | 4 | renewable_energy_act_support_id | string | true |
| energy_silver | support_registration | 5 | installation_register_identifier | string | true |
| energy_silver | support_registration | 6 | AnlagenkennzifferAnlagenregister_nv | string | true |
| energy_silver | support_registration | 7 | renewable_energy_act_installation_key | string | true |
| energy_silver | support_registration | 8 | pilot_plant_flag | string | true |
| energy_silver | support_registration | 9 | installed_capacity_kw | double | true |
| energy_silver | support_registration | 10 | yield_estimate_to_reference_yield_ratio | string | true |
| energy_silver | support_registration | 11 | VerhaeltnisErtragsschaetzungReferenzertrag_nv | string | true |
| energy_silver | support_registration | 12 | reference_yield_to_actual_yield_ratio_5_years | string | true |
| energy_silver | support_registration | 13 | VerhaeltnisReferenzertragErtrag5Jahre_nv | string | true |
| energy_silver | support_registration | 14 | VerhaeltnisReferenzertragErtrag10Jahre_nv | string | true |
| energy_silver | support_registration | 15 | VerhaeltnisReferenzertragErtrag15Jahre_nv | string | true |
| energy_silver | support_registration | 16 | auction_award_flag | string | true |
| energy_silver | support_registration | 17 | linked_unit_ids | string | true |
| energy_silver | support_registration | 18 | prototype_plant_flag | string | true |
| energy_silver | support_registration | 19 | auction_award_number | string | true |
| energy_silver | support_registration | 20 | reference_yield_to_actual_yield_ratio_10_years | string | true |
| energy_silver | support_registration | 21 | reference_yield_to_actual_yield_ratio_15_years | string | true |
| energy_silver | support_registration | 22 | support_status_code | string | true |
| energy_silver | support_registration | 23 | support_status_label_de | string | true |
| energy_silver | support_registration | 24 | support_status | string | true |
| energy_silver | support_registration | 25 | _unmatched_code_columns | string | true |
| energy_silver | support_registration | 26 | support_registration_id | string | true |
| energy_silver | support_registration | 27 | support_scheme | string | true |
| energy_silver | support_registration | 28 | unit_type | string | true |
| energy_silver | support_registration | 29 | source_dataset | string | true |
| energy_silver | support_registration | 30 | exclusive_use_of_biomass | string | true |
| energy_silver | support_registration | 31 | biogas_flexibility_premium_claimed | string | true |
| energy_silver | support_registration | 32 | biogas_flexibility_premium_claim_date | timestamp | true |
| energy_silver | support_registration | 33 | biogas_capacity_increased | string | true |
| energy_silver | support_registration | 34 | biogas_gas_production_capacity | string | true |
| energy_silver | support_registration | 35 | BiogasGaserzeugungskapazitaet_nv | string | true |
| energy_silver | support_registration | 36 | biogas_maximum_rated_capacity | string | true |
| energy_silver | support_registration | 37 | BiomethanErstmaligerEinsatz_nv | string | true |
| energy_silver | support_registration | 38 | biogas_capacity_increase_date | timestamp | true |
| energy_silver | support_registration | 39 | biogas_capacity_increase_extent | string | true |
| energy_silver | support_registration | 40 | biomethane_first_use | string | true |
| energy_silver | support_registration | 41 | retrofit_ids | string | true |
| energy_silver | support_registration | 42 | combined_heat_and_power_support_id | string | true |
| energy_silver | support_registration | 43 | commissioning_date | timestamp | true |
| energy_silver | support_registration | 44 | useful_thermal_output_kw | double | true |
| energy_silver | support_registration | 45 | electrical_combined_heat_and_power_capacity_kw | double | true |
| energy_silver | support_registration | 46 | source_record_id | string | true |
| energy_silver | support_registration | 47 | source_system | string | true |
| energy_silver | support_registration | 48 | _silver_loaded_at | timestamp | true |
| energy_silver | support_registration | 49 | _silver_run_id | string | true |
| energy_silver | thermal_energy | 1 | observation_key | string | true |
| energy_silver | thermal_energy | 2 | location_key | string | true |
| energy_silver | thermal_energy | 3 | source_location_id | string | true |
| energy_silver | thermal_energy | 4 | market_area_code | string | true |
| energy_silver | thermal_energy | 5 | observation_timestamp_native | string | true |
| energy_silver | thermal_energy | 6 | time_basis | string | true |
| energy_silver | thermal_energy | 7 | utc_offset_hours | double | true |
| energy_silver | thermal_energy | 8 | observation_timestamp_utc | timestamp | true |
| energy_silver | thermal_energy | 9 | observation_timestamp_project | timestamp | true |
| energy_silver | thermal_energy | 10 | local_date | date | true |
| energy_silver | thermal_energy | 11 | interval_seconds | int | true |
| energy_silver | thermal_energy | 12 | interval_reference | string | true |
| energy_silver | thermal_energy | 13 | heating_total_w | double | true |
| energy_silver | thermal_energy | 14 | heating_combined_heat_and_power_heat_w | double | true |
| energy_silver | thermal_energy | 15 | cooling_total_w | double | true |
| energy_silver | thermal_energy | 16 | sign_convention | string | true |
| energy_silver | thermal_energy | 17 | quality_flags | array<string> | true |
| energy_silver | thermal_energy | 18 | measurement_basis | string | true |
| energy_silver | thermal_energy | 19 | source_system | string | true |
| energy_silver | thermal_energy | 20 | source_dataset | string | true |
| energy_silver | thermal_energy | 21 | source_record_id | string | true |
| energy_silver | thermal_energy | 22 | _silver_loaded_at | timestamp | true |
| energy_silver | thermal_energy | 23 | _silver_run_id | string | true |
| energy_silver | unit_authorisation | 1 | authorisation_id | string | true |
| energy_silver | unit_authorisation | 2 | last_updated_at | timestamp | true |
| energy_silver | unit_authorisation | 3 | decision_date | timestamp | true |
| energy_silver | unit_authorisation | 4 | issuing_authority | string | true |
| energy_silver | unit_authorisation | 5 | file_reference | string | true |
| energy_silver | unit_authorisation | 6 | authorisation_deadline | timestamp | true |
| energy_silver | unit_authorisation | 7 | Frist_nv | string | true |
| energy_silver | unit_authorisation | 8 | WasserrechtAblaufdatum_nv | string | true |
| energy_silver | unit_authorisation | 9 | registration_date | timestamp | true |
| energy_silver | unit_authorisation | 10 | linked_unit_ids | string | true |
| energy_silver | unit_authorisation | 11 | application_date | timestamp | true |
| energy_silver | unit_authorisation | 12 | water_rights_number | string | true |
| energy_silver | unit_authorisation | 13 | water_rights_expiry_date | timestamp | true |
| energy_silver | unit_authorisation | 14 | authorisation_type_code | string | true |
| energy_silver | unit_authorisation | 15 | authorisation_type_label_de | string | true |
| energy_silver | unit_authorisation | 16 | authorisation_type | string | true |
| energy_silver | unit_authorisation | 17 | _unmatched_code_columns | string | true |
| energy_silver | unit_authorisation | 18 | source_record_id | string | true |
| energy_silver | unit_authorisation | 19 | source_dataset | string | true |
| energy_silver | unit_authorisation | 20 | source_system | string | true |
| energy_silver | unit_authorisation | 21 | _silver_loaded_at | timestamp | true |
| energy_silver | unit_authorisation | 22 | _silver_run_id | string | true |
| energy_silver | unit_deletion_event | 1 | last_updated_at | timestamp | true |
| energy_silver | unit_deletion_event | 2 | unit_id | string | true |
| energy_silver | unit_deletion_event | 3 | unit_type | string | true |
| energy_silver | unit_deletion_event | 4 | operating_status_code | string | true |
| energy_silver | unit_deletion_event | 5 | operating_status_label_de | string | true |
| energy_silver | unit_deletion_event | 6 | operating_status | string | true |
| energy_silver | unit_deletion_event | 7 | system_status_code | string | true |
| energy_silver | unit_deletion_event | 8 | system_status_label_de | string | true |
| energy_silver | unit_deletion_event | 9 | system_status | string | true |
| energy_silver | unit_deletion_event | 10 | _unmatched_code_columns | string | true |
| energy_silver | unit_deletion_event | 11 | source_record_id | string | true |
| energy_silver | unit_deletion_event | 12 | source_dataset | string | true |
| energy_silver | unit_deletion_event | 13 | source_system | string | true |
| energy_silver | unit_deletion_event | 14 | _silver_loaded_at | timestamp | true |
| energy_silver | unit_deletion_event | 15 | _silver_run_id | string | true |
| energy_silver | unit_repowering | 1 | repowering_id | string | true |
| energy_silver | unit_repowering | 2 | renewable_energy_act_support_id | string | true |
| energy_silver | unit_repowering | 3 | capacity_increase | double | true |
| energy_silver | unit_repowering | 4 | recommissioning_date | timestamp | true |
| energy_silver | unit_repowering | 5 | last_updated_at | timestamp | true |
| energy_silver | unit_repowering | 6 | repowering_type | string | true |
| energy_silver | unit_repowering | 7 | repowering_approval_required_flag | string | true |
| energy_silver | unit_repowering | 8 | _unmatched_code_columns | string | true |
| energy_silver | unit_repowering | 9 | source_record_id | string | true |
| energy_silver | unit_repowering | 10 | source_dataset | string | true |
| energy_silver | unit_repowering | 11 | source_system | string | true |
| energy_silver | unit_repowering | 12 | _silver_loaded_at | timestamp | true |
| energy_silver | unit_repowering | 13 | _silver_run_id | string | true |
| energy_silver | weather_cloud | 1 | observation_key | string | true |
| energy_silver | weather_cloud | 2 | location_key | string | true |
| energy_silver | weather_cloud | 3 | source_location_id | string | true |
| energy_silver | weather_cloud | 4 | observation_timestamp_native | string | true |
| energy_silver | weather_cloud | 5 | time_basis | string | true |
| energy_silver | weather_cloud | 6 | utc_offset_hours | double | true |
| energy_silver | weather_cloud | 7 | observation_timestamp_utc | timestamp | true |
| energy_silver | weather_cloud | 8 | observation_timestamp_project | timestamp | true |
| energy_silver | weather_cloud | 9 | local_date | date | true |
| energy_silver | weather_cloud | 10 | interval_seconds | int | true |
| energy_silver | weather_cloud | 11 | interval_reference | string | true |
| energy_silver | weather_cloud | 12 | cloud_cover_total_percent | double | true |
| energy_silver | weather_cloud | 13 | cloud_cover_total_native_value | double | true |
| energy_silver | weather_cloud | 14 | cloud_cover_total_native_unit | string | true |
| energy_silver | weather_cloud | 15 | cloud_cover_total_alternate | array<struct<value:double,native_value:double,native_unit:string,source_dataset:string,quality_code:string>> | true |
| energy_silver | weather_cloud | 16 | cloud_base_height_m | double | true |
| energy_silver | weather_cloud | 17 | observation_method | string | true |
| energy_silver | weather_cloud | 18 | layers | array<struct<layer_number:int,genus_code:string,genus_text:string,base_height_m:double,cover_percent:double,cover_native_value:double,cover_native_unit:string>> | true |
| energy_silver | weather_cloud | 19 | quality_code | string | true |
| energy_silver | weather_cloud | 20 | quality_label | string | true |
| energy_silver | weather_cloud | 21 | quality_flags | array<string> | true |
| energy_silver | weather_cloud | 22 | measurement_basis | string | true |
| energy_silver | weather_cloud | 23 | source_system | string | true |
| energy_silver | weather_cloud | 24 | source_dataset | string | true |
| energy_silver | weather_cloud | 25 | source_record_id | string | true |
| energy_silver | weather_cloud | 26 | _silver_loaded_at | timestamp | true |
| energy_silver | weather_cloud | 27 | _silver_run_id | string | true |
| energy_silver | weather_daily | 1 | daily_key | string | true |
| energy_silver | weather_daily | 2 | location_key | string | true |
| energy_silver | weather_daily | 3 | source_location_id | string | true |
| energy_silver | weather_daily | 4 | local_date | date | true |
| energy_silver | weather_daily | 5 | variable | string | true |
| energy_silver | weather_daily | 6 | statistic | string | true |
| energy_silver | weather_daily | 7 | level | string | true |
| energy_silver | weather_daily | 8 | is_primary | boolean | true |
| energy_silver | weather_daily | 9 | date_native | string | true |
| energy_silver | weather_daily | 10 | time_basis | string | true |
| energy_silver | weather_daily | 11 | value_native | double | true |
| energy_silver | weather_daily | 12 | unit_native | string | true |
| energy_silver | weather_daily | 13 | value | double | true |
| energy_silver | weather_daily | 14 | unit | string | true |
| energy_silver | weather_daily | 15 | value_origin | string | true |
| energy_silver | weather_daily | 16 | derivation_rule | string | true |
| energy_silver | weather_daily | 17 | observation_count | int | true |
| energy_silver | weather_daily | 18 | measurement_basis | string | true |
| energy_silver | weather_daily | 19 | source_system | string | true |
| energy_silver | weather_daily | 20 | source_dataset | string | true |
| energy_silver | weather_daily | 21 | source_column | string | true |
| energy_silver | weather_daily | 22 | source_record_id | string | true |
| energy_silver | weather_daily | 23 | _silver_loaded_at | timestamp | true |
| energy_silver | weather_daily | 24 | _silver_run_id | string | true |
| energy_silver | weather_humidity | 1 | observation_key | string | true |
| energy_silver | weather_humidity | 2 | location_key | string | true |
| energy_silver | weather_humidity | 3 | source_location_id | string | true |
| energy_silver | weather_humidity | 4 | observation_timestamp_native | string | true |
| energy_silver | weather_humidity | 5 | time_basis | string | true |
| energy_silver | weather_humidity | 6 | utc_offset_hours | double | true |
| energy_silver | weather_humidity | 7 | observation_timestamp_utc | timestamp | true |
| energy_silver | weather_humidity | 8 | observation_timestamp_project | timestamp | true |
| energy_silver | weather_humidity | 9 | local_date | date | true |
| energy_silver | weather_humidity | 10 | interval_seconds | int | true |
| energy_silver | weather_humidity | 11 | interval_reference | string | true |
| energy_silver | weather_humidity | 12 | relative_humidity_percent | double | true |
| energy_silver | weather_humidity | 13 | relative_humidity_alternate | array<struct<value:double,native_value:double,native_unit:string,source_dataset:string,quality_code:string>> | true |
| energy_silver | weather_humidity | 14 | absolute_humidity_g_per_m3 | double | true |
| energy_silver | weather_humidity | 15 | vapour_pressure_hpa | double | true |
| energy_silver | weather_humidity | 16 | quality_code | string | true |
| energy_silver | weather_humidity | 17 | quality_label | string | true |
| energy_silver | weather_humidity | 18 | quality_flags | array<string> | true |
| energy_silver | weather_humidity | 19 | measurement_basis | string | true |
| energy_silver | weather_humidity | 20 | source_system | string | true |
| energy_silver | weather_humidity | 21 | source_dataset | string | true |
| energy_silver | weather_humidity | 22 | source_record_id | string | true |
| energy_silver | weather_humidity | 23 | _silver_loaded_at | timestamp | true |
| energy_silver | weather_humidity | 24 | _silver_run_id | string | true |
| energy_silver | weather_longwave_radiation | 1 | observation_key | string | true |
| energy_silver | weather_longwave_radiation | 2 | location_key | string | true |
| energy_silver | weather_longwave_radiation | 3 | source_location_id | string | true |
| energy_silver | weather_longwave_radiation | 4 | observation_timestamp_native | string | true |
| energy_silver | weather_longwave_radiation | 5 | time_basis | string | true |
| energy_silver | weather_longwave_radiation | 6 | utc_offset_hours | double | true |
| energy_silver | weather_longwave_radiation | 7 | observation_timestamp_utc | timestamp | true |
| energy_silver | weather_longwave_radiation | 8 | observation_timestamp_project | timestamp | true |
| energy_silver | weather_longwave_radiation | 9 | local_date | date | true |
| energy_silver | weather_longwave_radiation | 10 | interval_seconds | int | true |
| energy_silver | weather_longwave_radiation | 11 | interval_reference | string | true |
| energy_silver | weather_longwave_radiation | 12 | true_solar_time_native | string | true |
| energy_silver | weather_longwave_radiation | 13 | longwave_downward_radiation_w_per_m2 | double | true |
| energy_silver | weather_longwave_radiation | 14 | longwave_downward_radiation_native_value | double | true |
| energy_silver | weather_longwave_radiation | 15 | longwave_downward_radiation_native_unit | string | true |
| energy_silver | weather_longwave_radiation | 16 | quality_code | string | true |
| energy_silver | weather_longwave_radiation | 17 | quality_label | string | true |
| energy_silver | weather_longwave_radiation | 18 | quality_flags | array<string> | true |
| energy_silver | weather_longwave_radiation | 19 | measurement_basis | string | true |
| energy_silver | weather_longwave_radiation | 20 | source_system | string | true |
| energy_silver | weather_longwave_radiation | 21 | source_dataset | string | true |
| energy_silver | weather_longwave_radiation | 22 | source_record_id | string | true |
| energy_silver | weather_longwave_radiation | 23 | _silver_loaded_at | timestamp | true |
| energy_silver | weather_longwave_radiation | 24 | _silver_run_id | string | true |
| energy_silver | weather_precipitation | 1 | observation_key | string | true |
| energy_silver | weather_precipitation | 2 | location_key | string | true |
| energy_silver | weather_precipitation | 3 | source_location_id | string | true |
| energy_silver | weather_precipitation | 4 | observation_timestamp_native | string | true |
| energy_silver | weather_precipitation | 5 | time_basis | string | true |
| energy_silver | weather_precipitation | 6 | utc_offset_hours | double | true |
| energy_silver | weather_precipitation | 7 | observation_timestamp_utc | timestamp | true |
| energy_silver | weather_precipitation | 8 | observation_timestamp_project | timestamp | true |
| energy_silver | weather_precipitation | 9 | local_date | date | true |
| energy_silver | weather_precipitation | 10 | interval_seconds | int | true |
| energy_silver | weather_precipitation | 11 | interval_reference | string | true |
| energy_silver | weather_precipitation | 12 | precipitation_mm | double | true |
| energy_silver | weather_precipitation | 13 | precipitation_occurred | boolean | true |
| energy_silver | weather_precipitation | 14 | precipitation_form_code | string | true |
| energy_silver | weather_precipitation | 15 | precipitation_form_text | string | true |
| energy_silver | weather_precipitation | 16 | quality_code | string | true |
| energy_silver | weather_precipitation | 17 | quality_label | string | true |
| energy_silver | weather_precipitation | 18 | quality_flags | array<string> | true |
| energy_silver | weather_precipitation | 19 | measurement_basis | string | true |
| energy_silver | weather_precipitation | 20 | source_system | string | true |
| energy_silver | weather_precipitation | 21 | source_dataset | string | true |
| energy_silver | weather_precipitation | 22 | source_record_id | string | true |
| energy_silver | weather_precipitation | 23 | _silver_loaded_at | timestamp | true |
| energy_silver | weather_precipitation | 24 | _silver_run_id | string | true |
| energy_silver | weather_present_weather | 1 | observation_key | string | true |
| energy_silver | weather_present_weather | 2 | location_key | string | true |
| energy_silver | weather_present_weather | 3 | source_location_id | string | true |
| energy_silver | weather_present_weather | 4 | observation_timestamp_native | string | true |
| energy_silver | weather_present_weather | 5 | time_basis | string | true |
| energy_silver | weather_present_weather | 6 | utc_offset_hours | double | true |
| energy_silver | weather_present_weather | 7 | observation_timestamp_utc | timestamp | true |
| energy_silver | weather_present_weather | 8 | observation_timestamp_project | timestamp | true |
| energy_silver | weather_present_weather | 9 | local_date | date | true |
| energy_silver | weather_present_weather | 10 | interval_seconds | int | true |
| energy_silver | weather_present_weather | 11 | interval_reference | string | true |
| energy_silver | weather_present_weather | 12 | present_weather_code | string | true |
| energy_silver | weather_present_weather | 13 | present_weather_text | string | true |
| energy_silver | weather_present_weather | 14 | quality_code | string | true |
| energy_silver | weather_present_weather | 15 | quality_label | string | true |
| energy_silver | weather_present_weather | 16 | quality_flags | array<string> | true |
| energy_silver | weather_present_weather | 17 | measurement_basis | string | true |
| energy_silver | weather_present_weather | 18 | source_system | string | true |
| energy_silver | weather_present_weather | 19 | source_dataset | string | true |
| energy_silver | weather_present_weather | 20 | source_record_id | string | true |
| energy_silver | weather_present_weather | 21 | _silver_loaded_at | timestamp | true |
| energy_silver | weather_present_weather | 22 | _silver_run_id | string | true |
| energy_silver | weather_pressure | 1 | observation_key | string | true |
| energy_silver | weather_pressure | 2 | location_key | string | true |
| energy_silver | weather_pressure | 3 | source_location_id | string | true |
| energy_silver | weather_pressure | 4 | observation_timestamp_native | string | true |
| energy_silver | weather_pressure | 5 | time_basis | string | true |
| energy_silver | weather_pressure | 6 | utc_offset_hours | double | true |
| energy_silver | weather_pressure | 7 | observation_timestamp_utc | timestamp | true |
| energy_silver | weather_pressure | 8 | observation_timestamp_project | timestamp | true |
| energy_silver | weather_pressure | 9 | local_date | date | true |
| energy_silver | weather_pressure | 10 | interval_seconds | int | true |
| energy_silver | weather_pressure | 11 | interval_reference | string | true |
| energy_silver | weather_pressure | 12 | pressure_station_hpa | double | true |
| energy_silver | weather_pressure | 13 | pressure_station_native_value | double | true |
| energy_silver | weather_pressure | 14 | pressure_station_native_unit | string | true |
| energy_silver | weather_pressure | 15 | pressure_station_alternate | array<struct<value:double,native_value:double,native_unit:string,source_dataset:string,quality_code:string>> | true |
| energy_silver | weather_pressure | 16 | pressure_sea_level_hpa | double | true |
| energy_silver | weather_pressure | 17 | pressure_sea_level_native_value | double | true |
| energy_silver | weather_pressure | 18 | pressure_sea_level_native_unit | string | true |
| energy_silver | weather_pressure | 19 | quality_code | string | true |
| energy_silver | weather_pressure | 20 | quality_label | string | true |
| energy_silver | weather_pressure | 21 | quality_flags | array<string> | true |
| energy_silver | weather_pressure | 22 | measurement_basis | string | true |
| energy_silver | weather_pressure | 23 | source_system | string | true |
| energy_silver | weather_pressure | 24 | source_dataset | string | true |
| energy_silver | weather_pressure | 25 | source_record_id | string | true |
| energy_silver | weather_pressure | 26 | _silver_loaded_at | timestamp | true |
| energy_silver | weather_pressure | 27 | _silver_run_id | string | true |
| energy_silver | weather_soil_temperature | 1 | observation_key | string | true |
| energy_silver | weather_soil_temperature | 2 | location_key | string | true |
| energy_silver | weather_soil_temperature | 3 | source_location_id | string | true |
| energy_silver | weather_soil_temperature | 4 | observation_timestamp_native | string | true |
| energy_silver | weather_soil_temperature | 5 | time_basis | string | true |
| energy_silver | weather_soil_temperature | 6 | utc_offset_hours | double | true |
| energy_silver | weather_soil_temperature | 7 | observation_timestamp_utc | timestamp | true |
| energy_silver | weather_soil_temperature | 8 | observation_timestamp_project | timestamp | true |
| energy_silver | weather_soil_temperature | 9 | local_date | date | true |
| energy_silver | weather_soil_temperature | 10 | interval_seconds | int | true |
| energy_silver | weather_soil_temperature | 11 | interval_reference | string | true |
| energy_silver | weather_soil_temperature | 12 | soil_temperature_2cm_degc | double | true |
| energy_silver | weather_soil_temperature | 13 | soil_temperature_5cm_degc | double | true |
| energy_silver | weather_soil_temperature | 14 | soil_temperature_10cm_degc | double | true |
| energy_silver | weather_soil_temperature | 15 | soil_temperature_20cm_degc | double | true |
| energy_silver | weather_soil_temperature | 16 | soil_temperature_50cm_degc | double | true |
| energy_silver | weather_soil_temperature | 17 | soil_temperature_100cm_degc | double | true |
| energy_silver | weather_soil_temperature | 18 | quality_code | string | true |
| energy_silver | weather_soil_temperature | 19 | quality_label | string | true |
| energy_silver | weather_soil_temperature | 20 | quality_flags | array<string> | true |
| energy_silver | weather_soil_temperature | 21 | measurement_basis | string | true |
| energy_silver | weather_soil_temperature | 22 | source_system | string | true |
| energy_silver | weather_soil_temperature | 23 | source_dataset | string | true |
| energy_silver | weather_soil_temperature | 24 | source_record_id | string | true |
| energy_silver | weather_soil_temperature | 25 | _silver_loaded_at | timestamp | true |
| energy_silver | weather_soil_temperature | 26 | _silver_run_id | string | true |
| energy_silver | weather_solar_geometry | 1 | observation_key | string | true |
| energy_silver | weather_solar_geometry | 2 | location_key | string | true |
| energy_silver | weather_solar_geometry | 3 | source_location_id | string | true |
| energy_silver | weather_solar_geometry | 4 | observation_timestamp_native | string | true |
| energy_silver | weather_solar_geometry | 5 | time_basis | string | true |
| energy_silver | weather_solar_geometry | 6 | utc_offset_hours | double | true |
| energy_silver | weather_solar_geometry | 7 | observation_timestamp_utc | timestamp | true |
| energy_silver | weather_solar_geometry | 8 | observation_timestamp_project | timestamp | true |
| energy_silver | weather_solar_geometry | 9 | local_date | date | true |
| energy_silver | weather_solar_geometry | 10 | interval_seconds | int | true |
| energy_silver | weather_solar_geometry | 11 | interval_reference | string | true |
| energy_silver | weather_solar_geometry | 12 | true_solar_time_native | string | true |
| energy_silver | weather_solar_geometry | 13 | solar_zenith_angle_degrees | double | true |
| energy_silver | weather_solar_geometry | 14 | quality_code | string | true |
| energy_silver | weather_solar_geometry | 15 | quality_label | string | true |
| energy_silver | weather_solar_geometry | 16 | quality_flags | array<string> | true |
| energy_silver | weather_solar_geometry | 17 | measurement_basis | string | true |
| energy_silver | weather_solar_geometry | 18 | source_system | string | true |
| energy_silver | weather_solar_geometry | 19 | source_dataset | string | true |
| energy_silver | weather_solar_geometry | 20 | source_record_id | string | true |
| energy_silver | weather_solar_geometry | 21 | _silver_loaded_at | timestamp | true |
| energy_silver | weather_solar_geometry | 22 | _silver_run_id | string | true |
| energy_silver | weather_solar_radiation | 1 | observation_key | string | true |
| energy_silver | weather_solar_radiation | 2 | location_key | string | true |
| energy_silver | weather_solar_radiation | 3 | source_location_id | string | true |
| energy_silver | weather_solar_radiation | 4 | observation_timestamp_native | string | true |
| energy_silver | weather_solar_radiation | 5 | time_basis | string | true |
| energy_silver | weather_solar_radiation | 6 | utc_offset_hours | double | true |
| energy_silver | weather_solar_radiation | 7 | observation_timestamp_utc | timestamp | true |
| energy_silver | weather_solar_radiation | 8 | observation_timestamp_project | timestamp | true |
| energy_silver | weather_solar_radiation | 9 | local_date | date | true |
| energy_silver | weather_solar_radiation | 10 | interval_seconds | int | true |
| energy_silver | weather_solar_radiation | 11 | interval_reference | string | true |
| energy_silver | weather_solar_radiation | 12 | true_solar_time_native | string | true |
| energy_silver | weather_solar_radiation | 13 | global_radiation_w_per_m2 | double | true |
| energy_silver | weather_solar_radiation | 14 | global_radiation_native_value | double | true |
| energy_silver | weather_solar_radiation | 15 | global_radiation_native_unit | string | true |
| energy_silver | weather_solar_radiation | 16 | diffuse_radiation_w_per_m2 | double | true |
| energy_silver | weather_solar_radiation | 17 | diffuse_radiation_native_value | double | true |
| energy_silver | weather_solar_radiation | 18 | diffuse_radiation_native_unit | string | true |
| energy_silver | weather_solar_radiation | 19 | quality_code | string | true |
| energy_silver | weather_solar_radiation | 20 | quality_label | string | true |
| energy_silver | weather_solar_radiation | 21 | quality_flags | array<string> | true |
| energy_silver | weather_solar_radiation | 22 | measurement_basis | string | true |
| energy_silver | weather_solar_radiation | 23 | source_system | string | true |
| energy_silver | weather_solar_radiation | 24 | source_dataset | string | true |
| energy_silver | weather_solar_radiation | 25 | source_record_id | string | true |
| energy_silver | weather_solar_radiation | 26 | _silver_loaded_at | timestamp | true |
| energy_silver | weather_solar_radiation | 27 | _silver_run_id | string | true |
| energy_silver | weather_sunshine_duration | 1 | observation_key | string | true |
| energy_silver | weather_sunshine_duration | 2 | location_key | string | true |
| energy_silver | weather_sunshine_duration | 3 | source_location_id | string | true |
| energy_silver | weather_sunshine_duration | 4 | observation_timestamp_native | string | true |
| energy_silver | weather_sunshine_duration | 5 | time_basis | string | true |
| energy_silver | weather_sunshine_duration | 6 | utc_offset_hours | double | true |
| energy_silver | weather_sunshine_duration | 7 | observation_timestamp_utc | timestamp | true |
| energy_silver | weather_sunshine_duration | 8 | observation_timestamp_project | timestamp | true |
| energy_silver | weather_sunshine_duration | 9 | local_date | date | true |
| energy_silver | weather_sunshine_duration | 10 | interval_seconds | int | true |
| energy_silver | weather_sunshine_duration | 11 | interval_reference | string | true |
| energy_silver | weather_sunshine_duration | 12 | true_solar_time_native | string | true |
| energy_silver | weather_sunshine_duration | 13 | sunshine_duration_minutes | double | true |
| energy_silver | weather_sunshine_duration | 14 | quality_code | string | true |
| energy_silver | weather_sunshine_duration | 15 | quality_label | string | true |
| energy_silver | weather_sunshine_duration | 16 | quality_flags | array<string> | true |
| energy_silver | weather_sunshine_duration | 17 | measurement_basis | string | true |
| energy_silver | weather_sunshine_duration | 18 | source_system | string | true |
| energy_silver | weather_sunshine_duration | 19 | source_dataset | string | true |
| energy_silver | weather_sunshine_duration | 20 | source_record_id | string | true |
| energy_silver | weather_sunshine_duration | 21 | _silver_loaded_at | timestamp | true |
| energy_silver | weather_sunshine_duration | 22 | _silver_run_id | string | true |
| energy_silver | weather_temperature | 1 | observation_key | string | true |
| energy_silver | weather_temperature | 2 | location_key | string | true |
| energy_silver | weather_temperature | 3 | source_location_id | string | true |
| energy_silver | weather_temperature | 4 | observation_timestamp_native | string | true |
| energy_silver | weather_temperature | 5 | time_basis | string | true |
| energy_silver | weather_temperature | 6 | utc_offset_hours | double | true |
| energy_silver | weather_temperature | 7 | observation_timestamp_utc | timestamp | true |
| energy_silver | weather_temperature | 8 | observation_timestamp_project | timestamp | true |
| energy_silver | weather_temperature | 9 | local_date | date | true |
| energy_silver | weather_temperature | 10 | interval_seconds | int | true |
| energy_silver | weather_temperature | 11 | interval_reference | string | true |
| energy_silver | weather_temperature | 12 | air_temperature_degc | double | true |
| energy_silver | weather_temperature | 13 | air_temperature_alternate | array<struct<value:double,native_value:double,native_unit:string,source_dataset:string,quality_code:string>> | true |
| energy_silver | weather_temperature | 14 | dew_point_temperature_degc | double | true |
| energy_silver | weather_temperature | 15 | dew_point_temperature_alternate | array<struct<value:double,native_value:double,native_unit:string,source_dataset:string,quality_code:string>> | true |
| energy_silver | weather_temperature | 16 | wet_bulb_temperature_degc | double | true |
| energy_silver | weather_temperature | 17 | quality_code | string | true |
| energy_silver | weather_temperature | 18 | quality_label | string | true |
| energy_silver | weather_temperature | 19 | quality_flags | array<string> | true |
| energy_silver | weather_temperature | 20 | measurement_basis | string | true |
| energy_silver | weather_temperature | 21 | source_system | string | true |
| energy_silver | weather_temperature | 22 | source_dataset | string | true |
| energy_silver | weather_temperature | 23 | source_record_id | string | true |
| energy_silver | weather_temperature | 24 | _silver_loaded_at | timestamp | true |
| energy_silver | weather_temperature | 25 | _silver_run_id | string | true |
| energy_silver | weather_uv_index | 1 | observation_key | string | true |
| energy_silver | weather_uv_index | 2 | location_key | string | true |
| energy_silver | weather_uv_index | 3 | source_location_id | string | true |
| energy_silver | weather_uv_index | 4 | observation_timestamp_native | string | true |
| energy_silver | weather_uv_index | 5 | time_basis | string | true |
| energy_silver | weather_uv_index | 6 | utc_offset_hours | double | true |
| energy_silver | weather_uv_index | 7 | observation_timestamp_utc | timestamp | true |
| energy_silver | weather_uv_index | 8 | observation_timestamp_project | timestamp | true |
| energy_silver | weather_uv_index | 9 | local_date | date | true |
| energy_silver | weather_uv_index | 10 | interval_seconds | int | true |
| energy_silver | weather_uv_index | 11 | interval_reference | string | true |
| energy_silver | weather_uv_index | 12 | uv_index | double | true |
| energy_silver | weather_uv_index | 13 | quality_code | string | true |
| energy_silver | weather_uv_index | 14 | quality_label | string | true |
| energy_silver | weather_uv_index | 15 | quality_flags | array<string> | true |
| energy_silver | weather_uv_index | 16 | measurement_basis | string | true |
| energy_silver | weather_uv_index | 17 | source_system | string | true |
| energy_silver | weather_uv_index | 18 | source_dataset | string | true |
| energy_silver | weather_uv_index | 19 | source_record_id | string | true |
| energy_silver | weather_uv_index | 20 | _silver_loaded_at | timestamp | true |
| energy_silver | weather_uv_index | 21 | _silver_run_id | string | true |
| energy_silver | weather_visibility | 1 | observation_key | string | true |
| energy_silver | weather_visibility | 2 | location_key | string | true |
| energy_silver | weather_visibility | 3 | source_location_id | string | true |
| energy_silver | weather_visibility | 4 | observation_timestamp_native | string | true |
| energy_silver | weather_visibility | 5 | time_basis | string | true |
| energy_silver | weather_visibility | 6 | utc_offset_hours | double | true |
| energy_silver | weather_visibility | 7 | observation_timestamp_utc | timestamp | true |
| energy_silver | weather_visibility | 8 | observation_timestamp_project | timestamp | true |
| energy_silver | weather_visibility | 9 | local_date | date | true |
| energy_silver | weather_visibility | 10 | interval_seconds | int | true |
| energy_silver | weather_visibility | 11 | interval_reference | string | true |
| energy_silver | weather_visibility | 12 | visibility_m | double | true |
| energy_silver | weather_visibility | 13 | visibility_native_value | double | true |
| energy_silver | weather_visibility | 14 | visibility_native_unit | string | true |
| energy_silver | weather_visibility | 15 | observation_method | string | true |
| energy_silver | weather_visibility | 16 | quality_code | string | true |
| energy_silver | weather_visibility | 17 | quality_label | string | true |
| energy_silver | weather_visibility | 18 | quality_flags | array<string> | true |
| energy_silver | weather_visibility | 19 | measurement_basis | string | true |
| energy_silver | weather_visibility | 20 | source_system | string | true |
| energy_silver | weather_visibility | 21 | source_dataset | string | true |
| energy_silver | weather_visibility | 22 | source_record_id | string | true |
| energy_silver | weather_visibility | 23 | _silver_loaded_at | timestamp | true |
| energy_silver | weather_visibility | 24 | _silver_run_id | string | true |
| energy_silver | weather_wind | 1 | observation_key | string | true |
| energy_silver | weather_wind | 2 | location_key | string | true |
| energy_silver | weather_wind | 3 | source_location_id | string | true |
| energy_silver | weather_wind | 4 | observation_timestamp_native | string | true |
| energy_silver | weather_wind | 5 | time_basis | string | true |
| energy_silver | weather_wind | 6 | utc_offset_hours | double | true |
| energy_silver | weather_wind | 7 | observation_timestamp_utc | timestamp | true |
| energy_silver | weather_wind | 8 | observation_timestamp_project | timestamp | true |
| energy_silver | weather_wind | 9 | local_date | date | true |
| energy_silver | weather_wind | 10 | interval_seconds | int | true |
| energy_silver | weather_wind | 11 | interval_reference | string | true |
| energy_silver | weather_wind | 12 | readings | array<struct<statistic:string,wind_speed_m_per_s:double,wind_direction_degrees:double,wind_direction_variable:boolean,wind_gust_m_per_s:double,source_dataset:string>> | true |
| energy_silver | weather_wind | 13 | quality_code | string | true |
| energy_silver | weather_wind | 14 | quality_label | string | true |
| energy_silver | weather_wind | 15 | quality_flags | array<string> | true |
| energy_silver | weather_wind | 16 | measurement_basis | string | true |
| energy_silver | weather_wind | 17 | source_system | string | true |
| energy_silver | weather_wind | 18 | source_dataset | string | true |
| energy_silver | weather_wind | 19 | source_record_id | string | true |
| energy_silver | weather_wind | 20 | _silver_loaded_at | timestamp | true |
| energy_silver | weather_wind | 21 | _silver_run_id | string | true |
| energy_silver_reference | balancing_area | 1 | balancing_area_id | string | true |
| energy_silver_reference | balancing_area | 2 | area_energy_identification_code | string | true |
| energy_silver_reference | balancing_area | 3 | control_zone | string | true |
| energy_silver_reference | balancing_area | 4 | balancing_area_connection_point | string | true |
| energy_silver_reference | balancing_area | 5 | _unmatched_code_columns | string | true |
| energy_silver_reference | balancing_area | 6 | _source_id_ordinal | int | true |
| energy_silver_reference | balancing_area | 7 | _source_id_disambiguated | boolean | true |
| energy_silver_reference | balancing_area | 8 | source_record_id | string | true |
| energy_silver_reference | balancing_area | 9 | source_dataset | string | true |
| energy_silver_reference | balancing_area | 10 | source_system | string | true |
| energy_silver_reference | balancing_area | 11 | _silver_loaded_at | timestamp | true |
| energy_silver_reference | balancing_area | 12 | _silver_run_id | string | true |
| energy_silver_reference | grid_connection_point | 1 | connection_point_id | string | true |
| energy_silver_reference | grid_connection_point | 2 | last_changed_at | timestamp | true |
| energy_silver_reference | grid_connection_point | 3 | location_id | string | true |
| energy_silver_reference | grid_connection_point | 4 | technical_location_name | string | true |
| energy_silver_reference | grid_connection_point | 5 | location_type | string | true |
| energy_silver_reference | grid_connection_point | 6 | metering_location | string | true |
| energy_silver_reference | grid_connection_point | 7 | voltage_level | string | true |
| energy_silver_reference | grid_connection_point | 8 | net_bottleneck_capacity_kw | double | true |
| energy_silver_reference | grid_connection_point | 9 | balancing_area_connection_point_id | string | true |
| energy_silver_reference | grid_connection_point | 10 | control_zone | string | true |
| energy_silver_reference | grid_connection_point | 11 | grid_id | string | true |
| energy_silver_reference | grid_connection_point | 12 | still_in_planning_flag | string | true |
| energy_silver_reference | grid_connection_point | 13 | grid_operator_id | string | true |
| energy_silver_reference | grid_connection_point | 14 | connection_point_name | string | true |
| energy_silver_reference | grid_connection_point | 15 | max_withdrawal_power_kw | double | true |
| energy_silver_reference | grid_connection_point | 16 | gas_quality | string | true |
| energy_silver_reference | grid_connection_point | 17 | grid_connection_capacity_kw | double | true |
| energy_silver_reference | grid_connection_point | 18 | max_feed_in_power_kw | double | true |
| energy_silver_reference | grid_connection_point | 19 | _unmatched_code_columns | string | true |
| energy_silver_reference | grid_connection_point | 20 | source_record_id | string | true |
| energy_silver_reference | grid_connection_point | 21 | source_dataset | string | true |
| energy_silver_reference | grid_connection_point | 22 | source_system | string | true |
| energy_silver_reference | grid_connection_point | 23 | _silver_loaded_at | timestamp | true |
| energy_silver_reference | grid_connection_point | 24 | _silver_run_id | string | true |
| energy_silver_reference | grid_location | 1 | last_updated_at | timestamp | true |
| energy_silver_reference | grid_location | 2 | location_id | string | true |
| energy_silver_reference | grid_location | 3 | technical_location_name | string | true |
| energy_silver_reference | grid_location | 4 | location_type | string | true |
| energy_silver_reference | grid_location | 5 | linked_unit_ids | string | true |
| energy_silver_reference | grid_location | 6 | connection_point_ids | string | true |
| energy_silver_reference | grid_location | 7 | _unmatched_code_columns | string | true |
| energy_silver_reference | grid_location | 8 | source_record_id | string | true |
| energy_silver_reference | grid_location | 9 | source_dataset | string | true |
| energy_silver_reference | grid_location | 10 | source_system | string | true |
| energy_silver_reference | grid_location | 11 | _silver_loaded_at | timestamp | true |
| energy_silver_reference | grid_location | 12 | _silver_run_id | string | true |
| energy_silver_reference | grid_network | 1 | last_updated_at | timestamp | true |
| energy_silver_reference | grid_network | 2 | grid_id | string | true |
| energy_silver_reference | grid_network | 3 | grid_sector | string | true |
| energy_silver_reference | grid_network | 4 | customers_connected_flag | string | true |
| energy_silver_reference | grid_network | 5 | is_closed_distribution_network | string | true |
| energy_silver_reference | grid_network | 6 | name | string | true |
| energy_silver_reference | grid_network | 7 | balancing_areas | string | true |
| energy_silver_reference | grid_network | 8 | market_area | string | true |
| energy_silver_reference | grid_network | 9 | federal_state_code | string | true |
| energy_silver_reference | grid_network | 10 | federal_state_label_de | string | true |
| energy_silver_reference | grid_network | 11 | federal_state | string | true |
| energy_silver_reference | grid_network | 12 | _unmatched_code_columns | string | true |
| energy_silver_reference | grid_network | 13 | source_record_id | string | true |
| energy_silver_reference | grid_network | 14 | source_dataset | string | true |
| energy_silver_reference | grid_network | 15 | source_system | string | true |
| energy_silver_reference | grid_network | 16 | _silver_loaded_at | timestamp | true |
| energy_silver_reference | grid_network | 17 | _silver_run_id | string | true |
| energy_silver_reference | market_actor | 1 | market_actor_id | string | true |
| energy_silver_reference | market_actor | 2 | last_updated_at | timestamp | true |
| energy_silver_reference | market_actor | 3 | actor_person_type | string | true |
| energy_silver_reference | market_actor | 4 | company_name | string | true |
| energy_silver_reference | market_actor | 5 | market_function | string | true |
| energy_silver_reference | market_actor | 6 | legal_form | string | true |
| energy_silver_reference | market_actor | 7 | country | string | true |
| energy_silver_reference | market_actor | 8 | street | string | true |
| energy_silver_reference | market_actor | 9 | house_number | string | true |
| energy_silver_reference | market_actor | 10 | Hausnummer_nv | string | true |
| energy_silver_reference | market_actor | 11 | postcode | string | true |
| energy_silver_reference | market_actor | 12 | locality | string | true |
| energy_silver_reference | market_actor | 13 | telephone | string | true |
| energy_silver_reference | market_actor | 14 | Fax_nv | string | true |
| energy_silver_reference | market_actor | 15 | Webseite_nv | string | true |
| energy_silver_reference | market_actor | 16 | activity_start_date | timestamp | true |
| energy_silver_reference | market_actor | 17 | AcerCode_nv | string | true |
| energy_silver_reference | market_actor | 18 | Umsatzsteueridentifikationsnummer_nv | string | true |
| energy_silver_reference | market_actor | 19 | federal_network_agency_operating_number | string | true |
| energy_silver_reference | market_actor | 20 | BundesnetzagenturBetriebsnummer_nv | string | true |
| energy_silver_reference | market_actor | 21 | delivery_address_country | string | true |
| energy_silver_reference | market_actor | 22 | delivery_address_postcode | string | true |
| energy_silver_reference | market_actor | 23 | delivery_address_city | string | true |
| energy_silver_reference | market_actor | 24 | delivery_address_street | string | true |
| energy_silver_reference | market_actor | 25 | delivery_address_house_number | string | true |
| energy_silver_reference | market_actor | 26 | HausnummerAnZustelladresse_nv | string | true |
| energy_silver_reference | market_actor | 27 | market_actor_registration_date | timestamp | true |
| energy_silver_reference | market_actor | 28 | email | string | true |
| energy_silver_reference | market_actor | 29 | foreign_registry_court | string | true |
| energy_silver_reference | market_actor | 30 | market_roles | string | true |
| energy_silver_reference | market_actor | 31 | grid | string | true |
| energy_silver_reference | market_actor | 32 | website | string | true |
| energy_silver_reference | market_actor | 33 | registry_court | string | true |
| energy_silver_reference | market_actor | 34 | registry_number_prefix | string | true |
| energy_silver_reference | market_actor | 35 | registry_number | string | true |
| energy_silver_reference | market_actor | 36 | energy_regulators_agency_code | string | true |
| energy_silver_reference | market_actor | 37 | value_added_tax_identification_number | string | true |
| energy_silver_reference | market_actor | 38 | fax | string | true |
| energy_silver_reference | market_actor | 39 | address_supplement | string | true |
| energy_silver_reference | market_actor | 40 | activity_end_date | timestamp | true |
| energy_silver_reference | market_actor | 41 | grid_operator_web_portal | string | true |
| energy_silver_reference | market_actor | 42 | delivery_address_supplement | string | true |
| energy_silver_reference | market_actor | 43 | region | string | true |
| energy_silver_reference | market_actor | 44 | european_statistical_region_level_2 | string | true |
| energy_silver_reference | market_actor | 45 | market_actor_first_name | string | true |
| energy_silver_reference | market_actor | 46 | market_actor_last_name | string | true |
| energy_silver_reference | market_actor | 47 | other_legal_form | string | true |
| energy_silver_reference | market_actor | 48 | is_small_or_medium_enterprise | string | true |
| energy_silver_reference | market_actor | 49 | main_economic_sector_division | string | true |
| energy_silver_reference | market_actor | 50 | main_economic_sector_group | string | true |
| energy_silver_reference | market_actor | 51 | economic_sector_section | string | true |
| energy_silver_reference | market_actor | 52 | direct_marketing_company | string | true |
| energy_silver_reference | market_actor | 53 | supplies_end_consumers_electricity | string | true |
| energy_silver_reference | market_actor | 54 | supplies_household_customers_electricity | string | true |
| energy_silver_reference | market_actor | 55 | electricity_wholesaler | string | true |
| energy_silver_reference | market_actor | 56 | gas_wholesaler | string | true |
| energy_silver_reference | market_actor | 57 | supplies_end_consumers_gas | string | true |
| energy_silver_reference | market_actor | 58 | supplies_household_customers_gas | string | true |
| energy_silver_reference | market_actor | 59 | foreign_registry_number | string | true |
| energy_silver_reference | market_actor | 60 | market_actor_salutation | string | true |
| energy_silver_reference | market_actor | 61 | federal_state_code | string | true |
| energy_silver_reference | market_actor | 62 | federal_state_label_de | string | true |
| energy_silver_reference | market_actor | 63 | federal_state | string | true |
| energy_silver_reference | market_actor | 64 | _unmatched_code_columns | string | true |
| energy_silver_reference | market_actor | 65 | source_record_id | string | true |
| energy_silver_reference | market_actor | 66 | source_dataset | string | true |
| energy_silver_reference | market_actor | 67 | source_system | string | true |
| energy_silver_reference | market_actor | 68 | _silver_loaded_at | timestamp | true |
| energy_silver_reference | market_actor | 69 | _silver_run_id | string | true |
| energy_silver_reference | market_actor_role | 1 | market_actor_id | string | true |
| energy_silver_reference | market_actor_role | 2 | last_updated_at | timestamp | true |
| energy_silver_reference | market_actor_role | 3 | market_actor_role_id | string | true |
| energy_silver_reference | market_actor_role | 4 | market_role | string | true |
| energy_silver_reference | market_actor_role | 5 | federal_network_agency_operating_number | string | true |
| energy_silver_reference | market_actor_role | 6 | BundesnetzagenturBetriebsnummer_nv | string | true |
| energy_silver_reference | market_actor_role | 7 | Marktpartneridentifikationsnummer_nv | string | true |
| energy_silver_reference | market_actor_role | 8 | market_role_contact_details | string | true |
| energy_silver_reference | market_actor_role | 9 | market_partner_identification_number | string | true |
| energy_silver_reference | market_actor_role | 10 | _unmatched_code_columns | string | true |
| energy_silver_reference | market_actor_role | 11 | source_record_id | string | true |
| energy_silver_reference | market_actor_role | 12 | source_dataset | string | true |
| energy_silver_reference | market_actor_role | 13 | source_system | string | true |
| energy_silver_reference | market_actor_role | 14 | _silver_loaded_at | timestamp | true |
| energy_silver_reference | market_actor_role | 15 | _silver_run_id | string | true |
| energy_silver_reference | mastr_code_list | 1 | catalog_kind | string | true |
| energy_silver_reference | mastr_code_list | 2 | code_id | string | true |
| energy_silver_reference | mastr_code_list | 3 | parent_id | string | true |
| energy_silver_reference | mastr_code_list | 4 | label | string | true |
| energy_silver_reference | mastr_code_list | 5 | source_system | string | true |
| energy_silver_reference | mastr_code_list | 6 | source_dataset | string | true |
| energy_silver_reference | mastr_code_list | 7 | source_record_id | string | true |
| energy_silver_reference | mastr_code_list | 8 | _silver_loaded_at | timestamp | true |
| energy_silver_reference | mastr_code_list | 9 | _silver_run_id | string | true |
| energy_silver_reference | weather_location | 1 | location_key | string | true |
| energy_silver_reference | weather_location | 2 | source_location_id | string | true |
| energy_silver_reference | weather_location | 3 | location_role | string | true |
| energy_silver_reference | weather_location | 4 | name | string | true |
| energy_silver_reference | weather_location | 5 | latitude | double | true |
| energy_silver_reference | weather_location | 6 | longitude | double | true |
| energy_silver_reference | weather_location | 7 | elevation_m | double | true |
| energy_silver_reference | weather_location | 8 | continent | string | true |
| energy_silver_reference | weather_location | 9 | country_code | string | true |
| energy_silver_reference | weather_location | 10 | region | string | true |
| energy_silver_reference | weather_location | 11 | city | string | true |
| energy_silver_reference | weather_location | 12 | official_municipality_key | string | true |
| energy_silver_reference | weather_location | 13 | geography_basis | string | true |
| energy_silver_reference | weather_location | 14 | source_system | string | true |
| energy_silver_reference | weather_location | 15 | source_dataset | string | true |
| energy_silver_reference | weather_location | 16 | source_record_id | string | true |
| energy_silver_reference | weather_location | 17 | _silver_loaded_at | timestamp | true |
| energy_silver_reference | weather_location | 18 | _silver_run_id | string | true |
| energy_silver_reference | weather_location_validity | 1 | location_key | string | true |
| energy_silver_reference | weather_location_validity | 2 | source_location_id | string | true |
| energy_silver_reference | weather_location_validity | 3 | valid_from | date | true |
| energy_silver_reference | weather_location_validity | 4 | valid_to | date | true |
| energy_silver_reference | weather_location_validity | 5 | record_ordinal | int | true |
| energy_silver_reference | weather_location_validity | 6 | name | string | true |
| energy_silver_reference | weather_location_validity | 7 | latitude | double | true |
| energy_silver_reference | weather_location_validity | 8 | longitude | double | true |
| energy_silver_reference | weather_location_validity | 9 | elevation_m | double | true |
| energy_silver_reference | weather_location_validity | 10 | quality_flags | array<string> | true |
| energy_silver_reference | weather_location_validity | 11 | source_system | string | true |
| energy_silver_reference | weather_location_validity | 12 | source_dataset | string | true |
| energy_silver_reference | weather_location_validity | 13 | source_record_id | string | true |
| energy_silver_reference | weather_location_validity | 14 | _silver_loaded_at | timestamp | true |
| energy_silver_reference | weather_location_validity | 15 | _silver_run_id | string | true |
| energy_silver_reference | weather_missing_value_period | 1 | location_key | string | true |
| energy_silver_reference | weather_missing_value_period | 2 | source_location_id | string | true |
| energy_silver_reference | weather_missing_value_period | 3 | name | string | true |
| energy_silver_reference | weather_missing_value_period | 4 | parameter_source_code | string | true |
| energy_silver_reference | weather_missing_value_period | 5 | gap_start_timestamp | timestamp | true |
| energy_silver_reference | weather_missing_value_period | 6 | gap_end_timestamp | timestamp | true |
| energy_silver_reference | weather_missing_value_period | 7 | record_ordinal | int | true |
| energy_silver_reference | weather_missing_value_period | 8 | missing_value_count | bigint | true |
| energy_silver_reference | weather_missing_value_period | 9 | gap_description | string | true |
| energy_silver_reference | weather_missing_value_period | 10 | source_system | string | true |
| energy_silver_reference | weather_missing_value_period | 11 | source_dataset | string | true |
| energy_silver_reference | weather_missing_value_period | 12 | source_record_id | string | true |
| energy_silver_reference | weather_missing_value_period | 13 | _silver_loaded_at | timestamp | true |
| energy_silver_reference | weather_missing_value_period | 14 | _silver_run_id | string | true |
| energy_silver_reference | weather_missingness_reconciliation | 1 | location_key | string | true |
| energy_silver_reference | weather_missingness_reconciliation | 2 | source_location_id | string | true |
| energy_silver_reference | weather_missingness_reconciliation | 3 | parameter_source_code | string | true |
| energy_silver_reference | weather_missingness_reconciliation | 4 | reconciliation_status | string | true |
| energy_silver_reference | weather_missingness_reconciliation | 5 | source_system | string | true |
| energy_silver_reference | weather_missingness_reconciliation | 6 | source_dataset | string | true |
| energy_silver_reference | weather_missingness_reconciliation | 7 | source_record_id | string | true |
| energy_silver_reference | weather_missingness_reconciliation | 8 | _silver_loaded_at | timestamp | true |
| energy_silver_reference | weather_missingness_reconciliation | 9 | _silver_run_id | string | true |
| energy_silver_reference | weather_parameter_catalog | 1 | parameter_source_code | string | true |
| energy_silver_reference | weather_parameter_catalog | 2 | parameter_business_name | string | true |
| energy_silver_reference | weather_parameter_catalog | 3 | parameter_unit | string | true |
| energy_silver_reference | weather_parameter_catalog | 4 | parameter_description_de | string | true |
| energy_silver_reference | weather_parameter_catalog | 5 | catalog_vintage | string | true |
| energy_silver_reference | weather_parameter_catalog | 6 | source_system | string | true |
| energy_silver_reference | weather_parameter_catalog | 7 | source_dataset | string | true |
| energy_silver_reference | weather_parameter_catalog | 8 | source_record_id | string | true |
| energy_silver_reference | weather_parameter_catalog | 9 | _silver_loaded_at | timestamp | true |
| energy_silver_reference | weather_parameter_catalog | 10 | _silver_run_id | string | true |
| energy_silver_reference | weather_parameter_period | 1 | location_key | string | true |
| energy_silver_reference | weather_parameter_period | 2 | source_location_id | string | true |
| energy_silver_reference | weather_parameter_period | 3 | valid_from | date | true |
| energy_silver_reference | weather_parameter_period | 4 | valid_to | date | true |
| energy_silver_reference | weather_parameter_period | 5 | record_ordinal | int | true |
| energy_silver_reference | weather_parameter_period | 6 | name | string | true |
| energy_silver_reference | weather_parameter_period | 7 | parameter_source_code | string | true |
| energy_silver_reference | weather_parameter_period | 8 | parameter_description_de | string | true |
| energy_silver_reference | weather_parameter_period | 9 | parameter_unit | string | true |
| energy_silver_reference | weather_parameter_period | 10 | parameter_data_source | string | true |
| energy_silver_reference | weather_parameter_period | 11 | parameter_extra_info | string | true |
| energy_silver_reference | weather_parameter_period | 12 | parameter_special_notes | string | true |
| energy_silver_reference | weather_parameter_period | 13 | parameter_reference | string | true |
| energy_silver_reference | weather_parameter_period | 14 | source_system | string | true |
| energy_silver_reference | weather_parameter_period | 15 | source_dataset | string | true |
| energy_silver_reference | weather_parameter_period | 16 | source_record_id | string | true |
| energy_silver_reference | weather_parameter_period | 17 | _silver_loaded_at | timestamp | true |
| energy_silver_reference | weather_parameter_period | 18 | _silver_run_id | string | true |
| energy_silver_reference | weather_station_instrument | 1 | location_key | string | true |
| energy_silver_reference | weather_station_instrument | 2 | source_location_id | string | true |
| energy_silver_reference | weather_station_instrument | 3 | valid_from | date | true |
| energy_silver_reference | weather_station_instrument | 4 | valid_to | date | true |
| energy_silver_reference | weather_station_instrument | 5 | record_ordinal | int | true |
| energy_silver_reference | weather_station_instrument | 6 | name | string | true |
| energy_silver_reference | weather_station_instrument | 7 | parameter_category | string | true |
| energy_silver_reference | weather_station_instrument | 8 | latitude | double | true |
| energy_silver_reference | weather_station_instrument | 9 | longitude | double | true |
| energy_silver_reference | weather_station_instrument | 10 | elevation_m | double | true |
| energy_silver_reference | weather_station_instrument | 11 | sensor_height_m | double | true |
| energy_silver_reference | weather_station_instrument | 12 | device_type | string | true |
| energy_silver_reference | weather_station_instrument | 13 | measurement_method | string | true |
| energy_silver_reference | weather_station_instrument | 14 | source_system | string | true |
| energy_silver_reference | weather_station_instrument | 15 | source_dataset | string | true |
| energy_silver_reference | weather_station_instrument | 16 | source_record_id | string | true |
| energy_silver_reference | weather_station_instrument | 17 | _silver_loaded_at | timestamp | true |
| energy_silver_reference | weather_station_instrument | 18 | _silver_run_id | string | true |
| energy_silver_reference | weather_station_name_history | 1 | location_key | string | true |
| energy_silver_reference | weather_station_name_history | 2 | source_location_id | string | true |
| energy_silver_reference | weather_station_name_history | 3 | valid_from | date | true |
| energy_silver_reference | weather_station_name_history | 4 | valid_to | date | true |
| energy_silver_reference | weather_station_name_history | 5 | record_ordinal | int | true |
| energy_silver_reference | weather_station_name_history | 6 | name | string | true |
| energy_silver_reference | weather_station_name_history | 7 | source_system | string | true |
| energy_silver_reference | weather_station_name_history | 8 | source_dataset | string | true |
| energy_silver_reference | weather_station_name_history | 9 | source_record_id | string | true |
| energy_silver_reference | weather_station_name_history | 10 | _silver_loaded_at | timestamp | true |
| energy_silver_reference | weather_station_name_history | 11 | _silver_run_id | string | true |
