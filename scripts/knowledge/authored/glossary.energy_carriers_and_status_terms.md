# Glossary: energy carriers and operating status

Every source that carries a concept spells it the same way, so the Gold layer needs no second normalisation.

Energy carrier names: `wind_onshore`, `wind_offshore`, `solar_photovoltaic`, `hydropower`, `pumped_storage`, `biomass`, `natural_gas`, `hard_coal`, `lignite`, `nuclear`, `geothermal`, `battery_storage`, `waste`, `mine_gas`, `other_renewable`, `other_conventional`, `heat`.

The redispatch primary energy type has only three coarse values (conventional, renewable, other). It maps to the coarse buckets (`other_conventional`, `other_renewable`), not to single carriers.

Operating-status buckets for MaStR and the power plant list: `planned`, `in_operation`, `temporarily_shut_down`, `permanently_shut_down`, `reserve`. The legal basis that BNetzA reports is kept in a separate `status_legal_basis` attribute and is not folded into the bucket.
