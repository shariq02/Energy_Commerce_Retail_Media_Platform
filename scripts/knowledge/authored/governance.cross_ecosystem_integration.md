# Governance: cross-ecosystem integration constraint

Cross-ecosystem integration is allowed only through one of three routes:
1. the conformed dimensions `dim_date`, `dim_time`, `dim_geography` and `dim_weather_context`;
2. a shared, externally governed standard identifier that both ecosystems' sources carry;
3. a real infrastructure relationship that the source data encodes.

Identity or entity joins across ecosystems are prohibited. Every cross-ecosystem output is labelled an observed contextual relationship. It is not identity and it is not causation. A join that none of the three routes can express is not built. "No relationship" is preferred to a fabricated one.

The layer-transition gates check that no Gold table and no cross-ecosystem mart holds a join key that resolves an entity across ecosystems. They also check that every derived `canonical_id` in Gold carries exactly one `ecosystem` value.
