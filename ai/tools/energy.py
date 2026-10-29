"""Energy tools: metrics, a comparison of two periods, market and weather analyses.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

The four tools read only the energy marts and the semantic layer of the energy
server. Both analyses are descriptive: they state what the data shows and make no
claim about a cause. The generation mart holds a daily row and quarter-hour rows for
many days; the carrier totals use the daily row where both exist. Library only.
"""

from __future__ import annotations

from datetime import date

from pydantic import BaseModel, ConfigDict

from ai.tools import common as cm
from ai.tools import metrics as mt

SERVER = "energy_tools"
SCOPE = cm.load_scopes()[SERVER]
DOMAIN = mt.Domain("energy", SCOPE, mt.load_metrics())

PRICE_MART = "energy_analytics.market_price_daily"
GENERATION_MART = "energy_analytics.generation_by_carrier_daily"
SITE_MART = "energy_analytics.site_energy_daily"
WEATHER_MART = "energy_analytics.site_weather_daily"

PRICE_SQL = (
    "SELECT COUNT(*) AS days, "
    "SUM(m.mean_price_eur_per_mwh * m.interval_count) "
    "/ NULLIF(SUM(m.interval_count), 0) AS mean_price, "
    "MIN(m.min_price_eur_per_mwh) AS min_price, "
    "MAX(m.max_price_eur_per_mwh) AS max_price, "
    "AVG(m.price_volatility_eur_per_mwh) AS mean_volatility "
    f"FROM [[{PRICE_MART}]] m "
    "WHERE m.market_area_code = :area "
    "AND m.local_date BETWEEN :date_start AND :date_end"
)
CARRIER_SQL = (
    "SELECT carrier_key, SUM(generation_mwh) AS generation_mwh FROM ("
    "SELECT * FROM (SELECT t.*, ROW_NUMBER() OVER ("
    "PARTITION BY market_area_code, local_date, carrier_key "
    "ORDER BY resolution_seconds DESC) AS rn "
    f"FROM [[{GENERATION_MART}]] t "
    "WHERE t.market_area_code = :area "
    "AND t.local_date BETWEEN :date_start AND :date_end) WHERE rn = 1) "
    "GROUP BY carrier_key ORDER BY generation_mwh DESC"
)
ENERGY_SQL = (
    "SELECT m.subsystem AS subsystem, m.unit AS unit, "
    "COUNT(DISTINCT m.local_date) AS days, SUM(m.daily_energy) AS total_energy, "
    "AVG(m.daily_energy) AS mean_per_day, "
    "COUNT(DISTINCT m.location_key) AS locations "
    f"FROM [[{SITE_MART}]] m "
    "WHERE m.channel = 'total' AND (:loc = '*' OR m.location_key = :loc) "
    "AND m.local_date BETWEEN :date_start AND :date_end "
    "GROUP BY m.subsystem, m.unit ORDER BY m.subsystem"
)
TEMPERATURE_SQL = (
    "SELECT COUNT(DISTINCT m.local_date) AS days, "
    "AVG(m.mean_air_temperature_degc) AS mean_temp, "
    "MIN(m.min_air_temperature_degc) AS min_temp, "
    "MAX(m.max_air_temperature_degc) AS max_temp "
    f"FROM [[{WEATHER_MART}]] m "
    "WHERE (:loc = '*' OR m.location_key = :loc) "
    "AND m.local_date BETWEEN :date_start AND :date_end"
)
AREA = ("area", "date_start", "date_end")
LOCATION = ("loc", "date_start", "date_end")
PRICE_T = SCOPE.register(cm.SqlTemplate("price statistics", PRICE_SQL, AREA))
CARRIER_T = SCOPE.register(cm.SqlTemplate("carrier totals", CARRIER_SQL, AREA))
ENERGY_T = SCOPE.register(cm.SqlTemplate("site energy", ENERGY_SQL, LOCATION))
TEMPERATURE_T = SCOPE.register(cm.SqlTemplate("temperature", TEMPERATURE_SQL, LOCATION))


class PriceStats(BaseModel):
    model_config = ConfigDict(extra="forbid")

    days: int
    mean_eur_per_mwh: float | None
    min_eur_per_mwh: float | None
    max_eur_per_mwh: float | None
    mean_daily_volatility_eur_per_mwh: float | None


class CarrierShare(BaseModel):
    model_config = ConfigDict(extra="forbid")

    carrier_key: str
    generation_mwh: float
    share: float


class MarketConditions(BaseModel):
    model_config = ConfigDict(extra="forbid")

    market_area_code: str
    start: date
    end: date
    price: PriceStats | None
    carriers: list[CarrierShare]


class MarketConditionsResult(cm.TrustedToolResult):
    data: MarketConditions


class TemperatureStats(BaseModel):
    model_config = ConfigDict(extra="forbid")

    days: int
    mean_degc: float | None
    min_degc: float | None
    max_degc: float | None


class SubsystemEnergy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    subsystem: str
    unit: str
    days: int
    total_energy: float | None
    mean_per_day: float | None


class WeatherConditions(BaseModel):
    model_config = ConfigDict(extra="forbid")

    location_key: str
    start: date
    end: date
    locations: int
    temperature: TemperatureStats | None
    energy: list[SubsystemEnergy]


class WeatherConditionsResult(cm.TrustedToolResult):
    data: WeatherConditions


@cm.guarded
def get_metric(
    metric_key: str,
    filters: dict[str, str] | None = None,
    start: date | None = None,
    end: date | None = None,
    limit: int = cm.ROW_CAP,
) -> mt.MetricSeries:
    return DOMAIN.series(metric_key, filters, start, end, limit)


@cm.guarded
def compare_periods(
    metric_key: str,
    filters: dict[str, str] | None = None,
    a_start: date | None = None,
    a_end: date | None = None,
    b_start: date | None = None,
    b_end: date | None = None,
) -> mt.PeriodComparison:
    return DOMAIN.compare(metric_key, filters, (a_start, a_end), (b_start, b_end))


@cm.guarded
def analyse_market_conditions(
    market_area_code: str = "de_lu",
    start: date | None = None,
    end: date | None = None,
) -> MarketConditionsResult:
    mt.check_text("market_area_code", market_area_code)
    start, end, last = DOMAIN.span(PRICE_MART, start, end)
    params = {"area": market_area_code, "date_start": start, "date_end": end}
    warehouse = cm.get_warehouse()
    price_rows = warehouse.run(SCOPE, PRICE_T, params).rows
    carrier_rows = warehouse.run(SCOPE, CARRIER_T, params).rows
    price_row = price_rows[0] if price_rows else None
    price = None
    if price_row and price_row["days"]:
        price = PriceStats(
            days=int(price_row["days"]),
            mean_eur_per_mwh=mt.number(price_row["mean_price"]),
            min_eur_per_mwh=mt.number(price_row["min_price"]),
            max_eur_per_mwh=mt.number(price_row["max_price"]),
            mean_daily_volatility_eur_per_mwh=mt.number(price_row["mean_volatility"]),
        )
    total = sum(float(r["generation_mwh"] or 0) for r in carrier_rows)
    carriers = [
        CarrierShare(
            carrier_key=r["carrier_key"],
            generation_mwh=float(r["generation_mwh"] or 0),
            share=(float(r["generation_mwh"] or 0) / total) if total else 0.0,
        )
        for r in carrier_rows
    ]
    notes = [
        (
            "the daily row of the generation mart is used where the mart also "
            "holds quarter-hour rows"
        ),
        mt.TRUST_PENDING,
    ]
    if price is None:
        notes.append("no price rows for this market area and range")
    if not carriers:
        notes.append("no generation rows for this market area and range")
    return MarketConditionsResult(
        data=MarketConditions(
            market_area_code=market_area_code,
            start=start,
            end=end,
            price=price,
            carriers=carriers,
        ),
        source_tables=[PRICE_MART, GENERATION_MART],
        as_of=str(last),
        row_count=len(carriers) + (1 if price else 0),
        warnings=notes,
        trust=cm.Trust(status="unknown"),
    )


@cm.guarded
def analyse_weather_conditions(
    location_key: str = "*",
    start: date | None = None,
    end: date | None = None,
) -> WeatherConditionsResult:
    mt.check_text("location_key", location_key)
    start, end, last = DOMAIN.span(SITE_MART, start, end)
    params = {"loc": location_key, "date_start": start, "date_end": end}
    warehouse = cm.get_warehouse()
    energy_rows = warehouse.run(SCOPE, ENERGY_T, params).rows
    temp_rows = warehouse.run(SCOPE, TEMPERATURE_T, params).rows
    temp = temp_rows[0] if temp_rows else None
    temperature = None
    if temp and temp["days"]:
        temperature = TemperatureStats(
            days=int(temp["days"]),
            mean_degc=mt.number(temp["mean_temp"]),
            min_degc=mt.number(temp["min_temp"]),
            max_degc=mt.number(temp["max_temp"]),
        )
    energy = [
        SubsystemEnergy(
            subsystem=r["subsystem"],
            unit=r["unit"],
            days=int(r["days"]),
            total_energy=mt.number(r["total_energy"]),
            mean_per_day=mt.number(r["mean_per_day"]),
        )
        for r in energy_rows
    ]
    locations = max((int(r["locations"]) for r in energy_rows), default=0)
    notes = [
        (
            "energy is the 'total' channel of each subsystem; the other channels "
            "are parts of it"
        ),
        mt.TRUST_PENDING,
    ]
    if locations > 1:
        notes.append("more than one location is included; give a location_key")
    if not energy:
        notes.append("no energy rows for this location and range")
    return WeatherConditionsResult(
        data=WeatherConditions(
            location_key=location_key,
            start=start,
            end=end,
            locations=locations,
            temperature=temperature,
            energy=energy,
        ),
        source_tables=[SITE_MART, WEATHER_MART],
        as_of=str(last),
        row_count=len(energy) + (1 if temperature else 0),
        warnings=notes,
        trust=cm.Trust(status="unknown"),
    )
