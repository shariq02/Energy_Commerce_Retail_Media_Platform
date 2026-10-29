"""Metric bindings and the metric tools of the energy and commerce servers.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Importing `energy` and `commerce` registers every template with the scope of its
server, so a template that leaves its scope fails here. The warehouse is a double.
"""

from __future__ import annotations

from datetime import date

import pytest

from ai.tools import commerce, energy
from ai.tools import common as cm
from ai.tools import metrics as mt
from tests.ai._fake_warehouse import FakeWarehouse

pytestmark = [pytest.mark.unit, pytest.mark.ai]

BOUNDS = [{"first_date": date(2018, 10, 1), "last_date": date(2026, 9, 5)}]


@pytest.fixture(autouse=True)
def reset_warehouse():
    yield
    cm.set_warehouse(None)


@pytest.fixture
def fresh_energy():
    """An energy domain with an empty bounds cache."""
    return mt.Domain("energy", energy.SCOPE, mt.load_metrics())


def install(answers):
    fake = FakeWarehouse(answers)
    cm.set_warehouse(fake)
    return fake


def series_answers(rows=None):
    return {
        "bounds energy_analytics.market_price_daily": BOUNDS,
        "average_market_price series": rows
        or [
            {
                "local_date": date(2026, 9, 5),
                "market_area_code": "de_lu",
                "value": 61.5,
            }
        ],
    }


# --- bindings -------------------------------------------------------------------


def test_the_bindings_hold_five_energy_and_three_commerce_metrics():
    bindings = mt.load_metrics()
    assert set(bindings["energy"]) == {
        "generation_by_carrier",
        "average_market_price",
        "price_volatility",
        "site_energy_consumption",
        "site_energy_weather_context",
    }
    assert set(bindings["commerce"]) == {
        "ga4_sessions",
        "ga4_revenue",
        "rees46_event_demand",
    }


def test_every_mart_belongs_to_its_ecosystem():
    for ecosystem, items in mt.load_metrics().items():
        for metric in items.values():
            assert metric.schema == f"{ecosystem}_analytics"


def test_the_site_metrics_default_to_the_total_channel():
    metric = mt.load_metrics()["energy"]["site_energy_consumption"]
    assert metric.default_filters == {"channel": "total"}


def test_the_generation_metric_takes_the_daily_row():
    metric = mt.load_metrics()["energy"]["generation_by_carrier"]
    sql = mt.series_sql(metric)
    assert "ROW_NUMBER() OVER (PARTITION BY market_area_code, local_date" in sql
    assert "ORDER BY t.resolution_seconds DESC" in sql and "WHERE rn = 1" in sql


def test_the_price_metric_is_a_weighted_mean_over_a_period():
    metric = mt.load_metrics()["energy"]["average_market_price"]
    sql = mt.period_sql(metric)
    assert "SUM(m.mean_price_eur_per_mwh * m.interval_count)" in sql
    assert "NULLIF(SUM(m.interval_count), 0)" in sql and "UNION ALL" in sql


def test_a_filter_is_a_parameter_with_the_all_marker():
    metric = mt.load_metrics()["commerce"]["rees46_event_demand"]
    sql = mt.series_sql(metric)
    assert "(:f_category_l1 = '*' OR m.category_l1 = :f_category_l1)" in sql
    assert mt.filter_names(metric) == ("f_category_l1", "f_event_type")


def test_every_template_is_registered_with_its_server():
    for domain in (energy.DOMAIN, commerce.DOMAIN):
        for key in domain.metrics:
            assert domain.series_templates[key].params[:2] == (
                "range_start",
                "range_end",
            )
            assert domain.period_templates[key].params[:4] == (
                "a_start",
                "a_end",
                "b_start",
                "b_end",
            )


GOOD = """
energy:
  m:
    mart: energy_analytics.t
    value: v
    unit: u
    period: sum
    dimensions: [d]
    source_system: s
"""


def write(tmp_path, text):
    path = tmp_path / "metrics.yml"
    path.write_text(text, encoding="utf-8")
    return path


def test_a_clean_binding_file_loads(tmp_path):
    assert set(mt.load_metrics(write(tmp_path, GOOD))["energy"]) == {"m"}


@pytest.mark.parametrize(
    "old, new",
    [
        ("energy_analytics.t", "commerce_analytics.t"),
        ("period: sum", "period: median"),
        ("period: sum", "period: weighted_mean"),
        ("value: v", "value: v; drop"),
        ("dimensions: [d]", "dimensions: [Bad-Name]"),
        ("source_system: s", "source_system: s\n    default_filters: {x: y}"),
        ("source_system: s", "source_system: s\n    prefer_largest: v"),
    ],
)
def test_a_bad_binding_is_refused(tmp_path, old, new):
    with pytest.raises(cm.ScopeError):
        mt.load_metrics(write(tmp_path, GOOD.replace(old, new)))


# --- series ---------------------------------------------------------------------


def test_a_series_without_dates_uses_the_last_thirty_days_of_the_mart(fresh_energy):
    fake = install(series_answers())
    result = fresh_energy.series("average_market_price", None, None, None, 200)
    name, params, cap = fake.calls[1]
    assert name == "average_market_price series" and cap == 200
    assert params["range_end"] == date(2026, 9, 5)
    assert params["range_start"] == date(2026, 8, 7)
    assert params["f_market_area_code"] == "*"
    assert result.as_of == "2026-09-05" and result.unit == "EUR/MWh"
    assert result.data[0].dimensions == {"market_area_code": "de_lu"}
    assert result.source_tables == ["energy_analytics.market_price_daily"]


def test_a_series_carries_an_unknown_trust_and_says_so(fresh_energy):
    install(series_answers())
    result = fresh_energy.series("average_market_price", None, None, None, 10)
    assert result.trust.status == "unknown"
    assert mt.TRUST_PENDING in result.warnings


def test_the_range_of_a_mart_is_read_once(fresh_energy):
    fake = install(series_answers())
    fresh_energy.series("average_market_price", None, None, None, 10)
    fresh_energy.series("average_market_price", None, None, None, 10)
    assert fake.names().count("bounds energy_analytics.market_price_daily") == 1


def test_a_filter_value_goes_in_as_a_parameter(fresh_energy):
    fake = install(series_answers())
    fresh_energy.series(
        "average_market_price", {"market_area_code": "de_lu"}, None, None, 10
    )
    assert fake.calls[1][1]["f_market_area_code"] == "de_lu"


def test_the_total_channel_is_the_default_and_a_star_shows_all():
    domain = mt.Domain("energy", energy.SCOPE, mt.load_metrics())
    metric = domain.metric("site_energy_consumption")
    params, notes = domain.filter_params(metric, None)
    assert params["f_channel"] == "total" and params["f_subsystem"] == "*"
    assert any("channel is filtered to 'total'" in n for n in notes)
    params, notes = domain.filter_params(metric, {"channel": "*"})
    assert params["f_channel"] == "*" and not notes


def test_a_metric_of_the_other_ecosystem_is_refused_not_answered(fresh_energy):
    fake = install(series_answers())
    with pytest.raises(cm.ToolFailure) as info:
        fresh_energy.series("ga4_sessions", None, None, None, 10)
    assert info.value.code == "wrong_ecosystem" and fake.calls == []
    other = mt.Domain("commerce", commerce.SCOPE, mt.load_metrics())
    with pytest.raises(cm.ToolFailure) as info:
        other.series("average_market_price", None, None, None, 10)
    assert info.value.code == "wrong_ecosystem"


def test_an_unknown_metric_is_refused(fresh_energy):
    install(series_answers())
    with pytest.raises(cm.ToolFailure) as info:
        fresh_energy.series("nothing", None, None, None, 10)
    assert info.value.code == "unknown_metric"
    assert "average_market_price" in info.value.hint


@pytest.mark.parametrize(
    "filters",
    [{"nope": "x"}, {"market_area_code": "de_lu; DROP"}, {"market_area_code": ""}],
)
def test_a_bad_filter_is_refused(fresh_energy, filters):
    install(series_answers())
    with pytest.raises(cm.ToolFailure) as info:
        fresh_energy.series("average_market_price", filters, None, None, 10)
    assert info.value.code == "invalid_argument"


@pytest.mark.parametrize(
    "start, end, code",
    [
        (date(2026, 1, 1), None, "invalid_argument"),
        (date(2026, 2, 1), date(2026, 1, 1), "invalid_argument"),
        (date(2030, 1, 1), date(2030, 2, 1), "range_outside_data"),
        (date(2010, 1, 1), date(2010, 2, 1), "range_outside_data"),
    ],
)
def test_a_bad_range_is_refused(fresh_energy, start, end, code):
    install(series_answers())
    with pytest.raises(cm.ToolFailure) as info:
        fresh_energy.series("average_market_price", None, start, end, 10)
    assert info.value.code == code


@pytest.mark.parametrize("limit", [0, cm.ROW_CAP_MAX + 1])
def test_a_limit_outside_the_range_is_refused(fresh_energy, limit):
    install(series_answers())
    with pytest.raises(cm.ToolFailure) as info:
        fresh_energy.series("average_market_price", None, None, None, limit)
    assert info.value.code == "invalid_argument"


def test_a_cut_result_is_marked_and_explained(fresh_energy):
    rows = [
        {"local_date": date(2026, 9, 5), "market_area_code": "de_lu", "value": i}
        for i in range(5)
    ]
    install(series_answers(rows))
    result = fresh_energy.series("average_market_price", None, None, None, 3)
    assert result.truncated and result.row_count == 3
    assert any("cut at 3 rows" in w for w in result.warnings)


def test_an_empty_answer_is_stated(fresh_energy):
    install({**series_answers(), "average_market_price series": []})
    result = fresh_energy.series("average_market_price", None, None, None, 10)
    assert result.data == [] and any("no rows" in w for w in result.warnings)


def test_the_generation_series_names_the_daily_row_rule(fresh_energy):
    answers = {
        "bounds energy_analytics.generation_by_carrier_daily": BOUNDS,
        "generation_by_carrier series": [],
    }
    install(answers)
    result = fresh_energy.series("generation_by_carrier", None, None, None, 10)
    assert any("daily row is used" in w for w in result.warnings)


# --- periods --------------------------------------------------------------------


def period_answers(a, b, days=(30, 30)):
    return {
        "bounds energy_analytics.market_price_daily": BOUNDS,
        "average_market_price periods": [
            {"period": "a", "value": a, "days": days[0]},
            {"period": "b", "value": b, "days": days[1]},
        ],
    }


def test_two_periods_without_dates_are_the_last_thirty_days_and_the_thirty_before(
    fresh_energy,
):
    fake = install(period_answers(50.0, 60.0))
    result = fresh_energy.compare(
        "average_market_price", None, (None, None), (None, None)
    )
    params = fake.calls[1][1]
    assert params["b_end"] == date(2026, 9, 5) and params["b_start"] == date(2026, 8, 7)
    assert params["a_end"] == date(2026, 8, 6) and params["a_start"] == date(2026, 7, 8)
    assert result.change == pytest.approx(10.0)
    assert result.relative_change == pytest.approx(0.2)
    assert result.aggregation == "weighted_mean"
    assert [item.period for item in result.data] == ["a", "b"]


def test_a_zero_first_period_has_no_relative_change(fresh_energy):
    install(period_answers(0.0, 5.0))
    result = fresh_energy.compare(
        "average_market_price", None, (None, None), (None, None)
    )
    assert result.change == 5.0 and result.relative_change is None


def test_a_missing_period_value_gives_no_change(fresh_energy):
    install(period_answers(None, 5.0, days=(0, 30)))
    result = fresh_energy.compare(
        "average_market_price", None, (None, None), (None, None)
    )
    assert result.change is None and result.data[0].days == 0


def test_two_empty_periods_are_stated(fresh_energy):
    install(period_answers(None, None, days=(0, 0)))
    result = fresh_energy.compare(
        "average_market_price", None, (None, None), (None, None)
    )
    assert any("no rows in either period" in w for w in result.warnings)


def test_some_dates_of_the_periods_are_refused(fresh_energy):
    install(period_answers(1.0, 2.0))
    with pytest.raises(cm.ToolFailure) as info:
        fresh_energy.compare(
            "average_market_price", None, (date(2026, 1, 1), None), (None, None)
        )
    assert info.value.code == "invalid_argument"
    with pytest.raises(cm.ToolFailure):
        fresh_energy.compare(
            "average_market_price",
            None,
            (date(2026, 2, 1), date(2026, 1, 1)),
            (date(2026, 3, 1), date(2026, 3, 2)),
        )


def test_a_mean_metric_says_how_a_period_is_built(fresh_energy):
    answers = {
        "bounds energy_analytics.market_price_daily": BOUNDS,
        "price_volatility periods": [
            {"period": "a", "value": 1.0, "days": 30},
            {"period": "b", "value": 2.0, "days": 30},
        ],
    }
    install(answers)
    result = fresh_energy.compare("price_volatility", None, (None, None), (None, None))
    assert any("mean of the daily values" in w for w in result.warnings)


# --- the check against the live definitions -------------------------------------


def test_the_binding_check_finds_a_missing_and_a_changed_metric(fresh_energy):
    live = [
        {"metric_key": "average_market_price", "source_mart": "other_table"},
        {"metric_key": "price_volatility", "source_mart": "market_price_daily"},
        {"metric_key": "extra_metric", "source_mart": "x"},
    ]
    install({"metric definitions": live})
    problems = fresh_energy.verify_bindings()
    assert any("extra_metric: in metric_definition" in p for p in problems)
    assert any("generation_by_carrier: in metrics.yml" in p for p in problems)
    changed = "average_market_price: mart market_price_daily differs"
    assert any(changed in p for p in problems)


def test_the_binding_check_passes_when_keys_and_marts_match(fresh_energy):
    live = [
        {"metric_key": key, "source_mart": metric.table}
        for key, metric in fresh_energy.metrics.items()
    ]
    install({"metric definitions": live})
    assert fresh_energy.verify_bindings() == []
