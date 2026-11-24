"""Unit tests for scripts/ingestion/_energy_commerce_formats.py -- the listings, the
parsers and the BigQuery fetch of the energy and commerce stages in sources.yml.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: November 2026
"""

from __future__ import annotations

import gzip
import importlib.util
import json
import sys
import zipfile
from pathlib import Path

import pytest
import yaml

pytestmark = [pytest.mark.unit]

ROOT = Path(__file__).resolve().parents[2]
INGESTION_DIR = ROOT / "scripts" / "ingestion"
SOURCES_FILE = ROOT / "scripts" / "download" / "sources.yml"


@pytest.fixture(scope="module")
def formats():
    sys.path.insert(0, str(INGESTION_DIR))
    try:
        spec = importlib.util.spec_from_file_location(
            "_energy_commerce_formats", INGESTION_DIR / "_energy_commerce_formats.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    finally:
        sys.path.remove(str(INGESTION_DIR))
    return module


@pytest.fixture(scope="module")
def sources():
    return yaml.safe_load(SOURCES_FILE.read_text(encoding="utf-8"))


def _write_zip(path, members):
    with zipfile.ZipFile(path, "w") as archive:
        for name, content in members.items():
            archive.writestr(name, content)
    return path


def _first(parse, path, tag=""):
    return list(parse(path, tag))


def test_smard_tables_hold_every_series_of_the_old_download(sources):
    groups = sources["vars"]["smard_series"]
    counts = {(g["region"], g["resolution"]): len(g["filters"]) for g in groups}
    assert counts[("DE-LU", "day")] == 22
    assert counts[("DE-LU", "quarterhour")] == 18
    assert counts[("DE-LU", "hour")] == 15
    for area in ("50Hertz", "Amprion", "TenneT", "TransnetBW"):
        assert counts[(area, "day")] == 4
    recent = [g for g in groups if g["resolution"] in ("quarterhour", "hour")]
    assert {g["chunks"] for g in recent} == {104}
    assert all("chunks" not in g for g in groups if g["resolution"] == "day")


def test_smard_listing_keeps_the_last_chunks_and_skips_a_missing_series(
    formats, monkeypatch
):
    pages = {
        "https://s/5078/DE-LU/index_hour.json": json.dumps({"timestamps": [1, 2, 3]}),
        "https://s/9/DE-LU/index_hour.json": None,
    }
    monkeypatch.setattr(formats, "listing_text", lambda url: pages[url])
    monkeypatch.setattr(formats.time, "sleep", lambda seconds: None)
    variables = {
        "smard": "https://s",
        "smard_series": [
            {
                "region": "DE-LU",
                "resolution": "hour",
                "chunks": 2,
                "filters": {5078: "price", 9: "gone"},
            }
        ],
    }
    files = formats.smard_series("energy_timeseries", variables)
    assert files == [
        (
            "price|5078|DE-LU|hour",
            "https://s/5078/DE-LU/5078_DE-LU_hour_2.json",
            "price_5078_DE-LU_hour_2.json",
        ),
        (
            "price|5078|DE-LU|hour",
            "https://s/5078/DE-LU/5078_DE-LU_hour_3.json",
            "price_5078_DE-LU_hour_3.json",
        ),
    ]


def test_smard_chunk_becomes_rows_with_the_tag_columns(formats, tmp_path):
    path = tmp_path / "c.json"
    path.write_text(json.dumps({"series": [[0, 1.5], [3600000, None]]}), "utf-8")
    batches, read = _first(formats.parse_smard_chart, path, "price|5078|DE-LU|hour")[0]
    frame = batches["energy_timeseries"]
    assert list(frame.columns) == formats.SMARD_COLUMNS
    assert frame["timestamp_utc"].tolist() == [
        "1970-01-01T00:00:00+00:00",
        "1970-01-01T01:00:00+00:00",
    ]
    assert frame["filter_id"].tolist() == [5078, 5078]
    assert read == 2
    path.write_text(json.dumps({"series": []}), "utf-8")
    assert _first(formats.parse_smard_chart, path, "a|1|DE-LU|day") == [({}, 0)]


def test_dwd_stations_resolve_by_name_without_umlauts_and_need_a_current_file(
    formats, monkeypatch
):
    def station(station_id, name):
        return f"{station_id:<61}{name}"

    description = "\n".join(
        [
            "Stations_id von_datum bis_datum ... Stationsname Bundesland",
            "-" * 60,
            station("00001", "Koeln/Bonn          Nordrhein-Westfalen"),
            station("00002", "Nurnberg-Netzstall Bayern"),
            station("00003", "Nurnberg           Bayern"),
            station("00004", "Kiel-Holtenau      Schleswig-Holstein"),
        ]
    )
    active = " ".join(f"stundenwerte_TU_0000{n}_akt.zip" for n in (1, 2, 3))
    pages = {
        "https://d/air_temperature/recent/": active,
        "https://d/air_temperature/recent/TU_Stundenwerte_Beschreibung_Stationen.txt": (
            description
        ),
    }
    monkeypatch.setattr(formats, "listing_text", lambda url: pages.get(url))
    wanted = {
        "cologne": "Koeln/Bonn",
        "nuremberg": "Nurnberg ",
        "kiel": "Kiel-Holtenau",
    }
    found = formats.dwd_station_ids("https://d", wanted)
    # Kiel has no current file; "Nurnberg " does not match "Nurnberg-Netzstall".
    assert found == {"cologne": "00001", "nuremberg": "00003"}


def test_dwd_files_list_only_what_the_folders_list(formats, monkeypatch, tmp_path):
    monkeypatch.setattr(formats, "DATA_RAW_DIR", tmp_path)
    monkeypatch.setattr(
        formats, "dwd_station_ids", lambda base, wanted: {"berlin": "00433"}
    )
    pages = {
        "https://d/wind/recent/": "stundenwerte_FF_00433_akt.zip",
        "https://d/wind/historical/": "stundenwerte_FF_00433_19500101_20241231_hist.zip",
        "https://d/solar/": "stundenwerte_ST_00433_row.zip",
    }
    monkeypatch.setattr(formats, "listing_text", lambda url: pages.get(url))
    variables = {
        "dwd": "https://d",
        "dwd_stations": {"berlin": "Berlin-Tempelhof"},
        "dwd_categories": {"wind": "FF", "sun": "SD"},
        "dwd_solar_parameter": "ST",
    }
    files = formats.dwd_files("wind", variables)
    assert [(tag, name) for tag, _, name in files] == [
        ("berlin|wind|recent", "berlin_wind_recent.zip"),
        ("berlin|wind|historical", "berlin_wind_historical.zip"),
        ("berlin|solar|solar", "berlin_solar_solar.zip"),
    ]
    assert files[1][1].endswith("stundenwerte_FF_00433_19500101_20241231_hist.zip")


def test_dwd_files_use_an_extracted_folder_and_download_only_the_rest(
    formats, monkeypatch, tmp_path
):
    monkeypatch.setattr(formats, "DATA_RAW_DIR", tmp_path)
    folder = tmp_path / "dwd" / "berlin" / "wind" / "recent"
    folder.mkdir(parents=True)
    (folder / "produkt_ff.txt").write_text("x", encoding="utf-8")
    (tmp_path / "dwd" / "berlin" / "solar").mkdir()  # an empty folder is not data
    asked = []

    def listing(url):
        asked.append(url)
        return {
            "https://d/wind/historical/": (
                "stundenwerte_FF_00433_19500101_20241231_hist.zip"
            )
        }.get(url)

    monkeypatch.setattr(formats, "listing_text", listing)
    monkeypatch.setattr(
        formats, "dwd_station_ids", lambda base, wanted: {"berlin": "00433"}
    )
    variables = {
        "dwd": "https://d",
        "dwd_stations": {"berlin": "Berlin-Tempelhof"},
        "dwd_categories": {"wind": "FF"},
        "dwd_solar_parameter": "ST",
    }
    files = formats.dwd_files("wind", variables)
    assert files[0] == (
        "berlin|wind|recent",
        f"manual:{folder}",
        "berlin/wind/recent",
    )
    assert [tag for tag, _, _ in files] == [
        "berlin|wind|recent",
        "berlin|wind|historical",
    ]
    assert "https://d/wind/recent/" not in asked


def test_dwd_files_need_no_network_when_every_folder_is_local(
    formats, monkeypatch, tmp_path
):
    monkeypatch.setattr(formats, "DATA_RAW_DIR", tmp_path)
    for kind in ("recent", "historical"):
        folder = tmp_path / "dwd" / "berlin" / "wind" / kind
        folder.mkdir(parents=True)
        (folder / "produkt_ff.txt").write_text("x", encoding="utf-8")
    solar = tmp_path / "dwd" / "berlin" / "solar"
    solar.mkdir()
    (solar / "produkt_st.txt").write_text("x", encoding="utf-8")

    def no_network(*args):
        raise AssertionError("the network was asked")

    monkeypatch.setattr(formats, "listing_text", no_network)
    monkeypatch.setattr(formats, "dwd_station_ids", no_network)
    variables = {
        "dwd": "https://d",
        "dwd_stations": {"berlin": "Berlin-Tempelhof"},
        "dwd_categories": {"wind": "FF"},
        "dwd_solar_parameter": "ST",
    }
    assert len(formats.dwd_files("wind", variables)) == 3


def test_dwd_stage_datasets_are_the_categories_solar_and_the_metadata(sources, formats):
    entry = next(e for e in sources["stages"] if e["source"] == "dwd")
    expected = (
        set(sources["vars"]["dwd_categories"])
        | {"solar"}
        | set(formats.DWD_METADATA.values())
    )
    assert set(entry["datasets"]) == expected
    assert len(entry["datasets"]) == 20
    assert len(sources["vars"]["dwd_stations"]) == 28


DWD_MEMBERS = {
    "produkt_tu_stunde_1_2.txt": (
        "STATIONS_ID;MESS_DATUM;TT_TU;eor;\n433;2020010100;1.5;eor;\n"
    ),
    "Metadaten_Geographie_00433.txt": (
        "Stations_id;Stationshoehe;Stationsname;\n433;48;Berlin;\n"
    ),
    "Metadaten_Geraete_Lufttemperatur_00433.txt": (
        "Stations_id;Geraet;\n433;Thermometer;\n"
    ),
    "Metadaten_Fehlwerte_00433_2020.txt": (
        "Stations_id;Von;\n433;2020-01-01;\n"
        "generiert: 01.01.2021 -- Deutscher Wetterdienst --\n"
    ),
    "Metadaten_Fehldaten_00433_2020.txt": "Stations_id;Von;\n433;combined;\n",
    # a web copy of a metadata file is not data
    "Metadaten_Geographie_00433.html": "<html>Stations_id</html>",
}


def _dwd_zip(path):
    return _write_zip(path, DWD_MEMBERS)


def _dwd_folder(path):
    path.mkdir(parents=True)
    for name, content in DWD_MEMBERS.items():
        (path / name).write_text(content, encoding="latin-1")
    return path


def test_dwd_folder_reads_like_a_zip_and_skips_the_html_copies(formats, tmp_path):
    from_zip = _first(
        formats.dwd_zip(), _dwd_zip(tmp_path / "a.zip"), "berlin|air_temperature|recent"
    )
    from_folder = _first(
        formats.dwd_zip(), _dwd_folder(tmp_path / "a"), "berlin|air_temperature|recent"
    )
    assert [(sorted(b), n) for b, n in from_zip] == [
        (sorted(b), n) for b, n in from_folder
    ]
    names = [name for batch, _ in from_folder for name in batch]
    assert names.count("station_geography") == 1  # the .html copy is not read


def test_dwd_zip_stages_measurements_and_metadata_once_by_content(formats, tmp_path):
    parse = formats.dwd_zip()
    first = _first(parse, _dwd_zip(tmp_path / "a.zip"), "berlin|air_temperature|recent")
    datasets = {name for batch, _ in first for name in batch}
    assert datasets == {
        "air_temperature",
        "station_geography",
        "device_instrument",
        "missing_value_periods",
    }
    by_name = {name: frame for batch, _ in first for name, frame in batch.items()}
    assert by_name["air_temperature"].columns.tolist()[:3] == [
        "STATIONS_ID",
        "city",
        "MESS_DATUM",
    ]
    assert "Unnamed: 4" not in by_name["air_temperature"].columns
    assert by_name["device_instrument"]["device_category"].tolist() == [
        "Lufttemperatur"
    ]
    assert len(by_name["missing_value_periods"]) == 1  # the footer line is not a row
    second = _first(
        parse, _dwd_zip(tmp_path / "b.zip"), "berlin|air_temperature|recent"
    )
    again = {name: frame for batch, _ in second for name, frame in batch.items()}
    assert len(again["station_geography"]) == 0
    assert len(again["device_instrument"]) == 0
    assert len(again["air_temperature"]) == 1  # measurements are never deduplicated
    assert len(again["missing_value_periods"]) == 1


def test_dwd_zip_of_solar_reads_the_measurement_only(formats, tmp_path):
    path = _dwd_zip(tmp_path / "s.zip")
    batches = _first(formats.dwd_zip(), path, "berlin|solar|solar")
    assert {name for batch, _ in batches for name in batch} == {"solar"}


def test_mastr_batches_share_the_union_of_the_field_names(formats, tmp_path):
    shard_one = "<r><e><A>1</A></e><e><A>2</A><B>x</B></e></r>"
    shard_two = "<r><e><C>z</C></e></r>"
    path = _write_zip(
        tmp_path / "m.zip",
        {
            "EinheitenSolar_1.xml": shard_one,
            "EinheitenSolar_2.xml": shard_two,
            "Other_1.xml": "<r><e><Q>1</Q></e></r>",
        },
    )
    parse = formats.mastr_xml({"EinheitenSolar": "einheiten_solar", "Missing": "x"})
    batches = _first(parse, path, "20260904")
    assert len(batches) == 1
    frame = batches[0][0]["einheiten_solar"]
    assert list(frame.columns) == ["A", "B", "C"]
    assert len(frame) == 3
    assert batches[0][1] == 3


def test_mastr_export_link_is_read_from_the_page(formats, monkeypatch):
    page = (
        '<a href="https://download.marktstammdatenregister.de/'
        'Gesamtdatenexport_20260904_26.1.zip">x</a>'
    )
    monkeypatch.setattr(formats, "listing_text", lambda url: page)
    files = formats.mastr_export("einheiten_solar", {"mastr_page": "https://m"})
    assert files == [
        (
            "20260904",
            "https://download.marktstammdatenregister.de/Gesamtdatenexport_20260904_26.1.zip",
            "Gesamtdatenexport_20260904_26.1.zip",
        )
    ]
    monkeypatch.setattr(formats, "listing_text", lambda url: "no link")
    with pytest.raises(RuntimeError):
        formats.mastr_export("einheiten_solar", {"mastr_page": "https://m"})


def test_power_plant_header_is_found_by_content_and_footnotes_are_dropped(
    formats, tmp_path
):
    path = tmp_path / "k.csv"
    path.write_text(
        "Kraftwerksliste;;;\n"
        "Datensatztyp;EinheitMastrNummer;Anlagenbetreiber*;Energietraeger\n"
        "Kraftwerk;SEE1;Betreiber A;Wind\n"
        "Stand 01.01.2026;;;\n",
        encoding="latin-1",
    )
    batches, read = _first(formats.parse_power_plant_list, path, "kraftwerksliste")[0]
    frame = batches["kraftwerksliste"]
    assert list(frame.columns) == [
        "Datensatztyp",
        "EinheitMastrNummer",
        "Anlagenbetreiber",
        "Energietraeger",
    ]
    assert frame["EinheitMastrNummer"].tolist() == ["SEE1"]
    assert read == 1


def test_power_plant_file_without_a_header_row_fails(formats, tmp_path):
    path = tmp_path / "k.csv"
    path.write_text("a;b\n1;2\n", encoding="latin-1")
    with pytest.raises(RuntimeError):
        formats.power_plant_table(path)


def test_redispatch_archive_gets_the_regime_column_first(formats, tmp_path):
    path = _write_zip(tmp_path / "r.zip", {"measures.csv": "﻿a;b\n1;2\n"})
    batches, read = _first(formats.parse_redispatch, path, "archive_2013_2020")[0]
    frame = batches["redispatch_measures"]
    assert list(frame.columns) == ["coverage_regime", "a", "b"]
    assert frame["coverage_regime"].tolist() == ["archive_2013_2020"]
    assert read == 1


def test_ga4_event_keeps_the_allowlists_and_flattens_the_scalars(formats):
    row = {
        "event_name": "purchase",
        "event_date": "20210101",
        "user_pseudo_id": "u",
        "event_params": [{"key": "ga_session_id"}, {"key": "debug_mode"}],
        "ecommerce": {"transaction_id": "t1", "purchase_revenue": 5.0, "other": 1},
        "items": [{"item_id": "i", "coupon": "x"}, {"coupon": "y"}],
        "geo": {"country": "US", "region": "CA"},
        "device": {"category": "mobile", "web_info": {"browser": "Chrome"}},
        "traffic_source": {"source": "google", "medium": "organic"},
        "platform": "WEB",
    }
    kept = formats.project_ga4_event(row)
    assert kept["event_params"] == [{"key": "ga_session_id"}]
    assert kept["ecommerce"] == {"transaction_id": "t1", "purchase_revenue": 5.0}
    assert kept["items"] == [{"item_id": "i"}]
    assert kept["geo_country"] == "US"
    assert kept["geo_region"] == "CA"
    assert kept["device_browser"] == "Chrome"
    assert kept["traffic_medium"] == "organic"
    assert kept["platform"] == "WEB"
    assert "geo_city" not in kept
    assert formats.project_ga4_event({"event_name": "page_view"}) is None


def test_ga4_day_file_yields_retained_records_and_counts_every_line(formats, tmp_path):
    path = tmp_path / "events_20210101.jsonl"
    path.write_text(
        '{"event_name": "purchase", "event_date": "20210101"}\n'
        '{"event_name": "page_view"}\n'
        "not json\n"
        "\n",
        encoding="utf-8",
    )
    batches, read = _first(formats.parse_ga4_events, path, "20210101")[0]
    assert [r["event_name"] for r in batches["events"]] == ["purchase"]
    assert read == 3


def test_ga4_day_files_in_raw_are_listed_without_asking_bigquery(
    formats, monkeypatch, tmp_path
):
    monkeypatch.setattr(formats, "DATA_RAW_DIR", tmp_path)
    raw = tmp_path / "ga4"
    raw.mkdir()
    for name in ("events_20201102.jsonl", "events_20201101.jsonl", "notes.txt"):
        (raw / name).write_text("{}", encoding="utf-8")

    def no_client():
        raise AssertionError("BigQuery was asked")

    monkeypatch.setattr(formats, "bigquery_client", no_client)
    variables = {"ga4_project": "p", "ga4_dataset": "d"}
    assert formats.ga4_tables("events", variables) == [
        ("20201101", "bigquery:p.d.events_20201101", "events_20201101.jsonl"),
        ("20201102", "bigquery:p.d.events_20201102", "events_20201102.jsonl"),
    ]


def test_bigquery_fetch_does_not_fetch_a_day_file_that_exists(
    formats, monkeypatch, tmp_path
):
    def no_client():
        raise AssertionError("BigQuery was asked")

    monkeypatch.setattr(formats, "bigquery_client", no_client)
    dest = tmp_path / "events_20201101.jsonl"
    dest.write_text('{"a": 1}\n', encoding="utf-8")
    facts = formats.fetch_bigquery("bigquery:p.d.events_20201101", dest, 60)
    assert facts == {"bytes": dest.stat().st_size, "last_modified": ""}
    assert dest.read_text(encoding="utf-8") == '{"a": 1}\n'


def test_ga4_tables_are_listed_as_bigquery_urls(formats, monkeypatch, tmp_path):
    pytest.importorskip("google.cloud.bigquery")
    monkeypatch.setattr(formats, "DATA_RAW_DIR", tmp_path)
    ids = ("events_20210102", "events_intraday_1", "events_20210101", "x")
    tables = [type("T", (), {"table_id": i}) for i in ids]
    client = type("C", (), {"list_tables": lambda self, reference: tables})()
    monkeypatch.setattr(formats, "bigquery_client", lambda: client)
    variables = {"ga4_project": "p", "ga4_dataset": "d"}
    assert formats.ga4_tables("events", variables) == [
        ("20210101", "bigquery:p.d.events_20210101", "events_20210101.jsonl"),
        ("20210102", "bigquery:p.d.events_20210102", "events_20210102.jsonl"),
    ]


def test_bigquery_fetch_writes_json_lines_and_returns_the_size(
    formats, monkeypatch, tmp_path
):
    def row(values):
        return type("R", (), {"items": lambda self: values.items()})()

    rows = [row({"a": 1}), row({"a": 2})]
    queries = []

    class Job:
        def result(self):
            return rows

    class Client:
        def query(self, sql, location):
            queries.append((sql, location))
            return Job()

    monkeypatch.setattr(formats, "bigquery_client", lambda: Client())
    dest = tmp_path / "raw" / "t.jsonl"
    facts = formats.fetch_bigquery("bigquery:p.d.events_1", dest, 60)
    assert queries == [("SELECT * FROM `p.d.events_1`", "US")]
    assert dest.read_text(encoding="utf-8").splitlines() == ['{"a": 1}', '{"a": 2}']
    assert facts["bytes"] == dest.stat().st_size


def test_events_csv_sets_the_columns_by_name(formats, tmp_path):
    path = tmp_path / "e.csv"
    path.write_text("b,a\n2,1\n", encoding="utf-8")
    parse = formats.events_csv("events", ["a", "b", "c"])
    batches, read = _first(parse, path)[0]
    assert list(batches["events"].columns) == ["a", "b", "c"]
    assert batches["events"].iloc[0]["a"] == 1
    assert batches["events"]["c"].isna().all()
    assert read == 1


def test_honda_file_gets_its_frequency_and_the_dataset_column_order(formats, tmp_path):
    path = tmp_path / "weather.csv.gz"
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        handle.write("datetime_utc,Igm,Ta\n2024-01-01,5,1\n")
    parse = formats.honda_iot({"weather": ["frequency", "datetime_utc", "Ta", "Igm"]})
    batches, read = _first(parse, path, "15min:weather")[0]
    frame = batches["weather"]
    assert list(frame.columns) == ["frequency", "datetime_utc", "Ta", "Igm"]
    assert frame.iloc[0].tolist() == ["15min", "2024-01-01", 1, 5]
    assert read == 1


def test_honda_stage_lists_every_dataset_for_each_frequency(sources):
    entry = next(e for e in sources["stages"] if e["source"] == "honda_iot")
    assert set(entry["parser"]["columns"]) == set(entry["datasets"])
    tags = [
        item["tag"].format(item=name)
        for item in entry["files"]
        for name in item["each"]
    ]
    assert len(tags) == 21
    assert {tag.split(":")[0] for tag in tags} == {"1min", "15min", "1h"}
