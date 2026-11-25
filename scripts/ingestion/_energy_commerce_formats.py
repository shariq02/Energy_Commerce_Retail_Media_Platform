# Energy and commerce source formats
# ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
# Author: Sharique Mohammad
# Date: November 2026
#
# Purpose: the file listings, the parsers and the BigQuery fetch of the energy and
# commerce stages of scripts/download/sources.yml (SMARD, DWD, MaStR, power plant
# list, redispatch, VG250 municipality points, GA4, REES46, Honda IoT), for
# download_sources.py.

import io
import json
import re
import sys
import time
import unicodedata
import xml.etree.ElementTree as ET
import zipfile
from contextlib import ExitStack
from datetime import UTC, datetime
from functools import cache
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "download"))

from _download_common import MANUAL

from config import DATA_RAW_DIR, get_logger

logger = get_logger(__name__)

REQUEST_TIMEOUT = 60
SMARD_DELAY_SECONDS = 0.5
SMARD_COLUMNS = [
    "metric",
    "filter_id",
    "region",
    "resolution",
    "timestamp_utc",
    "value",
]

# DWD files are Latin-1 (umlauts); the first line of a station file names the columns.
DWD_ENCODING = "latin-1"
DWD_CHUNK_ROWS = 100_000
DWD_STATION_CATEGORY = ("air_temperature", "TU")
# Metadata file prefix -> dataset. A station-level file is repeated in every
# measurement folder of a station, so the rows are kept once by content.
DWD_METADATA = {
    "Metadaten_Geographie_": "station_geography",
    "Metadaten_Stationsname_Betreibername_": "station_name_history",
    "Metadaten_Geraete_": "device_instrument",
    "Metadaten_Parameter_": "parameter_unit",
    "Metadaten_Fehlwerte_": "missing_value_periods",
}
DWD_DEDUPED = {
    "station_geography",
    "station_name_history",
    "device_instrument",
    "parameter_unit",
}
# The zip name of a station in a folder listing; `recent` and `solar` hold one file
# per station, `historical` has the date range in the name.
DWD_FILE_PATTERNS = {
    "recent": r"stundenwerte_{param}_{id}_akt\.zip",
    "historical": r"stundenwerte_{param}_{id}_\d{{8}}_\d{{8}}_hist\.zip",
    "solar": r"stundenwerte_{param}_{id}_row\.zip",
}

MASTR_BATCH_ROWS = 50_000

# The BNetzA CSV puts a variable number of title and disclaimer rows above the real
# header (one is a quoted cell spanning several lines) and footnotes below the data,
# so the header row is found by its content.
POWER_PLANT_ENCODING = "latin-1"
POWER_PLANT_HEADER_TOKENS = (
    "datensatztyp",
    "einheitmastrnummer",
    "anlagenbetreiber",
    "energietraeger",
    "nettonennleistung",
    "kraftwerksstatus",
    "bruttoleistung",
)
POWER_PLANT_SCAN_ROWS = 30
CAPACITY_ADDITION_COLUMNS = [
    "energietraeger",
    "2026",
    "2027",
    "2028",
    "2029",
    "2026_2029_total",
]

# The VG250 workbook sheet with the municipality points and the columns that Silver
# reads; the header row is found by its content.
VG250_SHEET = "VG250_PK"
VG250_COLUMNS = ("AGS", "LON_DEZ", "LAT_DEZ")
VG250_SCAN_ROWS = 10

EVENTS_CHUNK_ROWS = 300_000
HONDA_CHUNK_ROWS = 100_000

# Retained GA4 funnel events: the other events carry no item or ecommerce signal.
GA4_EVENTS = frozenset(
    {
        "view_item",
        "view_item_list",
        "select_item",
        "view_promotion",
        "select_promotion",
        "view_search_results",
        "add_to_cart",
        "begin_checkout",
        "add_shipping_info",
        "add_payment_info",
        "purchase",
    }
)
GA4_PARAM_KEYS = frozenset(
    {
        "ga_session_id",
        "ga_session_number",
        "page_location",
        "page_title",
        "search_term",
        "unique_search_term",
    }
)
GA4_ECOMMERCE_FIELDS = (
    "transaction_id",
    "purchase_revenue",
    "unique_items",
    "total_item_quantity",
    "tax_value",
    "shipping_value",
    "refund_value",
)
# Scalar fields flattened out of the raw record: staged name -> path in the record.
GA4_SCALARS = {
    "geo_country": ("geo", "country"),
    "geo_region": ("geo", "region"),
    "geo_city": ("geo", "city"),
    "platform": ("platform",),
    "device_category": ("device", "category"),
    "device_operating_system": ("device", "operating_system"),
    "device_browser": ("device", "web_info", "browser"),
    "traffic_source": ("traffic_source", "source"),
    "traffic_medium": ("traffic_source", "medium"),
    "traffic_campaign": ("traffic_source", "name"),
}
GA4_ITEM_FIELDS = (
    "item_id",
    "item_name",
    "item_category",
    "price",
    "quantity",
    "item_revenue",
)
GA4_BATCH_ROWS = 50_000


# --- file listings: listing(dataset, variables) -> [(tag, url, file name)]


def listing_text(url: str) -> str | None:
    """The page at `url`; None when it does not exist."""
    response = requests.get(url, timeout=REQUEST_TIMEOUT)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.text


def smard_series(dataset: str, variables: dict) -> list[tuple[str, str, str]]:
    """One file per weekly chunk of each series of `smard_series`. The tag holds the
    metric, filter, region and resolution that the chunk file does not."""
    base = variables["smard"]
    files = []
    for group in variables["smard_series"]:
        region, resolution = group["region"], group["resolution"]
        for filter_id, metric in group["filters"].items():
            page = listing_text(f"{base}/{filter_id}/{region}/index_{resolution}.json")
            time.sleep(SMARD_DELAY_SECONDS)
            if page is None:
                logger.warning(f"No SMARD series for filter {filter_id} in {region}")
                continue
            stamps = json.loads(page)["timestamps"]
            if group.get("chunks"):
                stamps = stamps[-group["chunks"] :]
            tag = "|".join([metric, str(filter_id), region, resolution])
            stem = f"{filter_id}_{region}_{resolution}"
            files += [
                (
                    tag,
                    f"{base}/{filter_id}/{region}/{stem}_{stamp}.json",
                    f"{metric}_{stem}_{stamp}.json",
                )
                for stamp in stamps
            ]
    return files


def dwd_ascii(text: str) -> str:
    """Text without diacritics, lower case (station names match without umlauts)."""
    folded = unicodedata.normalize("NFKD", text).encode("ascii", "ignore")
    return folded.decode("ascii").lower()


def dwd_station_ids(base: str, wanted: dict[str, str]) -> dict[str, str]:
    """Station key -> DWD station id, for the stations of `wanted` (key -> name part)
    that have a current file; the station ids are the same in every category."""
    category, param = DWD_STATION_CATEGORY
    recent = listing_text(f"{base}/{category}/recent/") or ""
    active = set(re.findall(rf"stundenwerte_{param}_(\d{{5}})_akt\.zip", recent))
    description = listing_text(
        f"{base}/{category}/recent/{param}_Stundenwerte_Beschreibung_Stationen.txt"
    )
    resolved: dict[str, str] = {}
    for line in (description or "").splitlines()[2:]:
        station_id = line[0:5].strip()
        if not line.strip() or station_id not in active:
            continue
        name = dwd_ascii(line[61:].strip() if len(line) > 61 else "")
        for key, part in wanted.items():
            if key not in resolved and dwd_ascii(part) in name:
                resolved[key] = station_id
    missing = set(wanted) - set(resolved)
    if missing:
        logger.warning(f"Could not resolve DWD station ids for: {sorted(missing)}")
    return resolved


def dwd_local_folder(key: str, category: str, kind: str) -> Path:
    """Where an extracted DWD zip lies under data/raw/dwd (solar has no sub-folder)."""
    base = DATA_RAW_DIR / "dwd" / key
    return base / "solar" if kind == "solar" else base / category / kind


def dwd_files(dataset: str, variables: dict) -> list[tuple[str, str, str]]:
    """The recent and historical files of each station and category, and the solar
    file. An extracted folder that is in data/raw/dwd is used as it is (a local
    folder is listed like a manual file); only the missing ones are downloaded."""
    categories = variables["dwd_categories"]
    wanted = [
        (key, category, param, kind)
        for kind in ("recent", "historical")
        for category, param in categories.items()
        for key in variables["dwd_stations"]
    ] + [
        (key, "solar", variables["dwd_solar_parameter"], "solar")
        for key in variables["dwd_stations"]
    ]
    files: list[tuple[str, str, str]] = []
    missing = []
    for key, category, param, kind in wanted:
        folder = dwd_local_folder(key, category, kind)
        if folder.is_dir() and any(folder.iterdir()):
            name = folder.relative_to(DATA_RAW_DIR / "dwd").as_posix()
            files.append((f"{key}|{category}|{kind}", f"{MANUAL}{folder}", name))
        else:
            missing.append((key, category, param, kind))
    logger.info(f"DWD: {len(files)} local folders, {len(missing)} to download")
    return files + dwd_downloads(variables, missing) if missing else files


def dwd_downloads(variables: dict, missing: list[tuple]) -> list[tuple[str, str, str]]:
    """The zip files of the (station, category, parameter, kind) items in `missing`,
    taken from the folder listings (a file that is not listed does not exist)."""
    base = variables["dwd"]
    stations = dwd_station_ids(base, variables["dwd_stations"])
    listed: dict[tuple[str, str], str | None] = {}
    files = []
    for key, category, param, kind in missing:
        if key not in stations:
            continue
        url = f"{base}/{category}/" + ("" if kind == "solar" else f"{kind}/")
        if (category, kind) not in listed:
            listed[(category, kind)] = listing_text(url)
            if listed[(category, kind)] is None:
                logger.warning(f"No DWD folder {url}")
        page = listed[(category, kind)] or ""
        pattern = DWD_FILE_PATTERNS[kind].format(param=param, id=stations[key])
        found = re.search(pattern, page)
        if found:
            files.append(
                (
                    f"{key}|{category}|{kind}",
                    f"{url}{found.group(0)}",
                    f"{key}_{category}_{kind}.zip",
                )
            )
    return files


def mastr_export(dataset: str, variables: dict) -> list[tuple[str, str, str]]:
    """The current full export: it is republished daily under a dated file name, so
    the link is read from the download page."""
    page = listing_text(variables["mastr_page"]) or ""
    found = re.search(
        r"https://download\.marktstammdatenregister\.de/Gesamtdatenexport_(\d{8})_[\d.]+\.zip",
        page,
    )
    if not found:
        raise RuntimeError(f"No Gesamtdatenexport link on {variables['mastr_page']}")
    url = found.group(0)
    return [(found.group(1), url, url.rsplit("/", 1)[-1])]


def ga4_tables(dataset: str, variables: dict) -> list[tuple[str, str, str]]:
    """The daily `events_YYYYMMDD` tables of the public GA4 sample: the day files that
    are in data/raw/ga4 when there are any (no BigQuery login is needed), else every
    table found live. A day file that is in data/raw/ga4 is never fetched again."""
    prefix = f"bigquery:{variables['ga4_project']}.{variables['ga4_dataset']}"
    ids = sorted(p.stem for p in (DATA_RAW_DIR / "ga4").glob("events_[0-9]*.jsonl"))
    if ids:
        logger.info(f"GA4: {len(ids)} day files in data/raw/ga4, {ids[0]} to {ids[-1]}")
    else:
        from google.cloud import bigquery

        reference = bigquery.DatasetReference(
            variables["ga4_project"], variables["ga4_dataset"]
        )
        ids = sorted(
            t.table_id
            for t in bigquery_client().list_tables(reference)
            if t.table_id.startswith("events_")
            and t.table_id[len("events_") :].isdigit()
        )
    return [(i.removeprefix("events_"), f"{prefix}.{i}", f"{i}.jsonl") for i in ids]


# --- fetch: fetch(url, destination, timeout) -> {"bytes", "last_modified"}


@cache
def bigquery_client():
    from google.cloud import bigquery

    from config import GCP_CONFIG

    return bigquery.Client(project=GCP_CONFIG["project_id"])


def fetch_bigquery(url: str, dest: Path, timeout: float) -> dict:
    """Query one table (all columns, job location US, no destination table) and
    stream the result to a newline-delimited JSON file."""
    table = url.removeprefix("bigquery:")
    if dest.exists():
        logger.info(f"{dest.name}: already in data/raw, not fetched again")
        return {"bytes": dest.stat().st_size, "last_modified": ""}
    dest.parent.mkdir(parents=True, exist_ok=True)
    rows = bigquery_client().query(f"SELECT * FROM `{table}`", location="US").result()
    temporary = dest.with_suffix(".jsonl.tmp")
    count = 0
    with open(temporary, "w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(dict(row.items()), default=str) + "\n")
            count += 1
    temporary.rename(dest)
    logger.info(f"{table}: {count} rows -> {dest}")
    return {"bytes": dest.stat().st_size, "last_modified": ""}


# --- parsers: parse(path, tag) yields ({dataset: batch}, rows read)


def parse_smard_chart(path: Path, tag: str):
    metric, filter_id, region, resolution = tag.split("|")
    series = json.loads(path.read_text(encoding="utf-8")).get("series") or []
    if not series:
        yield {}, 0
        return
    frame = pd.DataFrame(
        {
            "metric": metric,
            "filter_id": int(filter_id),
            "region": region,
            "resolution": resolution,
            "timestamp_utc": [
                datetime.fromtimestamp(ms / 1000, tz=UTC).isoformat()
                for ms, _ in series
            ],
            "value": [value for _, value in series],
        }
    )
    yield {"energy_timeseries": frame.reindex(columns=SMARD_COLUMNS)}, len(series)


def dwd_chunks(source, **options):
    """A semicolon file as text chunks; the empty column of a trailing `;` is dropped."""
    for chunk in pd.read_csv(
        source,
        sep=";",
        encoding=DWD_ENCODING,
        skipinitialspace=True,
        chunksize=DWD_CHUNK_ROWS,
        dtype=str,
        **options,
    ):
        chunk.columns = [c.strip() for c in chunk.columns]
        yield chunk.loc[:, ~chunk.columns.str.match(r"^Unnamed")]


def dwd_missing_value_chunks(member):
    """A `Metadaten_Fehlwerte` file ends with a 'generiert: <date>' footer line that is
    not a data row."""
    text = member.read_bytes().decode(DWD_ENCODING)
    lines = [
        line
        for line in text.splitlines()
        if line.strip() and not line.lstrip().startswith("generiert:")
    ]
    yield from dwd_chunks(io.BytesIO("\n".join(lines).encode(DWD_ENCODING)))


def new_rows(chunk: pd.DataFrame, seen: set) -> pd.DataFrame:
    """The rows of `chunk` not seen before (a missing value counts as equal)."""
    keep = []
    for row in chunk.itertuples(index=False, name=None):
        key = tuple(None if pd.isna(value) else value for value in row)
        keep.append(key not in seen)
        seen.add(key)
    return chunk.loc[keep]


def dwd_member_chunks(member, dataset: str):
    if dataset == "missing_value_periods":
        yield from dwd_missing_value_chunks(member)
        return
    with member.open("rb") as handle:  # closed, so the zip can be deleted
        yield from dwd_chunks(handle)


def dwd_zip():
    """One DWD zip, or an extracted folder of one: the measurement file as the dataset
    of its category, and (not for solar) the metadata files, kept once by content
    across all files of the stage. Only the `.txt` files are read (a `.html` copy of a
    metadata file is not data)."""
    seen: dict[str, set] = {dataset: set() for dataset in DWD_DEDUPED}

    def parse(path: Path, tag: str):
        station, category, kind = tag.split("|")
        with ExitStack() as stack:
            root = (
                path
                if path.is_dir()
                else zipfile.Path(stack.enter_context(zipfile.ZipFile(path)))
            )
            for member in sorted(root.iterdir(), key=lambda m: m.name):
                name = member.name
                if not name.endswith(".txt"):
                    continue
                if name.startswith("produkt_"):
                    for chunk in dwd_member_chunks(member, category):
                        chunk.insert(1, "city", station)
                        yield {category: chunk}, len(chunk)
                    continue
                prefix = next((p for p in DWD_METADATA if name.startswith(p)), None)
                if prefix is None or kind == "solar":
                    continue
                dataset = DWD_METADATA[prefix]
                for chunk in dwd_member_chunks(member, dataset):
                    read = len(chunk)
                    if dataset == "device_instrument":
                        device = name.removesuffix(".txt").removeprefix(prefix)
                        chunk.insert(1, "device_category", device.rsplit("_", 1)[0])
                    if dataset in DWD_DEDUPED:
                        chunk = new_rows(chunk, seen[dataset])
                    yield {dataset: chunk}, read

    return parse


def mastr_records(archive: zipfile.ZipFile, shard: str):
    """Every direct child of the root of one XML shard as a flat tag -> text mapping;
    the root is cleared after each record, so memory never holds more than one."""
    with archive.open(shard) as handle:
        depth, root = 0, None
        for event, element in ET.iterparse(handle, events=("start", "end")):
            if event == "start":
                depth += 1
                root = element if depth == 1 else root
            else:
                if depth == 2:
                    yield {child.tag: child.text for child in element}
                depth -= 1
                if depth == 0 and root is not None:
                    root.clear()


def mastr_xml(prefixes: dict[str, str]):
    """The object types of `prefixes` (file name prefix -> dataset) of the MaStR
    export, a zip of XML shards. MaStR leaves an absent optional field out of a
    record, so every batch is set to the union of the field names of its type."""

    def parse(path: Path, tag: str):
        with zipfile.ZipFile(path) as archive:
            names = archive.namelist()
            for prefix, dataset in prefixes.items():
                shards = sorted(
                    n
                    for n in names
                    if n == f"{prefix}.xml"
                    or (n.startswith(f"{prefix}_") and n.endswith(".xml"))
                )
                if not shards:
                    logger.warning(f"MaStR {prefix}: no shard in the export")
                    continue
                columns: dict[str, None] = {}
                for shard in shards:
                    for record in mastr_records(archive, shard):
                        columns.update(dict.fromkeys(record))
                    logger.info(f"MaStR {prefix}: columns of {shard}: {len(columns)}")
                names = list(columns)
                batch: list[dict] = []
                for shard in shards:
                    for record in mastr_records(archive, shard):
                        batch.append(record)
                        if len(batch) >= MASTR_BATCH_ROWS:
                            yield (
                                {dataset: pd.DataFrame(batch, columns=names)},
                                len(batch),
                            )
                            batch = []
                if batch:
                    yield {dataset: pd.DataFrame(batch, columns=names)}, len(batch)

    return parse


def power_plant_table(path: Path) -> pd.DataFrame:
    """The BNetzA list as text, from the header row (found by its content) on, without
    footnote and empty rows."""
    raw = pd.read_csv(
        path,
        sep=";",
        encoding=POWER_PLANT_ENCODING,
        header=None,
        dtype=str,
        keep_default_na=False,
    )
    header = None
    for at in range(min(POWER_PLANT_SCAN_ROWS, len(raw))):
        cells = [
            str(c).strip().lower() for c in raw.iloc[at].tolist() if str(c).strip()
        ]
        if sum(any(t in c for t in POWER_PLANT_HEADER_TOKENS) for c in cells) >= 2:
            header = at
            break
    if header is None:
        raise RuntimeError(f"{path.name}: no header row in the first rows")
    names = [str(c).strip().rstrip("*").strip() for c in raw.iloc[header].tolist()]
    blank = sum(1 for c in names if not c or c.lower() == "nan")
    if blank > 0.2 * len(names):
        raise RuntimeError(
            f"{path.name}: {blank} of {len(names)} header cells are blank"
        )
    frame = raw.iloc[header + 1 :].copy()
    frame.columns = names
    frame = frame.replace("", pd.NA)
    first = frame.columns[0]
    footnote = (
        frame.drop(columns=[first]).isna().all(axis=1)
        | frame[first].isna()
        | frame[first]
        .astype(str)
        .str.strip()
        .str.match(r"^(\*|\(|\[|Stand|Quelle|Hinweis|\d+\))")
    )
    return frame[~footnote]


def parse_power_plant_list(path: Path, tag: str):
    """`kraftwerksliste` is the plant list; `zu_und_rueckbau` the small summary of
    planned capacity additions by energy source."""
    if tag == "kraftwerksliste":
        frame = power_plant_table(path)
    else:
        raw = pd.read_excel(path, header=5).dropna(axis=1, how="all")
        raw.columns = CAPACITY_ADDITION_COLUMNS
        frame = raw.dropna(subset=["energietraeger"])
    yield {tag: frame}, len(frame)


def parse_redispatch(path: Path, tag: str):
    """The first CSV of the archive; the tag names the regime of its rows."""
    with zipfile.ZipFile(path) as archive:
        member = next(n for n in archive.namelist() if n.lower().endswith(".csv"))
        with archive.open(member) as handle:
            frame = pd.read_csv(handle, sep=";", encoding="utf-8-sig")
    frame.insert(0, "coverage_regime", tag)
    yield {"redispatch_measures": frame}, len(frame)


def parse_vg250_points(path: Path, tag: str):
    """The municipality points of the VG250 workbook: key and coordinates. A header
    row without the three columns, or a coordinate that is no number, stops the stage."""
    with zipfile.ZipFile(path) as archive:
        member = next(n for n in archive.namelist() if n.lower().endswith(".xlsx"))
        raw = pd.read_excel(
            io.BytesIO(archive.read(member)),
            sheet_name=VG250_SHEET,
            header=None,
            dtype=str,
            keep_default_na=False,
        )
    header = next(
        (
            at
            for at in range(min(VG250_SCAN_ROWS, len(raw)))
            if set(VG250_COLUMNS) <= {str(c).strip() for c in raw.iloc[at]}
        ),
        None,
    )
    if header is None:
        raise RuntimeError(f"{path.name}: no header row with {VG250_COLUMNS}")
    frame = raw.iloc[header + 1 :].copy()
    frame.columns = [str(c).strip() for c in raw.iloc[header]]
    frame = frame[list(VG250_COLUMNS)]
    frame = frame[frame["AGS"].str.strip() != ""].copy()
    frame["AGS"] = (
        frame["AGS"].str.strip().str.replace(r"\.0$", "", regex=True).str.zfill(8)
    )
    for column in ("LON_DEZ", "LAT_DEZ"):
        pd.to_numeric(frame[column])
    yield {"municipality_point": frame}, len(frame)


def project_ga4_event(row: dict) -> dict | None:
    """The retained fields of one raw GA4 event; None for an event outside GA4_EVENTS."""
    name = row.get("event_name")
    if name not in GA4_EVENTS:
        return None
    projected = {
        "event_date": row.get("event_date"),
        "event_timestamp": row.get("event_timestamp"),
        "event_name": name,
        "user_pseudo_id": row.get("user_pseudo_id"),
    }
    params = [
        p for p in (row.get("event_params") or []) if p.get("key") in GA4_PARAM_KEYS
    ]
    if params:
        projected["event_params"] = params
    ecommerce = row.get("ecommerce") or {}
    kept = {
        f: ecommerce[f] for f in GA4_ECOMMERCE_FIELDS if ecommerce.get(f) is not None
    }
    if kept:
        projected["ecommerce"] = kept
    items = [
        {f: item[f] for f in GA4_ITEM_FIELDS if item.get(f) is not None}
        for item in row.get("items") or []
    ]
    items = [item for item in items if item]
    if items:
        projected["items"] = items
    for field, steps in GA4_SCALARS.items():
        value = row
        for step in steps:
            value = value.get(step) if isinstance(value, dict) else None
        if value is not None:
            projected[field] = value
    return projected


def parse_ga4_events(path: Path, tag: str):
    """One day of raw events as the retained records (a list of mappings: the staged
    file is JSON lines, because an event holds nested lists)."""
    batch: list[dict] = []
    read = 0
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            read += 1
            try:
                projected = project_ga4_event(json.loads(line))
            except json.JSONDecodeError as error:
                logger.warning(f"{path.name}: malformed JSON line skipped ({error})")
                continue
            if projected is None:
                continue
            batch.append(projected)
            if len(batch) >= GA4_BATCH_ROWS:
                yield {"events": batch}, read
                batch, read = [], 0
    if batch or read:
        yield {"events": batch}, read


def events_csv(dataset: str, columns: list[str]):
    """A CSV read in chunks and set to `columns` (a column the file lacks is empty)."""

    def parse(path: Path, tag: str):
        for chunk in pd.read_csv(path, chunksize=EVENTS_CHUNK_ROWS):
            yield {dataset: chunk.reindex(columns=columns)}, len(chunk)

    return parse


def honda_iot(columns: dict[str, list[str]]):
    """A gzip CSV of one frequency; the tag is `<frequency>:<dataset>`. Columns are set
    by name, because the column order differs between files."""

    def parse(path: Path, tag: str):
        frequency, dataset = tag.split(":")
        for chunk in pd.read_csv(path, compression="gzip", chunksize=HONDA_CHUNK_ROWS):
            chunk.insert(0, "frequency", frequency)
            yield {dataset: chunk.reindex(columns=columns[dataset])}, len(chunk)

    return parse
