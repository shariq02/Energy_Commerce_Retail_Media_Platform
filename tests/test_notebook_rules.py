"""Static rules for the notebook sources of ML, Gold, models and Silver -- no Spark.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: November 2026

One file for the layer rules that `lint.py` and CI do not check: a notebook that writes
a table calls the hard-fail gate, `toPandas` stays in the files that may use it, the
build code never reads the test partition, shared tables are reached through the
helpers, and the findings call of every model notebook is complete. The header, the
cell limit and the document references are checked by `lint.py`, not here.
"""

from __future__ import annotations

import ast
import re

import pytest

from src.schemas._silver_notebook_scan import silver_notebooks
from tests._notebook_loader import (
    REPO_ROOT,
    load_ml_common,
    load_model_common,
    load_model_libs,
)

pytestmark = [pytest.mark.schema, pytest.mark.unit]

_DB = REPO_ROOT / "databricks"
_ML = _DB / "ml"
_MODELS = _DB / "models"
_GOLD = _DB / "gold"
_ANALYTICS = _DB / "analytics"
_SILVER = _DB / "silver"


def _read(path):
    return path.read_text(encoding="utf-8")


def _py(root):
    return sorted(root.rglob("*.py"))


def _notebooks(root):
    return [p for p in _py(root) if not p.name.startswith("_")]


def _rel(root):
    return lambda p: p.relative_to(root).as_posix()


def _calls(tree):
    return {
        n.func.id
        for n in ast.walk(tree)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
    }


def _first_call_line(tree, name):
    lines = [
        n.lineno
        for n in ast.walk(tree)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Name)
        and n.func.id == name
    ]
    return min(lines) if lines else None


def _name(path):
    return path.relative_to(_DB).as_posix()


# --- a notebook that writes a table calls the hard-fail gate -----------------

_WRITE_AND_GATE = {
    "ml": {
        "files": _notebooks(_ML),
        "write": {
            "write_ml",
            "write_dataset_rows",
            "write_stored_table",
            "build_datasets",
        },
        "gate": {
            "check",
            "assert_unique_grain",
            "record_check",
            "assert_values_present",
            "assert_no_forbidden_columns",
            # these library functions call the gates themselves
            "build_datasets",
            "write_stored_table",
        },
    },
    # the analytics notebooks write with the same library and the same gate
    "gold": {
        "files": [*_notebooks(_GOLD), *_notebooks(_ANALYTICS)],
        "write": {"write_tables"},
        "gate": {"check", "assert_unique_grain", "gate_tables"},
    },
}
_WRITERS = [
    (layer, path)
    for layer, rule in _WRITE_AND_GATE.items()
    for path in rule["files"]
    if _calls(ast.parse(_read(path))) & rule["write"]
]
_GOLD_WRITERS = [p for layer, p in _WRITERS if layer == "gold"]


@pytest.mark.parametrize(
    ("layer", "path"), _WRITERS, ids=[f"{layer}:{_name(p)}" for layer, p in _WRITERS]
)
def test_writing_notebooks_call_the_hard_fail_gate(layer, path):
    calls = _calls(ast.parse(_read(path)))
    assert calls & _WRITE_AND_GATE[layer]["gate"], (
        f"{_name(path)} writes without a gate"
    )


@pytest.mark.parametrize("path", _GOLD_WRITERS, ids=_name)
def test_the_grain_gate_runs_on_the_written_tables(path):
    """`gate_tables` checks the written tables and restores a failing one, so
    it must come after `write_tables` (the first gate call of the notebook)."""
    tree = ast.parse(_read(path))
    gate = _first_call_line(tree, "gate_tables")
    write = _first_call_line(tree, "write_tables")
    assert gate is not None and write is not None and write < gate


def test_no_gold_notebook_creates_a_view() -> None:
    """Every Gold object is a table."""
    creators = [
        p.name
        for p in _WRITE_AND_GATE["gold"]["files"]
        if "write_gold_view" in _calls(ast.parse(_read(p)))
        or "CREATE OR REPLACE VIEW" in _read(p).upper()
    ]
    assert not creators, f"Gold notebooks that create a view: {creators}"


# --- toPandas stays in the files that may use it ------------------------------

_TO_PANDAS = [
    # (folder, text that marks a call, files of that folder that may call it)
    (
        _ML,
        ".toPandas()",
        {
            "energy/02_unit_and_site_datasets.py",
            "energy/03_zone_and_weather_datasets.py",
            "gate/01_ml_gates.py",
        },
    ),
    (_MODELS, ".toPandas(", {"lib/_model_common.py"}),
]
_TO_PANDAS_FILES = [
    (root, needle, ok, p) for root, needle, ok in _TO_PANDAS for p in _py(root)
]


@pytest.mark.parametrize(
    ("root", "needle", "allowed", "path"),
    _TO_PANDAS_FILES,
    ids=[_name(p) for *_, p in _TO_PANDAS_FILES],
)
def test_to_pandas_is_limited_to_the_files_that_may_use_it(root, needle, allowed, path):
    if needle in _read(path):
        assert _rel(root)(path) in allowed, f"{_name(path)} calls toPandas"


# --- ML build code ------------------------------------------------------------


def test_the_spec_references_only_defined_calendars():
    calendars = set(load_ml_common()["SPLIT_CALENDARS"])
    pat = re.compile(r"(?:calendar_partition|rolling_fold)\([^,]+,\s*\"(\w+)\"")
    for p in _py(_ML):
        for key in pat.findall(_read(p)):
            assert key in calendars, f"{_name(p)} uses unknown calendar {key}"


def test_build_code_never_reads_the_test_partition():
    pat = re.compile(r"""(==|!=)\s*['"]test['"]|isin\([^)]*['"]test['"]""")
    for p in (*_ML.glob("*/0*.py"), _ML / "_ml_fit.py"):
        if p.parent.name == "gate":
            continue
        assert not pat.search(_read(p)), _name(p)


def test_registry_tables_are_defined_and_used():
    ddl = set(load_ml_common()["REGISTRY_DDL"])
    used = set()
    for p in _py(_ML):
        used |= set(re.findall(r"read_ml\(\"([a-z_]+)\"", _read(p)))
    registry = {
        "dataset_manifest",
        "partition_manifest",
        "null_class_registry",
        "imputer_parameters",
        "mask_specification",
        "feature_contract",
        "gate_results",
    }
    assert registry == ddl
    assert used & registry


def test_the_ml_folder_stays_under_the_file_target():
    """Fewer than 15 files: the notebooks, the libraries and the spec file."""
    spec = REPO_ROOT / "src" / "schemas" / "mappings" / "ml_datasets.yml"
    assert spec.exists()
    assert len(_py(_ML)) + 1 < 15, f"{len(_py(_ML))} files in databricks/ml"


_SHARED_NAME_ALLOWED = {"_ml_common.py", "00_ml_setup.py", "01_ml_gates.py"}


@pytest.mark.parametrize("path", _py(_ML), ids=_rel(_ML))
def test_shared_registry_tables_are_reached_through_the_helpers(path):
    """Both ecosystems share these tables, so a read goes through `read_ml` and a
    write through `replace_rows`; only the setup, the freeze gate and the library
    name the full table."""
    if path.name in _SHARED_NAME_ALLOWED:
        return
    text = _read(path)
    for name in load_ml_common()["ML_SHARED_TABLES"]:
        assert f'ml_fqn("{name}"' not in text, f"{_name(path)}: {name}"
        assert f"ml_fqn('{name}'" not in text, f"{_name(path)}: {name}"


# Each row: file, start of the scope, end of the scope, what comes first, what later.
_ORDER = [
    ("_ml_common.py", "def write_ml(", None, "_frozen_guard(", ".saveAsTable("),
    (
        "_ml_common.py",
        "def write_dataset_rows(",
        None,
        "_frozen_guard(",
        ".saveAsTable(",
    ),
    ("_ml_fit.py", "def build_datasets(", "\ndef ", "reopen_family(", "build_dataset("),
    (
        "gate/01_ml_gates.py",
        None,
        None,
        "error = protect_and_read(full, version)",
        "SET status = 'frozen'",
    ),
    (
        "gate/01_ml_gates.py",
        None,
        None,
        "if waiting or failed:",
        "error = protect_and_read(full, version)",
    ),
]
# Each row: file, texts that must be in it, texts that must not be in it.
_MARKERS = [
    (
        "gate/01_ml_gates.py",
        ['dataset_spec(ds).get("min_labelled_rows", MIN_LABELLED_ROWS)'],
        ["< MIN_LABELLED_ROWS"],
    ),
    (
        "gate/01_ml_gates.py",
        [
            "delta.deletedFileRetentionDuration",
            "delta.logRetentionDuration",
            'format("noop")',
            "waiting = [d for d in built",
            "if waiting or failed:",
        ],
        [],
    ),
]


@pytest.mark.parametrize(("name", "start", "end", "first", "later"), _ORDER)
def test_the_ml_code_keeps_the_order_of_its_steps(name, start, end, first, later):
    """A frozen dataset is refused before any write, a rerun after a freeze reopens
    the families before the first write, and a family is frozen only when every
    dataset of it passed."""
    text = _read(_ML / name)
    body = text[text.index(start) :] if start else text
    if end:
        body = body[: body.index(end)]
    assert body.index(first) < body.index(later), f"{name}: {first} after {later}"


@pytest.mark.parametrize(("name", "present", "absent"), _MARKERS)
def test_the_ml_code_keeps_its_markers(name, present, absent):
    text = _read(_ML / name)
    for marker in present:
        assert marker in text, f"{name}: {marker}"
    for marker in absent:
        assert marker not in text, f"{name}: {marker}"


def test_only_the_library_reopens_the_families():
    """A rerun after a freeze must work in every ML build notebook."""
    for path in (_ML / "genomics").glob("01_*.py"):
        assert "reopen_family(" not in _read(path)


def test_the_partition_frame_returns_the_fold_as_an_int():
    """The family table and the manifest keep `fold_id` as int; a grouped fold is
    a bigint hash, and Delta refuses to merge the two types."""
    text = _read(_ML / "_ml_common.py")
    body = text[text.index("def partition_frame(") :]
    body = body[: body.index("\ndef ")]
    assert 'F.col("fold_id").cast("int")' in body.rsplit("return", 1)[1]


# --- model notebooks ----------------------------------------------------------

_MODEL_FILES = _py(_MODELS)
_MODEL_NOTEBOOKS = _notebooks(_MODELS)
_mrel = _rel(_MODELS)
_RETIRED_NAMES = re.compile(
    r"foundation_(?:ts|tab|anomaly|survival|rank|rl|stack)|weak_supervision|"
    r"redispatch_matching|self_supervised_other|chronos|tabpfn|tabicl|momentfm|"
    r"timesfm|granite_tsfm|task_registry|evaluation_split_manifest"
)
_MAY_NAME_THE_TEST_PARTITION = {
    "lib/_model_common.py",
    "lib/_model_evaluate.py",
    "evaluate/01_evaluate.py",
    "evaluate/02_evaluation_gate.py",
}
_MODEL_TABLE_ACCESS = [
    r"saveAsTable\(model_fqn\(",
    r"CREATE TABLE IF NOT EXISTS \{model_fqn\(",
    r"spark\.table\(model_fqn\(",
]
_MODEL_FILE_LIST = {
    "setup/01_model_libraries.py",
    "setup/02_model_setup.py",
    "train/01_train.py",
    "train/02_candidate_gate.py",
    "evaluate/01_evaluate.py",
    "evaluate/02_evaluation_gate.py",
    "approve/01_recommend.py",
    "approve/02_record_decisions.py",
    "approve/03_model_cards.py",
    "approve/04_approval_gate.py",
    "register/01_register_models.py",
    "register/02_reproducibility.py",
    "register/03_registry_gate.py",
    "operate/01_reference_and_monitor.py",
    "operate/02_performance_and_triggers.py",
    "operate/03_score_request.py",
    "operate/04_operate_gate.py",
    "lib/_model_common.py",
    "lib/_model_fit.py",
    "lib/_model_evaluate.py",
    "lib/_model_govern.py",
    "lib/_model_operate.py",
}
_APPROVAL = [p for p in _MODEL_NOTEBOOKS if _mrel(p).startswith("approve/")]
_REGISTER = [p for p in _MODEL_NOTEBOOKS if _mrel(p).startswith("register/")]
_OPERATE = [p for p in _MODEL_NOTEBOOKS if _mrel(p).startswith("operate/")]
_REPRODUCIBILITY = _MODELS / "register" / "02_reproducibility.py"
_RECORD_ONLY = [p for p in _REGISTER if p != _REPRODUCIBILITY]


@pytest.mark.parametrize("path", _MODEL_FILES, ids=_mrel)
def test_no_code_of_a_removed_model_family_remains(path):
    hits = _RETIRED_NAMES.findall(_read(path))
    assert not hits, f"{_mrel(path)} names removed code: {hits}"


@pytest.mark.parametrize("path", _MODEL_FILES, ids=_mrel)
def test_model_code_never_names_the_test_partition(path):
    # The shared library assigns the partition and the evaluation code reads it; the
    # other libraries and the notebooks only ever read train and validation.
    if _mrel(path) in _MAY_NAME_THE_TEST_PARTITION:
        return
    assert not re.search(r"""['"]test['"]""", _read(path)), _mrel(path)


def test_every_notebook_has_a_numbered_name_in_its_folder():
    for p in _MODEL_NOTEBOOKS:
        assert re.match(r"\d\d_", p.name), _mrel(p)
    assert {p.parent.name for p in _MODEL_NOTEBOOKS} == {
        "setup",
        "train",
        "evaluate",
        "approve",
        "register",
        "operate",
    }


def test_the_task_notebooks_hold_no_task_id_of_their_own():
    task_ids = set(load_model_common()["TASK_BY_ID"])
    for p in _MODEL_NOTEBOOKS:
        text = _read(p)
        assert not [t for t in task_ids if f'"{t}"' in text], _mrel(p)


def test_training_and_evaluation_run_every_task_through_the_task_file():
    for name, entry in (
        ("train/01_train.py", "run_task("),
        ("evaluate/01_evaluate.py", "evaluate_by_id("),
    ):
        text = _read(_MODELS / name)
        assert entry in text, name
        assert "spark.table(" not in text and "spark.read" not in text, name
        assert "SMOKE = False" in text, name
        assert re.search(r"^(RETRAIN|REEVALUATE) = False$", text, re.MULTILINE), name


@pytest.mark.parametrize("path", _MODEL_FILES, ids=_mrel)
def test_no_model_file_reads_a_widget(path):
    assert "dbutils.widgets" not in _read(path), _mrel(path)


def test_only_the_volume_schema_keeps_an_ecosystem_name():
    for p in _MODEL_FILES:
        names = set(re.findall(r"\b(\w+_ml_models)\b", _read(p)))
        assert names <= {"energy_ml_models"}, _mrel(p)


@pytest.mark.parametrize("path", _MODEL_FILES, ids=_mrel)
def test_model_tables_are_reached_only_through_the_shared_helpers(path):
    """The ecosystems share each model table, so every read, create and append goes
    through `read_model`, `create_model_table` and `append_model_rows`."""
    if path.name == "_model_common.py":
        return
    text = _read(path)
    for pattern in _MODEL_TABLE_ACCESS:
        assert not re.search(pattern, text), f"{_mrel(path)}: {pattern}"


@pytest.mark.parametrize("path", _MODEL_FILES, ids=_mrel)
def test_a_delete_on_a_model_table_names_the_ecosystem(path):
    text = _read(path)
    if "DELETE FROM {model_fqn(" in text:
        assert "ecosystem_predicate(" in text, _mrel(path)


def test_the_expected_files_exist():
    for rel in _MODEL_FILE_LIST:
        assert (_MODELS / rel).exists(), rel
    assert {_mrel(p) for p in _MODEL_FILES} == _MODEL_FILE_LIST
    assert (_DB / "schema_registry" / "01_snapshot_schema.py").exists()


@pytest.mark.parametrize("path", _APPROVAL, ids=_mrel)
def test_approval_notebooks_read_recorded_results_only(path):
    text = _read(path)
    assert ".toPandas(" not in text
    assert "evaluate_by_id(" not in text and "load_bundle(" not in text
    assert "%run ../lib/_model_common" in text
    assert "%run ../lib/_model_govern" in text


@pytest.mark.parametrize("path", _RECORD_ONLY, ids=_mrel)
def test_registry_record_notebooks_read_recorded_results_only(path):
    text = _read(path)
    assert "evaluate_by_id(" not in text and "load_bundle(" not in text
    assert "%run ../lib/_model_common" in text
    assert "%run ../lib/_model_govern" in text


def test_reproducibility_notebook_uses_the_check_functions_and_scores_no_test_data():
    text = _read(_REPRODUCIBILITY)
    for lib in ("_model_common", "_model_fit", "_model_evaluate", "_model_govern"):
        assert f"%run ../lib/{lib}" in text
    assert "reproduce_ecosystem(" in text and "record_checks(" in text
    assert "evaluate_by_id(" not in text
    library = _read(_MODELS / "lib" / "_model_govern.py")
    evaluation = _read(_MODELS / "lib" / "_model_evaluate.py")
    assert "class ReproContext(EvalContext)" in evaluation
    assert (
        "ALLOWED_PARTITIONS" in evaluation
        and 'self.partition = "validation"' in evaluation
    )
    assert "class ReproContext" not in library
    assert "evaluate_task(" not in library and "run_candidate(" not in library


@pytest.mark.parametrize("path", _OPERATE, ids=_mrel)
def test_operate_notebooks_never_change_a_decision_or_a_registry_row(path):
    text = _read(path)
    pattern = r"""(?:replace_table_rows|replace_model_rows)\([^)]*['"]model_(?:registry|approval)"""
    assert not re.search(pattern, text), _mrel(path)
    assert 'saveAsTable(model_fqn("model_registry"' not in text, _mrel(path)
    assert 'saveAsTable(model_fqn("model_approval"' not in text, _mrel(path)


def test_the_score_request_takes_keys_and_a_window_and_no_feature_values():
    text = _read(_MODELS / "operate" / "03_score_request.py")
    for setting in ("TASK_ID", "KEYS", "WINDOW_START", "WINDOW_END"):
        assert f"{setting} = " in text
    assert "evaluate_by_id(" not in text and "widgets" not in text
    assert "key_columns(cfg)" in _read(_MODELS / "lib" / "_model_operate.py")


def test_operate_window_library_scores_no_target():
    text = _read(_MODELS / "lib" / "_model_operate.py")
    assert ".score(" not in text.replace("bundle.score(", "")
    assert "evaluate_task(" not in text and "record_evaluation(" not in text
    assert "window_partition(" in text and "TEST_PARTITION" in text


def test_genomics_texts_hold_no_identifier():
    """Findings and cards of the genomics ecosystem go to the repository as counts,
    rates and metrics only: no variant, gene, rsID or genomic position."""
    gate = _read(_MODELS / "approve" / "04_approval_gate.py")
    assert "genomics_texts_hold_no_identifier" in gate
    assert "GENOMICS_IDENTIFIER.search(" in gate


# --- the findings call of the model notebooks ---------------------------------

_FINDING_FILES = sorted(p for p in _MODELS.rglob("[0-9]*.py") if p.parent.name != "lib")
# a gate writes the findings files itself and ends with the merge
_GATES = [p for p in _FINDING_FILES if "FINDINGS_DIR" in _read(p)]
_FINDING_NOTEBOOKS = [
    p for p in _FINDING_FILES if p not in _GATES and p.name != "01_model_libraries.py"
]


def _findings_call(path):
    """The call of `write_model_findings` in a notebook, in any cell."""
    for node in ast.walk(ast.parse(_read(path))):
        if isinstance(node, ast.Call) and getattr(node.func, "id", "") == (
            "write_model_findings"
        ):
            return node
    return None


@pytest.fixture(scope="module")
def lib():
    return load_model_libs()


@pytest.mark.parametrize("path", _FINDING_NOTEBOOKS, ids=_mrel)
def test_notebook_writes_its_findings(path):
    assert _findings_call(path) is not None


@pytest.mark.parametrize("path", _FINDING_NOTEBOOKS, ids=_mrel)
def test_findings_call_names_known_tables_and_tasks(path, lib):
    call = _findings_call(path)
    tables = call.args[3]
    if not isinstance(tables, ast.List):
        return
    known = set(lib["MODEL_DDL"])
    named = {e.value for e in tables.elts if isinstance(e, ast.Constant)}
    assert named <= known, named - known
    keywords = {k.arg: k.value for k in call.keywords}
    if "task_ids" in keywords:
        wanted = {t["task_id"] for t in lib["TASKS"]} | set(lib["CHECK_ONLY"])
        listed = getattr(keywords["task_ids"], "elts", [])  # a computed list is skipped
        named = {e.value for e in listed if isinstance(e, ast.Constant)}
        assert named <= wanted


def test_the_findings_key_is_the_notebook_path():
    keys = set()
    for path in _FINDING_NOTEBOOKS:
        key = ast.literal_eval(_findings_call(path).args[1])
        assert key == _mrel(path)[:-3].replace("/", "__")
        keys.add(key)
    assert len(keys) == len(_FINDING_NOTEBOOKS)


def test_every_gate_that_writes_findings_is_listed():
    assert {_mrel(p) for p in _GATES} == {
        "train/02_candidate_gate.py",
        "evaluate/02_evaluation_gate.py",
        "approve/04_approval_gate.py",
        "register/03_registry_gate.py",
        "operate/04_operate_gate.py",
    }


@pytest.mark.parametrize("gate", _GATES, ids=lambda p: p.name)
def test_findings_gates_end_with_the_merge(gate):
    last = _read(gate).split("# COMMAND ----------")
    assert last[-1].strip().endswith("merge_notebook_findings()")


def test_notebook_findings_group_splits_prefix_smoke_and_key(lib):
    group = lib["notebook_findings_group"]
    assert group("energy__split__01_x.md") == ("energy", False, "energy__split__01_x")
    assert group("commerce__evaluate__01_evaluate_smoke.md") == (
        "commerce",
        True,
        "commerce__evaluate__01_evaluate",
    )


def test_merge_replaces_a_section_and_keeps_the_others(lib):
    import datetime as dt

    now = dt.datetime(2026, 10, 23, tzinfo=dt.UTC)
    one = "# H\n\n_x_\n\n## A\n\n### t\n\nrow\n"
    sections = {"energy__a": lib["section_of_findings_file"](one)}
    sections["energy__b"] = "## B\n\nold"
    text = lib["merged_findings_text"]("energy", sections, False, now)
    again = lib["read_merged_sections"](text)
    assert again == sections
    again["energy__b"] = "## B\n\nnew"
    text = lib["merged_findings_text"]("energy", again, False, now)
    result = lib["read_merged_sections"](text)
    assert result["energy__b"] == "## B\n\nnew"
    assert result["energy__a"].startswith("## A")


# --- Silver notebooks ---------------------------------------------------------

_SILVER_NOTEBOOKS = silver_notebooks(_SILVER)
# forbidden as real code use (quoted column name / function call), not prose
_FORBIDDEN = ('"canonical_id"', "'canonical_id'", "F.monotonically_increasing_id")


def _code_lines(text: str) -> str:
    """Drop comment / MAGIC lines so prose mentions do not trip token checks."""
    return "\n".join(ln for ln in text.splitlines() if not ln.lstrip().startswith("#"))


@pytest.mark.parametrize("nb", _SILVER_NOTEBOOKS, ids=_rel(_SILVER))
def test_silver_notebook_carries_the_author_the_date_and_the_project_name(nb):
    text = _read(nb)
    assert "# MAGIC **Author:** Sharique Mohammad" in text
    assert "# MAGIC **Date:**" in text
    assert "ECRMAP" in text


@pytest.mark.parametrize("nb", _SILVER_NOTEBOOKS, ids=_rel(_SILVER))
def test_no_forbidden_silver_boundary_tokens(nb) -> None:
    code = _code_lines(_read(nb))
    for token in _FORBIDDEN:
        assert token not in code, f"{nb.name} references forbidden `{token}`"


def test_shared_library_has_no_forbidden_tokens() -> None:
    code = _code_lines(_read(_SILVER / "_silver_common.py"))
    for token in _FORBIDDEN:
        assert token not in code


def test_silver_reads_the_clean_dwd_station_coordinate_columns() -> None:
    """Bronze replaces a dot in a column name with an underscore."""
    code = _code_lines(_read(_SILVER / "energy" / "01_weather_reference.py"))
    assert "Geogr_Breite" in code
    assert "Geogr_Laenge" in code
    assert "Geogr." not in code
