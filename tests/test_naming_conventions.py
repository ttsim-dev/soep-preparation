"""Enforce the variable naming convention (docs/naming_conventions.md).

Final variable names are English by default; German is kept only for the
programme/pillar/legal proper-noun allow-list. Names are Unicode-aware snake_case
with no leading digit. This checks both the generated metadata inventory (the
authoritative list of final names) and the `out["..."]` assignment literals in the
cleaning modules.
"""

import ast
import re
import unicodedata
from pathlib import Path

import yaml

_SRC = Path(__file__).parent.parent / "src" / "soep_preparation"
_MAPPING = _SRC / "create_metadata" / "variable_to_metadata_mapping.yaml"
_CLEAN_DIRS = ("clean_modules", "combine_modules")

# snake_case, no leading digit, ASCII lowercase plus German umlauts/ß (GETTSIM uses ü).
_SNAKE_CASE = re.compile(r"^[a-zßäöü][a-z0-9ßäöü_]*$")

# German programme/pillar/legal proper-noun tokens that may remain German. Multi-word
# stems contribute each of their tokens (e.g. grundsicherung_im_alter -> im, alter).
_ALLOW_GERMAN = frozenset(
    {
        "kindergeld",
        "kinderzuschlag",
        "wohngeld",
        "arbeitslosengeld",
        "arbeitslosenhilfe",
        "grundsicherung",
        "im",
        "alter",
        "sozialhilfe",
        "hilfe",
        "zum",
        "lebensunterhalt",
        "bafög",
        "pflegegeld",
        "mutterschaftsgeld",
        "mutterschutz",
        "elternzeit",
        "elterngeld",
        "kriegsopferversorgung",
        "betreuungsgeld",
        "eigenheimzulage",
        "vorruhestandsgeld",
        "kindesunterhalt",
        "ehegattenunterhalt",
        "unterhalt",
        "unterhaltsvorschuss",
        "gesetzliche",
        "rente",
        "knappschaftliche",
        "riester",
        "betriebliche",
        "altersversorgung",
        "berufsständische",
        "altersvorsorge",
        "private",
        "beamtenpension",
        "alterssicherung",
        "landwirte",
        "unfallversicherung",
        "altersteilzeit",
        "werkstatt",
        "für",
        "behinderte",
    }
)

# German scaffolding/descriptive tokens that must always be translated to English.
_DENY_GERMAN = frozenset(
    {
        "bezieht",
        "bezog",
        "aktuell",
        "anzahl",
        "monate",
        "monat",
        "letzten",
        "eingezahlt",
        "eingezahlte",
        "steuern",
        "brutto",
        "hinterbliebene",
        "gemeldet",
        "erwerbstätig",
        "betriebsgröße",
        "größe",
        "migräne",
        "rücken",
        "öffentlichen",
        "teilzeit",
        "tatsächliche",
        "vertragliche",
        "unregelmäßig",
        "geringfügig",
        "beschäftigungsverhältnis",
        "beschäftigungsende",
        "beendigung",
        "betriebsstillegung",
        "krankenhaus",
        "andere",
        "sonstige",
        "allgemeine",
        "zusätzliche",
        "versorgung",
        "einkommen",
        "einkünfte",
        "vermietung",
        "verpachtung",
        "zinsen",
        "dividenden",
        "selbstständiger",
        "gewinnbeteiligung",
        "urlaubsgeld",
        "weihnachtsgeld",
        "boni",
        "erhalten",
        "erhaltenes",
        "nicht",
        "voll",
        "grund",
        "beamter",
        "arbeitslos",
        "erwerbsgemindert",
        "schwerbehindert",
        "bundesland",
        "typ",
        "demenz",
        "depressiv",
        "herzkrankheit",
        "gelenk",
        "gewicht",
        "schlaganfall",
        "schlaf",
        "krebs",
        "raucher",
        "bluthochdruck",
        "psych",
        "schwierigkeiten",
        "anziehen",
        "treppen",
        "bett",
        "einkauf",
        "hausarb",
        "taten",
        "zufrieden",
        "sonst",
        "dienst",
        "arbeitszeit",
        "kategorien",
        "detailliert",
        "inkonsistente",
        "versicherung",
        "eigener",
        "aus",
    }
)


def _violations(name: str) -> list[str]:
    """Return the reasons `name` breaks the convention (empty list if it conforms)."""
    name = unicodedata.normalize("NFC", name)
    reasons = []
    if not _SNAKE_CASE.match(name):
        reasons.append("not Unicode-aware snake_case / leading digit")
    tokens = name.split("_")
    denied = sorted(set(tokens) & _DENY_GERMAN)
    if denied:
        reasons.append(f"German scaffolding tokens: {denied}")
    non_ascii_unknown = sorted(
        t
        for t in tokens
        if not t.isascii() and t not in _ALLOW_GERMAN and t not in _DENY_GERMAN
    )
    if non_ascii_unknown:
        reasons.append(f"non-ASCII token not on the allow-list: {non_ascii_unknown}")
    return reasons


def _final_names() -> list[str]:
    return list(yaml.safe_load(_MAPPING.read_text(encoding="utf-8")))


def _assigned_names() -> set[str]:
    """Every string-literal key assigned to `out[...]` in the cleaning modules (AST)."""
    names: set[str] = set()
    for directory in _CLEAN_DIRS:
        for module in (_SRC / directory).glob("*.py"):
            tree = ast.parse(module.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if (
                    isinstance(node, ast.Subscript)
                    and isinstance(node.value, ast.Name)
                    and node.value.id == "out"
                    and isinstance(node.slice, ast.Constant)
                    and isinstance(node.slice.value, str)
                ):
                    names.add(node.slice.value)
    return names


def test_final_variable_names_follow_the_convention():
    """No final variable name carries German scaffolding or breaks snake_case."""
    offenders = {name: _violations(name) for name in _final_names()}
    offenders = {name: reasons for name, reasons in offenders.items() if reasons}
    assert offenders == {}


def test_assigned_out_names_follow_the_convention():
    """`out[...]` assignment literals in the cleaning modules conform too."""
    index_columns = {"p_id", "hh_id", "hh_id_original", "hh_id_orig", "survey_year"}
    offenders = {
        name: _violations(name)
        for name in _assigned_names()
        if name not in index_columns and _violations(name)
    }
    assert offenders == {}


def test_convention_rejects_mixed_allow_and_deny_tokens():
    """An allowed German stem does not exempt the rest of the name (span-exact)."""
    assert _violations("bezieht_kindergeld_aktuell")


def test_convention_accepts_approved_unicode_names():
    """Approved Unicode allow-list names pass."""
    assert _violations("bafög_y") == []
    assert _violations("berufsständische_altersvorsorge_y") == []


def test_convention_rejects_leading_digit():
    """A leading digit fails PEP 8."""
    assert _violations("1989_place_of_residence")
