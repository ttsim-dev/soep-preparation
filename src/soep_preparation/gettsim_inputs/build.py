"""Build a GETTSIM-ready dataset from a SOEP final dataset.

GETTSIM consumes flat data: each column label is a double-underscore qualified name
(qname) such as `wohnen__wohnfläche_hh`. `build_gettsim_inputs` selects the SOEP final
variables that `SOEP_TO_GETTSIM` maps to a GETTSIM input, renames them to the GETTSIM
qname, and keeps the index variables (`p_id`, `hh_id`, `survey_year`) verbatim.

Inputs whose mapped SOEP variable is absent from the supplied frame are skipped; the
result therefore carries exactly the GETTSIM inputs that the data can currently supply.
Use `fail_if_required_inputs_missing` to surface the gap against a concrete set of
required GETTSIM inputs.
"""

from collections.abc import Mapping
from typing import TypedDict

import pandas as pd

from soep_preparation.gettsim_inputs.mapping import INDEX_VARIABLES, SOEP_TO_GETTSIM


class MappingReport(TypedDict):
    """Coverage summary of the GETTSIM-input mapping."""

    n_inputs_total: int
    """Number of GETTSIM inputs in the mapping."""
    n_inputs_mapped: int
    """Number of inputs with a SOEP source."""
    n_inputs_unmapped: int
    """Number of inputs without a SOEP source."""
    mapped: dict[str, str]
    """GETTSIM input qname to SOEP variable, for the mapped inputs."""
    unmapped: list[str]
    """GETTSIM input qnames without a SOEP source, sorted."""


def build_gettsim_inputs(final_dataset: pd.DataFrame) -> pd.DataFrame:
    """Select and rename SOEP final variables to their GETTSIM input qnames.

    Args:
        final_dataset: A SOEP final dataset with single-underscore SOEP variable names
            as columns, including index variables.

    Returns:
        A flat dataset whose columns are the GETTSIM input qnames (plus any present
        index variables), holding the values of the mapped SOEP variables.
    """
    resolved = resolve_available_mapping(
        mapping=SOEP_TO_GETTSIM,
        available_columns=final_dataset.columns,
    )

    out = pd.DataFrame(index=final_dataset.index)
    for index_variable in INDEX_VARIABLES:
        if index_variable in final_dataset.columns:
            out[index_variable] = final_dataset[index_variable]
    for gettsim_qname, soep_variable in resolved.items():
        out[gettsim_qname] = final_dataset[soep_variable]
    return out


def resolve_available_mapping(
    mapping: Mapping[str, str | None],
    available_columns: pd.Index,
) -> dict[str, str]:
    """Restrict the mapping to inputs with a mapped SOEP column present in the data.

    Args:
        mapping: GETTSIM input qname to SOEP variable name (or `None`).
        available_columns: Column labels of the SOEP final dataset.

    Returns:
        GETTSIM input qname to SOEP variable name, for non-`None` mappings whose SOEP
        variable is a column of the dataset. Index variables are excluded, as the build
        helper carries them through verbatim.
    """
    columns = set(available_columns)
    return {
        gettsim_qname: soep_variable
        for gettsim_qname, soep_variable in mapping.items()
        if soep_variable is not None
        and soep_variable in columns
        and gettsim_qname not in INDEX_VARIABLES
    }


def create_mapping_report(
    mapping: Mapping[str, str | None] = SOEP_TO_GETTSIM,
) -> MappingReport:
    """Summarize how many GETTSIM inputs the mapping covers.

    Args:
        mapping: GETTSIM input qname to SOEP variable name (or `None`).

    Returns:
        A dictionary with mapped/unmapped counts and the per-input table, suitable for
        serialization to JSON.
    """
    mapped = {
        qname: soep_variable
        for qname, soep_variable in mapping.items()
        if soep_variable is not None
    }
    unmapped = sorted(
        qname for qname, soep_variable in mapping.items() if soep_variable is None
    )
    return {
        "n_inputs_total": len(mapping),
        "n_inputs_mapped": len(mapped),
        "n_inputs_unmapped": len(unmapped),
        "mapped": dict(sorted(mapped.items())),
        "unmapped": unmapped,
    }


def fail_if_required_inputs_missing(
    required_inputs: frozenset[str],
    available_columns: pd.Index,
    mapping: Mapping[str, str | None] = SOEP_TO_GETTSIM,
) -> None:
    """Fail if any required GETTSIM input cannot be supplied from the data.

    A required input is unsupplied when it is either unmapped (`None` in `mapping`,
    or absent from `mapping`) or mapped to a SOEP variable not present in the data.

    Args:
        required_inputs: GETTSIM input qnames that must be supplied.
        available_columns: Column labels of the SOEP final dataset.
        mapping: GETTSIM input qname to SOEP variable name (or `None`).

    Raises:
        ValueError: If any required input is unsupplied, listing the offenders.
    """
    columns = set(available_columns)
    unmapped = sorted(
        qname
        for qname in required_inputs
        if mapping.get(qname) is None and qname not in INDEX_VARIABLES
    )
    mapped_but_absent = sorted(
        qname
        for qname in required_inputs
        if (soep_variable := mapping.get(qname)) is not None
        and soep_variable not in columns
        and qname not in INDEX_VARIABLES
    )
    if unmapped or mapped_but_absent:
        msg = (
            "Some required GETTSIM inputs cannot be supplied from the SOEP data.\n"
            f"Unmapped (no SOEP source): {unmapped}\n"
            f"Mapped but column absent from the dataset: {mapped_but_absent}"
        )
        raise ValueError(msg)
