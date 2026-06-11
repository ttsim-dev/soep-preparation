"""Clean and convert SOEP health variables to appropriate data types."""

import pandas as pd

from soep_preparation.utilities.data_manipulator import (
    apply_smallest_int_dtype,
    create_dummy,
    object_to_float,
)


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Create cleaned variables from the health module.

    The health module is a generated person-year dataset with health
    indicators collected in a two-year replication cycle since 2002. Its
    centerpiece is the SOEP version of the SF-12v2: eight norm-based
    subscales plus the physical (PCS) and mental (MCS) component summary
    scales, all standardized to mean 50 and standard deviation 10 in the
    SOEP 2004 population, with higher values representing better
    health-related quality of life.

    Args:
        raw_data: The raw health data.

    Returns:
        The processed health data.
    """
    out = pd.DataFrame()

    out["p_id"] = apply_smallest_int_dtype(raw_data["pid"])
    out["hh_id_original"] = apply_smallest_int_dtype(raw_data["cid"])
    out["survey_year"] = apply_smallest_int_dtype(raw_data["syear"])

    # Component summary scales.
    out["sf12_pcs"] = object_to_float(raw_data["pcs"])
    out["sf12_mcs"] = object_to_float(raw_data["mcs"])

    # The eight norm-based subscales underlying the summary scales.
    out["sf12_physical_functioning_nbs"] = object_to_float(raw_data["pf_nbs"])
    out["sf12_role_physical_nbs"] = object_to_float(raw_data["rp_nbs"])
    out["sf12_bodily_pain_nbs"] = object_to_float(raw_data["bp_nbs"])
    out["sf12_general_health_nbs"] = object_to_float(raw_data["gh_nbs"])
    out["sf12_vitality_nbs"] = object_to_float(raw_data["vt_nbs"])
    out["sf12_social_functioning_nbs"] = object_to_float(raw_data["sf_nbs"])
    out["sf12_role_emotional_nbs"] = object_to_float(raw_data["re_nbs"])
    out["sf12_mental_health_nbs"] = object_to_float(raw_data["mh_nbs"])

    # Whether all twelve items needed for the SF-12 scoring are complete.
    out["sf12_valid"] = create_dummy(
        series=raw_data["valid"],
        value_for_comparison="[1] Yes",
        comparison_type="equal",
    )

    out["bmi_health"] = object_to_float(raw_data["bmi"])

    return out
