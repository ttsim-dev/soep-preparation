"""Clean and convert SOEP health variables to appropriate data types."""

import pandas as pd

from soep_preparation.utilities.data_manipulator import (
    apply_smallest_float_dtype,
    apply_smallest_int_dtype,
    create_dummy,
    replace_missing_codes_with_na,
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

    # Component summary scales. The SF-12 scores and BMI arrive as floats with
    # SOEP missing codes encoded as negative values.
    out["sf12_pcs"] = apply_smallest_float_dtype(
        replace_missing_codes_with_na(raw_data["pcs"])
    )
    out["sf12_mcs"] = apply_smallest_float_dtype(
        replace_missing_codes_with_na(raw_data["mcs"])
    )

    # The eight norm-based subscales (suffix `_nbs`) underlying the summary scales.
    out["sf12_physical_functioning_nbs"] = apply_smallest_float_dtype(
        replace_missing_codes_with_na(raw_data["pf_nbs"])
    )
    out["sf12_role_physical_nbs"] = apply_smallest_float_dtype(
        replace_missing_codes_with_na(raw_data["rp_nbs"])
    )
    out["sf12_bodily_pain_nbs"] = apply_smallest_float_dtype(
        replace_missing_codes_with_na(raw_data["bp_nbs"])
    )
    out["sf12_general_health_nbs"] = apply_smallest_float_dtype(
        replace_missing_codes_with_na(raw_data["gh_nbs"])
    )
    out["sf12_vitality_nbs"] = apply_smallest_float_dtype(
        replace_missing_codes_with_na(raw_data["vt_nbs"])
    )
    out["sf12_social_functioning_nbs"] = apply_smallest_float_dtype(
        replace_missing_codes_with_na(raw_data["sf_nbs"])
    )
    out["sf12_role_emotional_nbs"] = apply_smallest_float_dtype(
        replace_missing_codes_with_na(raw_data["re_nbs"])
    )
    out["sf12_mental_health_nbs"] = apply_smallest_float_dtype(
        replace_missing_codes_with_na(raw_data["mh_nbs"])
    )

    # Whether all twelve items needed for the SF-12 scoring are complete.
    out["sf12_valid"] = create_dummy(
        series=raw_data["valid"],
        value_for_comparison="[1] Yes",
        comparison_type="equal",
    )

    out["bmi_health"] = apply_smallest_float_dtype(
        replace_missing_codes_with_na(raw_data["bmi"])
    )

    return out
