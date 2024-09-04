from soep_cleaning.config import pd
from soep_cleaning.create_variables import education_mapping
from soep_cleaning.create_variables.helper import (
    create_dummy,
    create_in_education_dummy_categorical,
    create_selfemployed_occupations,
    generate_education_variable,
    generate_employment_status,
    manipulate_mschaftsgeld_monate,
)
from soep_cleaning.utilities import apply_lowest_float_dtype, apply_lowest_int_dtype


def pequiv(data: pd.DataFrame) -> pd.DataFrame:
    med_vars = [
        "med_pe_schw_anziehen",
        "med_pe_schw_bett",
        "med_pe_schw_einkauf",
        "med_pe_schw_hausarb",
        "med_pe_schw_treppen",
        "med_pe_krnkhaus",
        "med_pe_bluthdrck",
        "med_pe_diabetes",
        "med_pe_krebs",
        "med_pe_herzkr",
        "med_pe_schlaganf",
        "med_pe_gelenk",
        "med_pe_psych",
        "med_pe_subj_status",
    ]
    out = data[data.columns[~data.columns.isin([med_vars])]]
    out["self_empl_earnings_prev"] = out["self_empl_earnings_prev"].fillna(0)
    out[med_vars] = data.groupby("p_id")[med_vars].ffill()
    out["bmi_pe"] = apply_lowest_float_dtype(
        data["med_pe_gewicht"] / ((data["med_pe_groesse"] / 100) ** 2),
    )
    out["bmi_pe_dummy"] = apply_lowest_int_dtype(out["bmi_pe"] >= 30)
    out["med_pe_subj_status_dummy"] = apply_lowest_int_dtype(
        data["med_pe_subj_status"] <= 5,
    )

    med_vars.append("bmi_pe_dummy")
    med_vars.append("med_pe_subj_status_dummy")
    med_vars.remove("med_pe_subj_status")

    out["frailty_pe"] = apply_lowest_float_dtype(out[med_vars].mean(axis=1))
    return out


def pgen(data: pd.DataFrame) -> pd.DataFrame:
    out = data.copy()

    out.loc[
        out["employment_status"] == "Nicht erwerbstätig",
        ["weekly_working_hours_actual", "weekly_working_hours_contract"],
    ] = 0
    out["curr_earnings_m"] = out["curr_earnings_m"].fillna(0)
    out["net_wage_m"] = out["curr_earnnet_wage_mngs_m"].fillna(0)

    out["german"] = create_dummy(out["nationality_first"], "Deutschland")
    out["retired"] = create_dummy(out["occupation_status"], "NE: Rentner/Rentnerin")
    out["in_education"] = create_in_education_dummy_categorical(
        out["employment_status"],
        out["occupation_status"],
    )
    out["self_employed"] = create_dummy(
        out["occupation_status"],
        create_selfemployed_occupations(out["occupation_status"]),
        "isin",
    )
    out["military"] = create_dummy(
        out["occupation_status"],
        "NE: Wehr- und Zivildienst",
    )
    out["erwerbstätig"] = (
        create_dummy(out["employment_status"], "Nicht erwerbstätig", "neq")
    ) & (~out["in_education"].dropna())

    out["nicht_erwerbstätig"] = create_dummy(
        out["employment_status"],
        "Nicht erwerbstätig",
    )
    out["unemployed"] = create_dummy(
        out["occupation_status"],
        "NE: arbeitslos gemeldet",
    )
    out["full_time"] = create_dummy(out["employment_status"], "Voll erwerbstätig")
    out["part_time"] = create_dummy(out["employment_status"], "Teilzeitbeschäftigung")
    out["geringfügig_erwb"] = create_dummy(
        out["employment_status"],
        "Unregelmässig, geringfügig erwerbstät.",
    )
    out["werkstatt"] = create_dummy(
        out["employment_status"],
        "Werkstatt für behinderte Menschen",
    )
    out["beamte"] = out["occupation_status"].str.startswith("Beamte", na=False)
    out["parental_leave"] = create_dummy(
        out["laborf_status"],
        "NE: Mutterschutz/Elternzeit (seit 1991) ",
    )

    out["education"] = generate_education_variable(
        out["education_casmin"],
        out["education_isced"],
        education_mapping,
    )
    return out


def pkal(data: pd.DataFrame) -> pd.DataFrame:
    out = data.copy()

    out["unempl_months_prev"] = out["unempl_months_prev"].fillna(0)

    # mschaftsgeld
    out["mschaftsgeld_monate_prev_1"] = out["mschaftsgeld_monate_prev"]
    out["mschaftsgeld_monate_prev"] = manipulate_mschaftsgeld_monate(
        out["mschaftsgeld_monate_prev"],
        out["mschaftsgeld_bezogen_prev"],
    )

    # months_empl_prev
    for m in range(1, 13):
        out[
            [
                f"full_empl_prev{m}",
                f"half_empl_b_prev_{m}",
                f"employed_m_prev_{m}",
            ]
        ] = generate_employment_status(out, m)

    out["months_empl_prev"] = out[list(out.filter(regex="employed_m_prev_"))].sum(
        axis=1,
    )

    return out
