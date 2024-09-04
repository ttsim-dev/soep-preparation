from soep_cleaning.config import pd


def hgen(data: pd.DataFrame) -> pd.DataFrame:
    out = data.copy()
    out.loc[out["rented_or_owned"] == "Eigentuemer", ["bruttokaltmiete_m_hh"]] = 0
    return out


def hl(data: pd.DataFrame) -> pd.DataFrame:
    out = data.copy()

    # fill missing for kindergeld_hl_m_hh with 0 if kindergeld_bezug_aktuell is False
    out.loc[
        (out["kindergeld_aktuell_hl_m_hh"].isnull())
        & (~(out["kindergeld_bezug_aktuell"].notna())),
        "kindergeld_aktuell_hl_m_hh",
    ] = 0

    return out
