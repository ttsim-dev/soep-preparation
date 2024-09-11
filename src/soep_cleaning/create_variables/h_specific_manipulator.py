from soep_cleaning.config import pd


def hgen(data: pd.DataFrame) -> pd.DataFrame:
    out = data.copy()
    out["bruttokaltmiete_m_hh"] = out["bruttokaltmiete_m_hh"].where(
        out["rented_or_owned"] != "Eigentuemer",
        0,
    )
    return out


def hl(data: pd.DataFrame) -> pd.DataFrame:
    out = data.copy()
    out["kindergeld_aktuell_hl_m_hh"] = out["kindergeld_aktuell_hl_m_hh"].where(
        ~(out["kindergeld_aktuell_hl_m_hh"].isnull())
        & (out["kindergeld_bezug_aktuell"].notna()),
        0,
    )
    return out
