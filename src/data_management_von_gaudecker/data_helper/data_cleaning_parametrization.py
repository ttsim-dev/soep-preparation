from data_management_von_gaudecker.config import SRC, BLD

def create_parametrization():
    """Create the parametrization for the task."""
    id_to_kwargs = {}
    for id_ in ["biobirth"]:
        id_to_kwargs[id_] = {
            "depends_on": SRC.joinpath("data", "V37", f"{id_}.dta").resolve(),
            "produces": BLD.joinpath("python", "data", f"{id_}_long_and_cleaned.pkl").resolve(),
            "data_set_name": id_,
        }
    return id_to_kwargs