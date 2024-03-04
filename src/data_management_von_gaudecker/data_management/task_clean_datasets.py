from pytask import task

from data_management_von_gaudecker.config import SRC, BLD
import data_management_von_gaudecker.data_specific_cleaner.clean_biobirth as clean_biobirth

def create_parametrization():
    """Create the parametrization for the task."""
    id_to_kwargs = {}
    for id_ in ["biobirth"]:
        id_to_kwargs[id_] = {
            "depends_on": SRC.joinpath( "data", "V37", f"{id_}.dta").resolve(),
            "produces": BLD.joinpath( "python", "data", f"{id_}_long_and_cleaned.pickle").resolve(),
            # "data_set_name": id_,
            # "out_format": "pickle",
        }
    return id_to_kwargs

ID_TO_KWARGS = create_parametrization()

for id_, kwargs in ID_TO_KWARGS.items():

    @task(id=id_, kwargs=kwargs)
    def task_clean_one_dataset(depends_on, produces):
        """Clean one dataset."""
        # breakpoint()
        return clean_biobirth.clean_biobirth(depends_on)