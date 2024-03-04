from pytask import task

from data_management_von_gaudecker.config import SRC, BLD
from data_management_von_gaudecker.data_helper.data_cleaning_parametrization import create_parametrization
import data_management_von_gaudecker.data_specific_cleaner.clean_biobirth as clean_biobirth

ID_TO_KWARGS = create_parametrization()

for id_, kwargs in ID_TO_KWARGS.items():

    @task(id=id_, kwargs=kwargs)
    def task_clean_one_dataset(depends_on, produces, data_set_name):
        # TODO: write docstring
        """Clean one dataset."""
        out = clean_biobirth.clean_biobirth(depends_on)
        return out.to_pickle(produces)