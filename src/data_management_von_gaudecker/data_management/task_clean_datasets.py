from pytask import task

from data_management_von_gaudecker.config import SRC, BLD
from data_management_von_gaudecker.data_helper.data_loader import dta_loader
from data_management_von_gaudecker.data_helper.data_cleaning_parametrization import create_parametrization
from data_management_von_gaudecker.data_specific_cleaner.clean_biobirth import clean_biobirth, melt_biobirth

ID_TO_KWARGS = create_parametrization()

for id_, kwargs in ID_TO_KWARGS.items():

    @task(id=id_, kwargs=kwargs)
    def task_clean_one_dataset(depends_on, produces, data_set_name):
        # TODO: write docstring
        """Clean one dataset."""
        raw_data = dta_loader(depends_on)
        cleaned = clean_biobirth(raw_data)
        long_cleaned = melt_biobirth(cleaned)
        long_cleaned.to_pickle(produces)
