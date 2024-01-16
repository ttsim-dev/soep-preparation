"""Tasks running the core analyses."""

from pathlib import Path

import pandas as pd
import pytask

from data_management_von_gaudecker.analysis.model import fit_logit_model, load_model
from data_management_von_gaudecker.analysis.predict import predict_prob_by_age
from data_management_von_gaudecker.config import BLD, GROUPS, SRC
from data_management_von_gaudecker.utilities import read_yaml

fit_model_deps = {
    "scripts": [Path("model.py"), Path("predict.py")],
    "data": BLD / "python" / "data" / "data_clean.csv",
    "data_info": SRC / "data_management" / "data_info.yaml",
}


def task_fit_model_python(
    depends_on=fit_model_deps,
    produces=BLD / "python" / "models" / "model.pickle",
):
    """Fit a logistic regression model (Python version)."""
    data_info = read_yaml(depends_on["data_info"])
    data = pd.read_csv(depends_on["data"])
    model = fit_logit_model(data, data_info, model_type="linear")
    model.save(produces)


for group in GROUPS:
    predict_deps = {
        "data": BLD / "python" / "data" / "data_clean.csv",
        "model": BLD / "python" / "models" / "model.pickle",
    }

    @pytask.task(id=group)
    def task_predict_python(
        group=group,
        depends_on=predict_deps,
        produces=BLD / "python" / "predictions" / f"{group}.csv",
    ):
        """Predict based on the model estimates (Python version)."""
        model = load_model(depends_on["model"])
        data = pd.read_csv(depends_on["data"])
        predicted_prob = predict_prob_by_age(data, model, group)
        predicted_prob.to_csv(produces, index=False)
