import dags.tree as dt
from _gettsim.config import RESOURCE_DIR
from gettsim import set_up_policy_environment
from ttsim import PolicyInput


def get_policy_inputs() -> dict[int, list[str]]:
    """Get the policy inputs from the policy environment of GETTSIM."""
    res = {}
    for year in range(2005, 2021):
        policy_environment = set_up_policy_environment(
            resource_dir=RESOURCE_DIR,
            date=year,
        )

        flat_objects = dt.flatten_to_qual_names(policy_environment.raw_objects_tree)

        inputs = {
            qual_name: obj
            for qual_name, obj in flat_objects.items()
            if isinstance(obj, PolicyInput)
        }

        res[year] = list(inputs.keys())
    return res
