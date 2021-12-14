import requests
from config import BASE_URL


def id_to_name(id: float, requested_json: list) -> float:
    """Extracts a name of patch or hero for a corresponding id from
    requested json.
    """
    if isinstance(requested_json, dict):
        for key, hero_dict in requested_json.items():
            if hero_dict["id"] == int(id):
                return hero_dict["localized_name"]
    else:
        for hero_dict in requested_json:
            if hero_dict["id"] == int(id):
                return hero_dict["name"]


def get_patches_data() -> list:
    """Gets a json of patch data from OpenDota"""
    patches_data = requests.get(BASE_URL + "constants/patch").json()
    return patches_data


def get_current_patch(json: list) -> str:
    """Gets current patch to use as default argument for initiating class
    instance.
    """
    current_patch = id_to_name(json[-1]['id'], json)
    return current_patch


#def split_col_by_time(df: pd.DataFrame,
#                      col: str,
#                      new_cols_names: list,
#                      timestamps: list):
#    values_per_time = df[col].apply(pd.Series)
#    data = df.drop(columns=[col]).assign(
#        new_cols_names[0] = values_per_time[timestamps[0]],
#        new_cols_names[1] = values_per_time[timestamps[1]],
#        new_cols_names[2] = values_per_time[timestamps[2]]
#    )
#    return data

def negate(values):
    """Turns values in a list to negatives"""
    return [value * -1 for value in values]
