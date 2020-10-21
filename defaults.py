import requests


def id_to_name(x, requested_json):
    """Extracts a name of patch or hero for a corresponding id from
    requested json.

    Args:
        x (float): ID of a patch.
        requested_json (list): List of dictionaries requested through
        OpenDota API that contains information about patches.

    Returns:
        float: Actual patch number(for example, '7.25').
    """
    if isinstance(requested_json, dict):
        for k, v in requested_json.items():
            if v["id"] == int(x):
                x = v["localized_name"]
                return x
    else:
        for d in requested_json:
            if d["id"] == int(x):
                x = d["name"]
                return x


def get_current_patch():
    """Gets current patch to use as default argument for initiating class
    instance.

    Returns:
        str: string representation of current dota patch
    """

    patches_data = requests.get(
        "http://api.opendota.com/api/constants/patch"
    ).json()
    current_patch = id_to_name(patches_data[-1]['id'], patches_data)
    return (current_patch, patches_data)


patch = get_current_patch()[0]
data = get_current_patch()[1]
