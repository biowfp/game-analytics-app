import requests
import pandas as pd


pd.options.mode.use_inf_as_na = True


BASE_URL = "https://api.opendota.com/api/"

required_data = [
    "match_id",
    "duration",
    "radiant_score",
    "dire_score",
    "radiant_gold_adv",
    "radiant_xp_adv",
    "radiant_team",
    "dire_team",
    "players",
    "league",
    "patch",
    "start_time",
]
core_stats = [
    "match_id",
    "player_slot",
    "win",
    "hero_id",
    "kills",
    "assists",
    "deaths",
    "denies",
    "dn_t",
    "last_hits",
    "lh_t",
    "gold_per_min",
    "gold_t",
    "total_gold",
    "kill_streaks",
    "pings",
    "xp_per_min",
    "xp_t",
    "kda",
    "neutral_kills",
    "lane_kills",
    "lane",
    "is_roaming",
]
supp_stats = None


def id_to_name(x: float, requested_json: list) -> float:
    """Extracts a name of patch or hero for a corresponding id from
    requested json.
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


def get_patches_data() -> list:
    """Gets current patch to use as default argument for initiating class
    instance.
    """
    patches_data = requests.get(BASE_URL + "constants/patch").json()
    return patches_data


#patches_json = get_patches_data()


def get_current_patch(json: list) -> str:
    current_patch = id_to_name(json[-1]['id'], json)
    return current_patch


#current_patch = get_current_patch(patches_json)