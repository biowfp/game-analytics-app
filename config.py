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
