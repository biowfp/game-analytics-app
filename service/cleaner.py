from __future__ import annotations
from dataclasses import dataclass, field
import pandas as pd
import requests

from utils.helpers import id_to_name, negate


@dataclass
class DataCleaner:
    """Collection of methods to clean data from OpenDota API.
    """
    
    def clean_patch(self, data) -> PlayerData:
        """Replaces ids with corresponding names of patches."""
        data["patch"] = data["patch"].apply(
            id_to_name, args=(self.patches_data,)
        )
        return self

    def clean_team(self, data) -> PlayerData:
        """Extracts a team's name from a dict with team information."""
        data["radiant_team"] = data["radiant_team"].apply(
            lambda team: team["name"]
        )
        data["dire_team"] = data["dire_team"].apply(
            lambda team: team["name"]
        )
        return self

    def clean_league(self, data) -> PlayerData:
        """Extracts a league's name from a dict with league information."""
        data["league"] = data["league"].apply(
            lambda league: league["name"]
        )
        return self

    def clean_win(self, data) -> PlayerData:
        """Replaces numeric representation with text labels."""
        data["win"] = data["win"].replace(
            {1: "Win", 0: "Lose"}
            )
        return self

    def clean_hero(self, data) -> PlayerData:
        """Replaces ids with corresponding names of heroes."""
        heroes_data = requests.get(
            "http://api.opendota.com/api/constants/heroes"
        ).json()
        data["hero_id"] = data["hero_id"].apply(
            id_to_name, args=(heroes_data,)
        )
        data = data.rename(columns={"hero_id": "hero"})
        return self

    def clean_start_time(self, data) -> PlayerData:
        """Replaces timestamp with normal date."""
        data["start_time"] = pd.to_datetime(
            data["start_time"], unit="s"
            ).dt.strftime("%Y-%m-%d")
        return self

    def clean_duration(self, data) -> PlayerData:
        """Replaces timestamp with normal duration."""
        data["duration"] = pd.to_datetime(
            data["duration"], unit="s"
        ).dt.strftime("%M:%S")
        return self

    def clean_kda(self, data) -> PlayerData:
        """Replaces KDA values with traditional formula of (K + A) / D."""
        data["kda"] = round(
            (data["kills"] + data["assists"])
            / data["deaths"], 2
        ).fillna(
            round((data["kills"] + data["assists"])
                / 1, 2)
        )
        return self

    def clean_roaming(self, data) -> PlayerData:
        """Replaces numeric representation with text labels."""
        data["is_roaming"] = data["is_roaming"].replace(
            {True: 'Yes', False: 'No'}
        )
        return self

    def clean_side(self, data) -> PlayerData:
        """Replaces numeric representation with text labels."""
        data = data.rename(
            columns={"player_slot": "side"}
            )
        sides = {
            0: "Radiant",
            1: "Radiant",
            2: "Radiant",
            3: "Radiant",
            4: "Radiant",
            128: "Dire",
            129: "Dire",
            130: "Dire",
            131: "Dire",
            132: "Dire",
        }
        data["side"] = data["side"].map(sides)
        return self

    def clean_lane(self, data) -> PlayerData:
        """Replaces numeric representation with text labels."""
        lanes = {1: "bot", 2: "mid", 3: "top"}
        data["lane"] = data["lane"].map(lanes)
        return self

    def clean_lane_neutral_kills(self, data) -> PlayerData:
        """Renames columns to be more representative."""
        data = data.rename(
            columns={"lane_kills": "lane_creeps", "neutral_kills": "neutral_creeps"}
        )
        return self

    def clean_denies(self, data) -> PlayerData:
        """Extracts values for 10-, 20- and 30-minute marks from a list into
        new columns.
        """
        denies_per_time = data["dn_t"].apply(pd.Series)
        data = data.drop(columns=["dn_t"]).assign(
            dn_10=denies_per_time[9],
            dn_20=denies_per_time[19],
            dn_30=denies_per_time[29],
        )
        return self

    def clean_lh(self, data) -> PlayerData:
        """Extracts values for 10-, 20- and 30-minute marks from a list into
        new columns.
        """
        lh_per_time = data["lh_t"].apply(pd.Series)
        data = data.drop(columns=["lh_t"]).assign(
            lh_10=lh_per_time[9],
            lh_20=lh_per_time[19],
            lh_30=lh_per_time[29],
        )
        return self

    def clean_nw(self, data) -> PlayerData:
        """Extracts values for 10-, 20- and 30-minute marks from a list into
        new columns.
        """
        nw_per_time = data["gold_t"].apply(pd.Series)
        data = data.drop(columns=["gold_t"]).assign(
            nw_10=nw_per_time[9],
            nw_20=nw_per_time[19],
            nw_30=nw_per_time[29],
        )
        return self

    def clean_xp(self, data) -> PlayerData:
        """Extracts values for 10-, 20- and 30-minute marks from a list into
        new columns.
        """
        xp_per_time = data["xp_t"].apply(pd.Series)
        data = data.drop(columns=["xp_t"]).assign(
            xp_10=xp_per_time[9],
            xp_20=xp_per_time[19],
            xp_30=xp_per_time[29],
        )
        return self

    def clean_gold_diff(self, data) -> PlayerData:
        """Extracts values for 10-, 20- and 30-minute marks from a list into
        new columns. Takes into consideration which side requested player
        played on.
        """
        data.loc[
            data.side == "Dire", "radiant_gold_adv"
        ] = data["radiant_gold_adv"].apply(negate)

        gold_diff_per_time = data["radiant_gold_adv"].apply(pd.Series)

        data = data.drop(
            columns=["radiant_gold_adv"]).assign(
            gold_diff_10=gold_diff_per_time[9],
            gold_diff_20=gold_diff_per_time[19],
            gold_diff_30=gold_diff_per_time[29],
        )
        return self

    def clean_xp_diff(self, data) -> PlayerData:
        """Extracts values for 10-, 20- and 30-minute marks from a list into
        new columns. Takes into consideration which side requested player
        played on.
        """
        data.loc[
            data.side == "Dire", "radiant_xp_adv"
        ] = data["radiant_xp_adv"].apply(negate)

        xp_diff_per_time = data["radiant_xp_adv"].apply(pd.Series)

        data = data.drop(
            columns=["radiant_xp_adv"]).assign(
            xp_diff_10=xp_diff_per_time[9],
            xp_diff_20=xp_diff_per_time[19],
            xp_diff_30=xp_diff_per_time[29],
        )
        return self

    def convert_to_int(self, data) -> PlayerData:
        """Converts appropriate columns to int."""
        to_int = [
            "dire_score",
            "radiant_score",
            "pings",
            "neutral_creeps",
            "lane_creeps",
        ]
        data[to_int] = data[to_int].astype("int")
        return self

    def get_highest_streak(self, data) -> PlayerData:
        """Replaces dict with its' max key indicating highest kill streak
        achieved by the player in a particular game.
        """
        data["kill_streaks"] = (
            data["kill_streaks"]
            .apply(lambda streak: max(
                streak.keys()) if "3" in streak.keys() else None)
            .astype("float")
        )
        data = data.rename(
            columns={"kill_streaks": "highest_ks"}
        )
        return self


def clean_data(data):
    cleaner = DataCleaner()
    cleaner.clean_patch(data
    ).clean_team(data
    ).clean_league(data
    ).clean_hero(data
    ).clean_win(data
    ).clean_start_time(data
    ).clean_duration(data
    ).clean_kda(data
    ).clean_roaming(data
    ).clean_side(data
    ).clean_denies(data
    ).clean_lh(data
    ).clean_nw(data
    ).clean_xp(data
    ).clean_lane(data
    ).clean_lane_neutral_kills(data
    ).convert_to_int(data
    ).get_highest_streak(data
    ).clean_xp_diff(data
    ).clean_gold_diff(data)