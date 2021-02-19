from __future__ import annotations
from dataclasses import dataclass, field
import pandas as pd
import requests
from requests.utils import quote
import time

from typing import List
from typing_extensions import TypedDict

import config


class PatchDict(TypedDict):
    name: str
    date: str
    id: int


@dataclass
class PlayerData:
    """Class to represent a DataFrame of player's data fetched from
    OpenDota API.
    """

    player: str
    min_patch: str = None
    player_id: str = None
    match_ids: List[int] = None
    patches_data: List[PatchDict] = field(
        default_factory=config.get_patches_data
    )
    matches_data: pd.DataFrame = None
    player_stats: pd.DataFrame = None
    player_data: pd.DataFrame = None

    def __post_init__(self):
        if self.min_patch is None:
            self.min_patch = config.get_current_patch(self.patches_data)

    def get_player_id(self) -> PlayerData:
        """Gets a player's id to ease communication with API."""
        print("\nGanking player's id.")

        data = requests.get(config.BASE_URL + "proPlayers").json()
        self.player_id = str(
            next(
                player["account_id"]
                for player in data
                if player["name"].lower() == self.player.lower()
            )
        )
        print("Got it!")
        return self

    def get_match_ids(self) -> PlayerData:
        """Gets ids for all played matches by the player based on provided
        query. We need those to request parsed data from each.
        """
        print(f"\nRaiding OpenDota for {self.player} match ids.")

        query = f"""
        SELECT
        matches.match_id
        FROM matches
        JOIN match_patch using(match_id)
        JOIN player_matches using(match_id)
        LEFT JOIN notable_players ON
        notable_players.account_id = player_matches.account_id
        LEFT JOIN teams using(team_id)
        WHERE TRUE
        AND match_patch.patch >= cast({self.min_patch} as varchar)
        AND player_matches.account_id = {self.player_id}
        ORDER BY matches.match_id NULLS LAST
        """

        query = quote(query)
        data = requests.get(config.BASE_URL + f"explorer?sql={query}").json()
        match_ids = []
        for row in data["rows"]:
            match_ids.append(row.get("match_id"))
        self.match_ids = match_ids
        print(f"Got those too! Wooping {len(self.match_ids)} matches!")
        return self

    def get_matches_data(self) -> PlayerData:
        """Gets parsed data for every match id."""
        print("\nFarming dat OpenDota's match data...")

        matches_data = []
        for m_id in self.match_ids:
            time.sleep(1.1)
            matches_data.append(
                requests.get(config.BASE_URL + "matches/" + str(m_id)).json()
            )

        self.matches_data = pd.DataFrame(matches_data)[config.required_data]
        print(f"Looted data on all {len(self.matches_data)} matches.")

        self.matches_data = self.matches_data.dropna(
            subset=["match_id", "players"]
            )
        print(f"Looking for missing rows. {len(self.matches_data)} games left.")
        return self

    def get_player_stats(self) -> PlayerData:
        """Extracts data on a required player from all games and creates a
        DataFrame with it.
        """
        player_df = pd.DataFrame()

        games = [f"{row}" for row in range(len(self.matches_data))]

        print("\nBuilding DataFrames for every match...")
        dfs = {
            game: pd.DataFrame(self.matches_data.players.iloc[int(game)])
            for game in games
        }

        print(f"Drafting {self.player}-only DataFrame...")
        for game, df in dfs.items():
            player_df = pd.concat([player_df, df])
        player_df = player_df[player_df.account_id.isin([self.player_id])]

        print("Dropping unnecessary columns...")
        self.player_stats = player_df[config.core_stats]
        print("All good!")
        return self

    def merge_player_data_with_match(self) -> PlayerData:
        """Merges extracted player's data with match-level stats."""
        print("\nStacking player-specific data with general match data...")

        self.player_data = self.player_stats.merge(
            self.matches_data.drop(columns=["players"]), on="match_id"
        )
        self.player_data = self.player_data.dropna()
        print(f"Dropped some more: {len(self.player_data)} games left!")
        return self

    def get_data(self) -> None:
        """Super-function to acquire data from OpenDota."""
        self.get_player_id(
        ).get_match_ids(
        ).get_matches_data(
        ).get_player_stats(
        ).merge_player_data_with_match()

    # Here come data cleaning methods
    @staticmethod
    def id_to_name(id: float, requested_json: list) -> float:
        """Extracts a name of patch or hero for a corresponding id from
        requested json.
        """
        if isinstance(requested_json, dict):
            for key, hero_dict in requested_json.items():
                if hero_dict["id"] == int(id):
                    id = hero_dict["localized_name"]
                    return id
        else:
            for hero_dict in requested_json:
                if hero_dict["id"] == int(id):
                    id = hero_dict["name"]
                    return id

    def clean_patch(self) -> PlayerData:
        """Replaces ids with corresponding names of patches."""
        self.player_data["patch"] = self.player_data["patch"].apply(
            PlayerData.id_to_name, args=(self.patches_data,)
        )
        return self

    def clean_team(self) -> PlayerData:
        """Extracts a team's name from a dict with team information."""
        self.player_data["radiant_team"] = self.player_data["radiant_team"].apply(
            lambda team: team["name"]
        )
        self.player_data["dire_team"] = self.player_data["dire_team"].apply(
            lambda team: team["name"]
        )
        return self

    def clean_league(self) -> PlayerData:
        """Extracts a league's name from a dict with league information."""
        self.player_data["league"] = self.player_data["league"].apply(
            lambda league: league["name"]
        )
        return self

    def clean_win(self) -> PlayerData:
        """Replaces numeric representation with text labels."""
        self.player_data["win"] = self.player_data["win"].replace(
            {1: "Win", 0: "Lose"}
            )
        return self

    def clean_hero(self) -> PlayerData:
        """Replaces ids with corresponding names of heroes."""
        heroes_data = requests.get(
            "http://api.opendota.com/api/constants/heroes"
        ).json()
        self.player_data["hero_id"] = self.player_data["hero_id"].apply(
            PlayerData.id_to_name, args=(heroes_data,)
        )
        self.player_data = self.player_data.rename(columns={"hero_id": "hero"})
        return self

    def clean_start_time(self) -> PlayerData:
        """Replaces timestamp with normal date."""
        self.player_data["start_time"] = pd.to_datetime(
            self.player_data["start_time"], unit="s"
            ).dt.strftime("%Y-%m-%d")
        return self

    def clean_duration(self) -> PlayerData:
        """Replaces timestamp with normal duration."""
        self.player_data["duration"] = pd.to_datetime(
            self.player_data["duration"], unit="s"
        ).dt.strftime("%M:%S")
        return self

    def clean_kda(self) -> PlayerData:
        """Replaces KDA values with traditional formula of (K + A) / D."""
        self.player_data["kda"] = round(
            (self.player_data["kills"] + self.player_data["assists"])
            / self.player_data["deaths"], 2
        ).fillna(
            round((self.player_data["kills"] + self.player_data["assists"])
                / 1, 2)
        )
        return self

    def clean_roaming(self) -> PlayerData:
        """Replaces numeric representation with text labels."""
        self.player_data["is_roaming"] = self.player_data["is_roaming"].replace(
            {True: 'Yes', False: 'No'}
        )
        return self

    def clean_side(self) -> PlayerData:
        """Replaces numeric representation with text labels."""
        self.player_data = self.player_data.rename(
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
        self.player_data["side"] = self.player_data["side"].map(sides)
        return self

    def clean_lane(self) -> PlayerData:
        """Replaces numeric representation with text labels."""
        lanes = {1: "bot", 2: "mid", 3: "top"}
        self.player_data["lane"] = self.player_data["lane"].map(lanes)
        return self

    def clean_lane_neutral_kills(self) -> PlayerData:
        """Renames columns to be more representative."""
        self.player_data = self.player_data.rename(
            columns={"lane_kills": "lane_creeps", "neutral_kills": "neutral_creeps"}
        )
        return self

    def clean_denies(self) -> PlayerData:
        """Extracts values for 10-, 20- and 30-minute marks from a list into
        new columns.
        """
        denies_per_time = self.player_data["dn_t"].apply(pd.Series)
        self.player_data = self.player_data.drop(columns=["dn_t"]).assign(
            dn_10=denies_per_time[9],
            dn_20=denies_per_time[19],
            dn_30=denies_per_time[29],
        )
        return self

    def clean_lh(self) -> PlayerData:
        """Extracts values for 10-, 20- and 30-minute marks from a list into
        new columns.
        """
        lh_per_time = self.player_data["lh_t"].apply(pd.Series)
        self.player_data = self.player_data.drop(columns=["lh_t"]).assign(
            lh_10=lh_per_time[9],
            lh_20=lh_per_time[19],
            lh_30=lh_per_time[29],
        )
        return self

    def clean_nw(self) -> PlayerData:
        """Extracts values for 10-, 20- and 30-minute marks from a list into
        new columns.
        """
        nw_per_time = self.player_data["gold_t"].apply(pd.Series)
        self.player_data = self.player_data.drop(columns=["gold_t"]).assign(
            nw_10=nw_per_time[9],
            nw_20=nw_per_time[19],
            nw_30=nw_per_time[29],
        )
        return self

    def clean_xp(self) -> PlayerData:
        """Extracts values for 10-, 20- and 30-minute marks from a list into
        new columns.
        """
        xp_per_time = self.player_data["xp_t"].apply(pd.Series)
        self.player_data = self.player_data.drop(columns=["xp_t"]).assign(
            xp_10=xp_per_time[9],
            xp_20=xp_per_time[19],
            xp_30=xp_per_time[29],
        )
        return self

    def clean_gold_diff(self) -> PlayerData:
        """Extracts values for 10-, 20- and 30-minute marks from a list into
        new columns. Takes into consideration which side requested player
        played on.
        """

        def revert(values):
            return [value * -1 for value in values]

        self.player_data.loc[
            self.player_data.side == "Dire", "radiant_gold_adv"
        ] = self.player_data["radiant_gold_adv"].apply(revert)

        gold_diff_per_time = self.player_data["radiant_gold_adv"].apply(pd.Series)

        self.player_data = self.player_data.drop(
            columns=["radiant_gold_adv"]).assign(
            gold_diff_10=gold_diff_per_time[9],
            gold_diff_20=gold_diff_per_time[19],
            gold_diff_30=gold_diff_per_time[29],
        )
        return self

    def clean_xp_diff(self) -> PlayerData:
        """Extracts values for 10-, 20- and 30-minute marks from a list into
        new columns. Takes into consideration which side requested player
        played on.
        """

        def revert(values):
            return [v * -1 for v in values]

        self.player_data.loc[
            self.player_data.side == "Dire", "radiant_xp_adv"
        ] = self.player_data["radiant_xp_adv"].apply(revert)

        xp_diff_per_time = self.player_data["radiant_xp_adv"].apply(pd.Series)

        self.player_data = self.player_data.drop(
            columns=["radiant_xp_adv"]).assign(
            xp_diff_10=xp_diff_per_time[9],
            xp_diff_20=xp_diff_per_time[19],
            xp_diff_30=xp_diff_per_time[29],
        )
        return self

    def convert_to_int(self) -> PlayerData:
        """Converts appropriate columns to int."""
        to_int = [
            "dire_score",
            "radiant_score",
            "pings",
            "neutral_creeps",
            "lane_creeps",
        ]
        self.player_data[to_int] = self.player_data[to_int].astype("int")
        return self

    def get_highest_streak(self) -> PlayerData:
        """Replaces dict with its' max key indicating highest kill streak
        achieved by the player in a particular game.
        """
        self.player_data["kill_streaks"] = (
            self.player_data["kill_streaks"]
            .apply(lambda streak: max(
                streak.keys()) if "3" in streak.keys() else None)
            .astype("float")
        )
        self.player_data = self.player_data.rename(
            columns={"kill_streaks": "highest_ks"}
        )
        return self

    def clean_data(self) -> None:
        """Super-function to clean data from OpenDota."""
        self.clean_patch(
        ).clean_team(
        ).clean_league(
        ).clean_hero(
        ).clean_win(
        ).clean_start_time(
        ).clean_duration(
        ).clean_kda(
        ).clean_roaming(
        ).clean_side(
        ).clean_denies(
        ).clean_lh(
        ).clean_nw(
        ).clean_xp(
        ).clean_lane(
        ).clean_lane_neutral_kills(
        ).convert_to_int(
        ).get_highest_streak(
        ).clean_xp_diff(
        ).clean_gold_diff()


if __name__ == "__main__":
    player = PlayerData("mind_control", "7.22")
    player.get_data()
    player.player_data.to_csv("data/mc_data_raw.csv", index=False)

    player.clean_data()

    player.player_data.to_csv("data/mc_data.csv", index=False)
    print("\nCleaned everything and copied data to a separate file.")
