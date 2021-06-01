from __future__ import annotations
from dataclasses import dataclass, field
import pandas as pd
import requests
from requests.utils import quote
import time

from typing import List
from typing_extensions import TypedDict

import config
from utils.helpers import get_current_patch, get_patches_data


class PatchDict(TypedDict):
    name: str
    date: str
    id: int


@dataclass
class PlayerData:
    """Class to represent a DataFrame of player's data 
    fetched from OpenDota API.
    """

    player: str
    min_patch: str = None
    player_id: str = None
    match_ids: List[int] = None
    patches_data: List[PatchDict] = field(
        default_factory=get_patches_data
    )
    matches_data: pd.DataFrame = None
    player_stats: pd.DataFrame = None
    player_data: pd.DataFrame = None

    def __post_init__(self):
        if self.min_patch is None:
            self.min_patch = get_current_patch(self.patches_data)

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
