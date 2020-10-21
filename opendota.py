import pandas as pd
import requests
from requests.utils import quote
import time

import defaults

pd.options.mode.use_inf_as_na = True


class TeamData:
    """Class to store all PlayerData instances from one team."""
    # add request for team data to see players' nicks and ids
    players_data = {}
    pass


class PlayerData:
    """Class to represent a DataFrame of player's data fetched from OpenDota API.

    Attributes:
        player: str
        min_patch: str
        player_id: str
        match_ids: list
        patches_data: list
        matches_data: pd.DataFrame
        player_stats: pd.DataFrame
        player_data: pd.DataFrame

    Methods:
        get_player_id: Gets a player's id to ease communication with API.
        get_match_ids: Gets ids for all played matches by the player based on
        provided query. We need those to request parsed data from each.
        get_matches_data: Gets parsed data for every match id.
        get_player_stats: Extracts data on a required player from all games
        and creates a DataFrame with it.
        merge_player_data_with_match: Merges extracted player's data with
        match-level stats.
        get_data: Super-function to acquire data from OpenDota.

        id_to_name: Extracts a name of patch or hero for a corresponding id
        from requested json.
        clean_patch: Replaces ids with corresponding names of patches.
        clean_team: Extracts a team's name from a dict with team information.
        clean_league: Extracts a league's name from a dict with league
        information.
        clean_win: Replaces numeric representation with text labels.
        clean_hero: Replaces ids with corresponding names of heroes.
        clean_start_time: Replaces timestamp with normal date.
        clean_duration: Replaces timestamp with normal duration.
        clean_kda: Replaces KDA values with traditional formula of (K + A) / D.
        clean_roaming: Replaces numeric representation with text labels.
        clean_side: Replaces numeric representation with text labels.
        clean_lane: Replaces numeric representation with text labels.
        clean_lane_neutral_kills: Renames columns to be more representative.
        clean_denies: Extracts values for 10-, 20- and 30-minute marks from
        a list into new columns.
        clean_lh: Extracts values for 10-, 20- and 30-minute marks from a list
        into new columns.
        clean_nw: Extracts values for 10-, 20- and 30-minute marks from a list
        into new columns.
        clean_xp: Extracts values for 10-, 20- and 30-minute marks from a list
        into new columns.
        convert_to_int: Converts appropriate columns to int.
        get_highest_streak: Replaces dict with its' max key indicating highest
        kill streak achieved by the player in a particular game.
        clean_xp_diff: Extracts values for 10-, 20- and 30-minute marks from
        a list into new columns. Takes into consideration which side requested
        player played on.
        clean_gold_diff: Extracts values for 10-, 20- and 30-minute marks from
        a list into new columns. Takes into consideration which side requested
        player played on.
        clean_data: Super-function to clean data from OpenDota.
    """

    def __init__(self, player: str, min_patch: str = defaults.patch):
        self.player = player
        self.min_patch = min_patch
        self.player_id = None
        self.match_ids = None
        self.patches_data = defaults.data
        self.matches_data = []
        self.player_stats = None
        self.player_data = None

        self.required_data = [
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
        self.core_stats = [
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
            "gold_reasons",
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
        self.supp_stats = None

    def get_player_id(self):
        """Gets a player's id to ease communication with API.

        Returns:
            object: PlayerData class instance.
        """
        print("\nGanking player's id.")

        data = requests.get("https://api.opendota.com/api/proPlayers").json()
        self.player_id = str(
            next(
                (
                    x["account_id"]
                    for x in data
                    if x["name"].lower() == self.player.lower()
                )
            )
        )
        print("Got it!")
        return self

    def get_match_ids(self):
        """Gets ids for all played matches by the player based on provided
        query. We need those to request parsed data from each.

        Returns:
            object: PlayerData class instance.
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
        data = requests.get(f"https://api.opendota.com/api/explorer?sql={query}").json()
        match_ids = []
        for x in data["rows"]:
            match_ids.append(x.get("match_id"))
        self.match_ids = match_ids
        print(f"Got those too! Wooping {len(self.match_ids)} matches!")
        return self

    def get_matches_data(self):
        """Gets parsed data for every match id.

        Returns:
            object: PlayerData class instance.
        """
        print("\nFarming dat OpenDota's match data...")

        matches_data = []
        for m_id in self.match_ids:
            time.sleep(1.06)  # to stay under 60 requests per minute (3 by now)
            matches_data.append(
                requests.get("http://api.opendota.com/api/matches/" + str(m_id)).json()
            )

        self.matches_data = pd.DataFrame(matches_data)[self.required_data]
        print(f"Looted data on all {len(self.matches_data)} matches.")

        self.matches_data = self.matches_data.dropna(subset=["match_id", "players"])
        print(f"Dropped missing rows. {len(self.matches_data)} games left.")
        return self

    def get_player_stats(self):
        """Extracts data on a required player from all games and creates a
        DataFrame with it.

        Returns:
            object: PlayerData class instance.
        """
        out_df = pd.DataFrame()
        # Each cell in 'players' column represents one unique game of Dota2
        # and contains player-specific data about all 10 players in a game.
        names_for_dfs = [f"{row}" for row in range(len(self.matches_data))]

        print("\nBuilding DataFrames for every match...")
        dfs = {
            # We create a df for every game - those consist of 10 rows, each
            # representing a single player in the game.
            name: pd.DataFrame(self.matches_data.players.iloc[int(name)])
            for name in names_for_dfs
        }

        print(f"Drafting {self.player}-only DataFrame...")
        # We then look for a specific player's row by its' id and append it to
        # an out_df which will contain info from all games of one specific
        # player.
        for name, df in dfs.items():
            out_df = out_df.append(
                df[df.account_id.isin([self.player_id])], ignore_index=True
            )

        print("Dropping unnecessary columns...")
        self.player_stats = out_df[self.core_stats]
        print("All good!")
        return self

    def merge_player_data_with_match(self):
        """Merges extracted player's data with match-level stats.

        Returns:
            object: PlayerData class instance.
        """
        print("\nStacking player-specific data with general match data...")

        self.player_data = self.player_stats.merge(
            self.matches_data.drop(columns=["players"]), on="match_id"
        )
        self.player_data.dropna(inplace=True)
        print(f"Dropped some more: {len(self.player_data)} games left!")
        return self

    def get_data(self):
        """Super-function to acquire data from OpenDota."""
        self.get_player_id().get_match_ids().get_matches_data(
        ).get_player_stats().merge_player_data_with_match()

    # Here come data cleaning methods
    @staticmethod
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

    def clean_patch(self):
        """Replaces ids with corresponding names of patches.

        Returns:
            object: PlayerData class instance.
        """
        self.player_data["patch"] = self.player_data["patch"].apply(
            PlayerData.id_to_name, args=(self.patches_data,)
        )
        return self

    def clean_team(self):
        """Extracts a team's name from a dict with team information.

        Returns:
            object: PlayerData class instance.
        """
        self.player_data["radiant_team"] = self.player_data["radiant_team"].apply(
            lambda x: x["name"]
        )
        self.player_data["dire_team"] = self.player_data["dire_team"].apply(
            lambda x: x["name"]
        )
        return self

    def clean_league(self):
        """Extracts a league's name from a dict with league information.

        Returns:
            object: PlayerData class instance.
        """
        self.player_data["league"] = self.player_data["league"].apply(
            lambda x: x["name"]
        )
        return self

    def clean_win(self):
        """Replaces numeric representation with text labels.

        Returns:
            object: PlayerData class instance.
        """
        self.player_data["win"].replace({1: "Win", 0: "Lose"}, inplace=True)
        return self

    def clean_hero(self):
        """Replaces ids with corresponding names of heroes.

        Returns:
            object: PlayerData class instance.
        """
        heroes_data = requests.get(
            "http://api.opendota.com/api/constants/heroes"
        ).json()
        self.player_data["hero_id"] = self.player_data["hero_id"].apply(
            PlayerData.id_to_name, args=(heroes_data,)
        )
        self.player_data.rename(columns={"hero_id": "hero"}, inplace=True)
        return self

    def clean_start_time(self):
        """Replaces timestamp with normal date.

        Returns:
            object: PlayerData class instance.
        """
        self.player_data["start_time"] = pd.to_datetime(
            self.player_data["start_time"], unit="s"
        ).dt.strftime("%Y-%m-%d")
        return self

    def clean_duration(self):
        """Replaces timestamp with normal duration.

        Returns:
            object: PlayerData class instance.
        """
        self.player_data["duration"] = pd.to_datetime(
            self.player_data["duration"], unit="s"
        ).dt.strftime("%M:%S")
        return self

    def clean_kda(self):
        """Replaces KDA values with traditional formula of (K + A) / D.

        Returns:
            object: PlayerData class instance.
        """
        self.player_data["kda"] = (
            (self.player_data["kills"] + self.player_data["assists"])
            / self.player_data["deaths"]
        ).fillna((self.player_data["kills"] + self.player_data["assists"]) / 1)
        return self

    def clean_roaming(self):
        """Replaces numeric representation with text labels.

        Returns:
            object: PlayerData class instance.
        """
        self.player_data["is_roaming"].replace({True: 1, False: 0}, inplace=True)
        return self

    def clean_side(self):
        """Replaces numeric representation with text labels.

        Returns:
            object: PlayerData class instance.
        """
        self.player_data.rename(columns={"player_slot": "side"}, inplace=True)
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

    def clean_lane(self):
        """Replaces numeric representation with text labels.

        Returns:
            object: PlayerData class instance.
        """
        lanes = {1: "bot", 2: "mid", 3: "top"}
        self.player_data["lane"] = self.player_data["lane"].map(lanes)
        return self

    def clean_lane_neutral_kills(self):
        """Renames columns to be more representative.

        Returns:
            object: PlayerData class instance.
        """
        self.player_data.rename(
            columns={"lane_kills": "lane_creeps", "neutral_kills": "neutral_creeps"},
            inplace=True,
        )
        return self

    def clean_denies(self):
        """Extracts values for 10-, 20- and 30-minute marks from a list into
        new columns.

        Returns:
            object: PlayerData class instance.
        """
        denies_per_time = self.player_data["dn_t"].apply(pd.Series)
        self.player_data = self.player_data.drop(columns=["dn_t"]).assign(
            dn_10=denies_per_time[9],
            dn_20=denies_per_time[19],
            dn_30=denies_per_time[29],
        )
        return self

    def clean_lh(self):
        """Extracts values for 10-, 20- and 30-minute marks from a list into
        new columns.

        Returns:
            object: PlayerData class instance.
        """
        lh_per_time = self.player_data["lh_t"].apply(pd.Series)
        self.player_data = self.player_data.drop(columns=["lh_t"]).assign(
            lh_10=lh_per_time[9],
            lh_20=lh_per_time[19],
            lh_30=lh_per_time[29]
        )
        return self

    def clean_nw(self):
        """Extracts values for 10-, 20- and 30-minute marks from a list into
        new columns.

        Returns:
            object: PlayerData class instance.
        """
        nw_per_time = self.player_data["gold_t"].apply(pd.Series)
        self.player_data = self.player_data.drop(columns=["gold_t"]).assign(
            nw_10=nw_per_time[9],
            nw_20=nw_per_time[19],
            nw_30=nw_per_time[29]
        )
        return self

    def clean_xp(self):
        """Extracts values for 10-, 20- and 30-minute marks from a list into
        new columns.

        Returns:
            object: PlayerData class instance.
        """
        xp_per_time = self.player_data["xp_t"].apply(pd.Series)
        self.player_data = self.player_data.drop(columns=["xp_t"]).assign(
            xp_10=xp_per_time[9],
            xp_20=xp_per_time[19],
            xp_30=xp_per_time[29]
        )
        return self

    def clean_gold_diff(self):
        """Extracts values for 10-, 20- and 30-minute marks from a list into
        new columns. Takes into consideration which side requested player
        played on.

        Returns:
            object: PlayerData class instance.
        """

        def revert(values):
            return [v * -1 for v in values]

        self.player_data.loc[
            self.player_data.side == "Dire", "radiant_gold_adv"
        ] = self.player_data["radiant_gold_adv"].apply(revert)

        gold_diff_per_time = self.player_data["radiant_gold_adv"].apply(pd.Series)

        self.player_data = self.player_data.drop(columns=["radiant_gold_adv"]).assign(
            gold_diff_10=gold_diff_per_time[9],
            gold_diff_20=gold_diff_per_time[19],
            gold_diff_30=gold_diff_per_time[29],
        )
        return self

    def clean_xp_diff(self):
        """Extracts values for 10-, 20- and 30-minute marks from a list into
        new columns. Takes into consideration which side requested player
        played on.

        Returns:
            object: PlayerData class instance.
        """

        def revert(values):
            return [v * -1 for v in values]

        self.player_data.loc[
            self.player_data.side == "Dire", "radiant_xp_adv"
        ] = self.player_data["radiant_xp_adv"].apply(revert)

        xp_diff_per_time = self.player_data["radiant_xp_adv"].apply(pd.Series)

        self.player_data = self.player_data.drop(columns=["radiant_xp_adv"]).assign(
            xp_diff_10=xp_diff_per_time[9],
            xp_diff_20=xp_diff_per_time[19],
            xp_diff_30=xp_diff_per_time[29],
        )
        return self

    def convert_to_int(self):
        """Converts appropriate columns to int.

        Returns:
            object: PlayerData class instance.
        """
        to_int = [
            "dire_score",
            "radiant_score",
            "pings",
            "neutral_creeps",
            "lane_creeps",
        ]
        self.player_data[to_int] = self.player_data[to_int].astype("int")
        return self

    def get_highest_streak(self):
        """Replaces dict with its' max key indicating highest kill streak
        achieved by the player in a particular game.

        Returns:
            object: PlayerData class instance.
        """
        self.player_data["kill_streaks"] = (
            self.player_data["kill_streaks"]
            .apply(lambda x: max(x.keys()) if "3" in x.keys() else None)
            .astype("float")
        )
        self.player_data.rename(columns={"kill_streaks": "highest_ks"}, inplace=True)
        return self
    
    def clean_data(self):
        """Super-function to clean data from OpenDota."""
        self.clean_patch().clean_team().clean_league().clean_hero(
        ).clean_win().clean_start_time().clean_duration().clean_kda(
        ).clean_roaming().clean_side().clean_denies().clean_lh(
        ).clean_nw().clean_xp().clean_lane().clean_lane_neutral_kills(
        ).convert_to_int().get_highest_streak().clean_xp_diff(
        ).clean_gold_diff()


player = PlayerData("mind_control", '7.22')
player.get_data()
player.clean_data()

player.player_data.to_csv("data/mc_data.csv", index=False)

print("\nCleaned everything and copied data to separate file.")