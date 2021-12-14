import pandas as pd
import streamlit as st
import requests
import time

from service.player_data import PlayerData
from service.cleaner import DataCleaner
from utils.helpers import get_patches_data

st.title("That's gonna be dota analysis app")

# SIDEBAR
# add input box for player name and patch
player_name = st.sidebar.text_input(
    "Enter player's name (case_insensitive, like 'mind_control')")
min_patch = st.sidebar.text_input(
    "Enter minimal patch here, like 7.27 (App requests data up to last match played, starting from that patch)")  # make slider (min-max)
patch_data = get_patches_data()
run = st.sidebar.button('Run')


# get data of a player
@st.cache
def get_data(player_name, min_patch) -> None:
    """Super-function to acquire data from OpenDota."""
    player = PlayerData(player=player_name, min_patch=min_patch)

    player.get_player_id(
    ).get_match_ids(
    ).get_matches_data(
    ).get_player_stats(
    ).merge_player_data_with_match()

    return player.player_data


def clean_data(data, patch_data):
    cleaner = DataCleaner()
    clean_data = (
        data
        .pipe(cleaner.clean_patch, patch_data)
        .pipe(cleaner.clean_team)
        .pipe(cleaner.clean_league)
        .pipe(cleaner.clean_hero)
        .pipe(cleaner.clean_win)
        .pipe(cleaner.clean_start_time)
        .pipe(cleaner.clean_duration)
        .pipe(cleaner.clean_kda)
        .pipe(cleaner.clean_roaming)
        .pipe(cleaner.clean_player_slot)
        .pipe(cleaner.clean_dn_t)
        .pipe(cleaner.clean_lh_t)
        .pipe(cleaner.clean_gold_t)
        .pipe(cleaner.clean_xp_t)
        .pipe(cleaner.clean_lane)
        .pipe(cleaner.clean_lane_neutral_kills)
        .pipe(cleaner.convert_to_int)
        .pipe(cleaner.get_highest_streak)
        .pipe(cleaner.clean_xp_adv)
        .pipe(cleaner.clean_gold_adv)
    )

    return clean_data


if run:
    if player_name and min_patch:
        player_data = get_data(player_name, min_patch)
        cleaned_data = clean_data(player_data.copy(), patch_data)

        st.write("Data sample after cleaning")
        st.write(cleaned_data.sample())
    else:
        st.sidebar.warning('Input necessary data, please')
