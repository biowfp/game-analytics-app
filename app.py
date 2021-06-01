import streamlit as st
import time

from service.player_data import PlayerData
from service.cleaner import DataCleaner, clean_data

st.title("That's gonna be dota analysis app")

# SIDEBAR
# add input box for player name and patch
player_name = st.sidebar.text_input(
    "Enter player's name (case_insensitive, like 'mind_control')")
min_patch = st.sidebar.text_input(
    "Enter minimal patch here, like 7.27 (App requests data up to last match played, starting from that patch)")  # make slider (min-max)
run = st.sidebar.button('Run')

# get data of a player
@st.cache(suppress_st_warning=True)
def get_clean_data(**kwargs):
    player = PlayerData(player="mind_control", min_patch="7.22")
    player.get_data()
    clean_data(player.player_data)
    return player


if run:
    if player_name and min_patch:
        player = get_clean_data()
        st.write("Data sample after cleaning")
        st.write(player.player_data.sample())
    else:
        st.sidebar.warning('Input necessary data, please')




#from player_data file
if __name__ == "__main__":
    player = PlayerData(player="mind_control", min_patch="7.22")
    player.get_data()
    player.player_data.to_csv("data/mc_data_raw.csv", index=False)

    player.clean_data()

    player.player_data.to_csv("data/mc_data.csv", index=False)
    print("\nCleaned everything and copied data to a separate file.")

