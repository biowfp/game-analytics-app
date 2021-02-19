## About
____
The goal of the project is to allow DOTA 2 players to get stats and analysis of performance for their favourite teams and players without needing to collect and work with the data themselves. Right now it’s really easy to access the data and not at all (for average people) - to see what that data means.

## Usage
____
Clone the repository, cd into the directory and run an opendota.py.

`cd ...\dota-project`

`python opendota.py`

To change the player or minimal patch (programm requests data up to current patch for now) go to the bottom of the file and find a row:
```python
player = PlayerData("mind_control", "7.22")
```
Input your desired player (make sure you spell nickname correctly) and minimal patch then run the file.
