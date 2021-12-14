import unittest

from service.player_data import PlayerData


class TestGetter(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.player = PlayerData('mind_control')

    def test_get_player_id(self):
        self.player.get_player_id()
        self.assertIsInstance(self.player.player_id, str, 'not a string')
        self.assertNotEqual(self.player.player_id, 'None', 'player id is none')

    def test_get_match_ids(self):
        self.player.get_match_ids()
        self.assertIsInstance(self.player.match_ids, list, 'match ids is not a list')
        self.assertIsInstance(self.player.match_ids[0], int, 'first match id is not an int')


if __name__ == "__main__":
    unittest.main()
