import unittest, sys
from enem_loader_with_fork import *

class TestMongoData(unittest.TestCase):
    def setUp(self):
        pass

    def test_city_of_sao_paulo_exists(self):
        city = City.objects(name='Sao Paulo').first()

        self.assertIsNotNone(city)

    def test_city_of_sao_paulo_has_correct_stats(self):
        city = City.objects(name='Sao Paulo').first()

        self.assertEqual(city.stats.score_counts, [
            [0, 0, 922, 11784, 25980, 18297, 4895, 694, 31, 0], 
            [0, 0, 550, 9651, 25477, 21841, 4797, 287, 0, 0], 
            [0, 0, 0, 2890, 14056, 31746, 12528, 420, 0, 0], 
            [0, 0, 0, 9088, 16880, 15827, 12252, 5759, 1692, 142]
        ])

    def test_state_of_sao_paulo_exists(self):
        state = State.objects(abbreviation='SP').first()

        self.assertIsNotNone(state)

    def test_state_of_sao_paulo_has_correct_stats(self):
        state = State.objects(abbreviation='SP').first()

        self.assertEqual(state.stats.score_counts, [
            [0, 0, 3752, 50655, 114519, 77272, 16867, 1981, 66, 0], 
            [0, 0, 2456, 43516, 112529, 90034, 15865, 712, 0, 0], 
            [0, 0, 0, 14125, 64438, 135717, 43548, 1130, 0, 0], 
            [0, 0, 0, 37081, 72330, 70094, 53610, 20806, 4708, 329]
        ])

if __name__ == '__main__':
    if len(sys.argv) < 2: 
        raise Exception('Usage: %s <db-name>' % sys.argv[0])
    
    db_name = sys.argv[1]
    connect(db_name)

    unittest.main(argv=[sys.argv[0]])
