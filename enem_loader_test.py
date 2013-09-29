import unittest, sys
from enem_loader import *

class TestMongoData(unittest.TestCase):
    def setUp(self):
        pass

    def test_sao_paulo_city_exists(self):
        city = City.objects(name='Sao Paulo').first()

        self.assertIsNotNone(city)

    def test_sao_paulo_city_has_correct_stats(self):
        city = City.objects(name='Sao Paulo').first()

        self.assertEqual(city.stats.score_counts, [
            [0, 0, 922, 11784, 25980, 18297, 4895, 694, 31, 0], 
            [0, 0, 550, 9651, 25477, 21841, 4797, 287, 0, 0], 
            [0, 0, 0, 2890, 14056, 31746, 12528, 420, 0, 0], 
            [0, 0, 0, 9088, 16880, 15827, 12252, 5759, 1692, 142]
        ])

    def test_sp_state_exists(self):
        state = State.objects(abbreviation='SP').first()

        self.assertIsNotNone(state)

    def test_sp_state_has_correct_stats(self):
        state = State.objects(abbreviation='SP').first()

        self.assertEqual(state.stats.score_counts, [
            [0, 0, 3752, 50655, 114519, 77272, 16867, 1981, 66, 0], 
            [0, 0, 2456, 43516, 112529, 90034, 15865, 712, 0, 0], 
            [0, 0, 0, 14125, 64438, 135717, 43548, 1130, 0, 0], 
            [0, 0, 0, 37081, 72330, 70094, 53610, 20806, 4708, 329]
        ])

    def test_rj_state_exists(self):
        state = State.objects(abbreviation='RJ').first()

        self.assertIsNotNone(state)

    def test_rj_state_has_correct_stats(self):
        state = State.objects(abbreviation='RJ').first()

        self.assertEqual(state.stats.score_counts, [
            [0, 0, 1152, 15803, 37535, 29478, 8479, 931, 22, 0], 
            [0, 0, 709, 12631, 35934, 35206, 8469, 451, 0, 0], 
            [0, 0, 0, 4239, 20276, 48549, 18431, 524, 0, 0], 
            [0, 0, 0, 11712, 24027, 24527, 20097, 9273, 2208, 175]
        ])

    def test_ecc_school_exists(self):
        school = School.objects(code='35112689').first()

        self.assertIsNotNone(school)

    def test_ecc_school_has_correct_stats(self):
        school = School.objects(code='35112689').first()

        self.assertEqual(school.stats.score_counts, [
            [0, 0, 0, 0, 2, 19, 42, 7, 0, 0], 
            [0, 0, 0, 0, 1, 18, 47, 4, 0, 0], 
            [0, 0, 0, 0, 0, 9, 58, 3, 0, 0], 
            [0, 0, 0, 0, 2, 0, 12, 39, 17, 0]
        ])

    def test_entity_counts_match(self):
        self.assertEqual([
            State.objects.count(), City.objects.count(), School.objects.count()
        ], [
            27, 5552, 31069
        ])

if __name__ == '__main__':
    if len(sys.argv) < 2: 
        raise Exception('Usage: %s <db-name>' % sys.argv[0])
    
    db_name = sys.argv[1]
    connect(db_name)

    unittest.main(argv=[sys.argv[0]])
