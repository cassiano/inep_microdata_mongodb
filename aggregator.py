# coding=UTF-8

from enem_loader import *

if __name__ == '__main__':
    connect('inep_scores')

    for i, state in enumerate(State.all()):
        print('% 1d. ' % (i + 1) + state.abbreviation)

        state.stats = ScoreStatistics.create_empty()

        for j, city in enumerate(state.cities()):
            print(' ' * 2 + '% 3d. ' % (j + 1) + city.name)
            
            city.stats = ScoreStatistics.create_empty()
            
            for k, school in enumerate(city.schools()):
                print(' ' * 6 + '% 4d. ' % (k + 1) + school.code)
                
                for knowledge_area in ScoreStatistics.KNOWLEDGE_AREAS:
                    for m in range(len(ScoreStatistics.EMPTY_LIST)):
                        city.stats.values[knowledge_area][m] += school.stats.values[knowledge_area][m]

            city.save()

            for knowledge_area in ScoreStatistics.KNOWLEDGE_AREAS:
                for m in range(len(ScoreStatistics.EMPTY_LIST)):
                    state.stats.values[knowledge_area][m] = city.stats.values[knowledge_area][m]

                # state.stats.values[knowledge_area] = map(
                #     sum, 
                #     zip(state.stats.values[knowledge_area], city.stats.values[knowledge_area])
                # )
                
        state.save()
