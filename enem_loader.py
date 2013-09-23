# coding=UTF-8

from mongoengine import *

KNOWLEDGE_AREAS = ['nc', 'hc', 'lc', 'mt']

class ScoreStatistics(EmbeddedDocument):
    EMPTY_LIST = [0] * 10
    
    # Define a matrix of score counts per range and per knowledge area (ranges in columns and knowledge areas in rows).
    score_counts = ListField(ListField(IntField(min_value=0)))
    
    @classmethod
    def create_empty(cls):
        return cls(score_counts=[cls.EMPTY_LIST for _ in KNOWLEDGE_AREAS])

    def count(self):
        return reduce(sum, [reduce(sum, self.knowledge_area_row) for knowledge_area_row in self.score_counts])
    
    def percentages(self, knowledge_area):
        knowledge_area_score_counts = self.score_counts[KNOWLEDGE_AREAS.index(knowledge_area)]
        
        total_counts = reduce(sum, knowledge_area_score_counts)
        
        return map(lambda value: value * 1.0 / total_counts, knowledge_area_score_counts)

class School(Document):
    code  = StringField(max_length=8, required=True, unique=True)
    name  = StringField(max_length=255, required=True)
    city  = ReferenceField('City')
    stats = EmbeddedDocumentField(ScoreStatistics)

    meta = {
        'ordering': ['+name'],
        'indexes': ['city']
    }

    @classmethod
    def all(cls):
        return cls.objects
    
class City(Document):
    code  = StringField(max_length=7, required=True, unique=True)
    name  = StringField(max_length=255, required=True)
    state = ReferenceField('State')
    stats = EmbeddedDocumentField(ScoreStatistics)

    meta = {
        'ordering': ['+name'],
        'indexes': ['state']
    }

    def schools(self):
        return School.objects(city=self.id)

    def full_name(self):
        return self.name + "-" + self.state.abbreviation

    @classmethod
    def all(cls):
        return cls.objects.order_by('name')
        
class State(Document):
    abbreviation = StringField(max_length=2, required=True, unique=True)
    stats        = EmbeddedDocumentField(ScoreStatistics)

    meta = {
        'ordering': ['+abbreviation']
    }

    def cities(self):
        return City.objects(state=self.id)
        
    @classmethod
    def all(cls):
        return cls.objects.order_by('abbreviation')

def parse_line(line):
    present_in_exam = { ka: (line[532 + i : 532 + i + 1] == '1')                                                  for i, ka in enumerate(KNOWLEDGE_AREAS) }
    scores          = { ka: (float(line[536 + i * 9 : 536 + i * 9 + 9].strip()) if present_in_exam[ka] else None) for i, ka in enumerate(KNOWLEDGE_AREAS) }
    ranges          = { ka: (int(scores[ka] / 100.0001) + 1 if present_in_exam[ka] else None)                     for i, ka in enumerate(KNOWLEDGE_AREAS) }

    return {
        'scores': scores,
        'ranges': ranges,
        'subscription_code': line[0 : 0 + 12],                            
        'year': int(line[12 : 12 + 4]),                      
        'age': int(line[16 : 16 + 3]),                      
        'gender': int(line[19 : 19 + 1]),                      
        'school_code': line[203 : 203 + 8],                         
        'city': {
            'code': line[211 : 211 + 7],                         
            'name': line[218 : 218 + 150].strip().title()
        },
        'state': line[368 : 368 + 2]
    }

if __name__ == '__main__':
    DATA_FILE = '/Users/cassiano/projects/python/inep_microdata/sql/data/enem_estado_sao_paulo.txt'

    connect('inep_scores4')

    School.drop_collection()
    City.drop_collection()
    State.drop_collection()

    with open(DATA_FILE) as f:
        state = city = school = None

        for i, line in enumerate(f):
            if i % 200 == 0: print(i)
            # if i > 2000: break

            row = parse_line(line)
            
            # print(row)

            # If state non-existent or distinct from current state, get or create it.
            if not state or row['state'] != state.abbreviation:
                state, _ = State.objects.get_or_create(abbreviation=row['state'])
            
            # If city non-existent or distinct from current city, get or create it.
            if not city or row['city']['code'] != city.code:
                city, _ = City.objects.get_or_create(code=row['city']['code'], defaults={ 'name': row['city']['name'], 'state': state })

            # If school non-existent or distinct from current school, get or create it.
            if not school or row['school_code'] != school.code:
                school, _ = School.objects.get_or_create(
                    code=row['school_code'], 
                    defaults={
                        'name': 'Escola ID=%s da cidade de %s-%s' % (row['school_code'], row['city']['name'], row['state']), 
                        'city': city, 
                        'stats': ScoreStatistics.create_empty()
                    }
                )

            for i, knowledge_area in enumerate(KNOWLEDGE_AREAS):
                range_value = row['ranges'][knowledge_area]
                
                if range_value: 
                    school.stats.score_counts[i][range_value - 1] += 1

            school.save()
