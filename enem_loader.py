# coding=UTF-8

# To span multiple processes: find data_269043/* | xargs -I fn ./run.command python `pwd`/enem_loader.py inep_scores `pwd`/fn

from mongoengine import *
import time

KNOWLEDGE_AREAS = ['nc', 'hc', 'lc', 'mt']

class Retry(object):
    default_exceptions = (Exception)
    def __init__(self, tries, exceptions=None, delay=0):
        """
        Decorator for retrying function if exception occurs
        
        tries -- num tries
        exceptions -- exceptions to catch
        delay -- wait between retries
        """
        self.tries = tries
        if exceptions is None:
            exceptions = Retry.default_exceptions
        self.exceptions = exceptions
        self.delay = delay

    def __call__(self, f):
        def fn(*args, **kwargs):
            exception = None
            for _ in range(self.tries):
                try:
                    return f(*args, **kwargs)
                except self.exceptions, e:
                    print "Retry, exception: "+str(e)
                    time.sleep(self.delay)
                    exception = e
            # If no success after tries, raise last exception.
            raise exception
        return fn

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
    def get_string(line, start, size, emptyValue = '.'):
        value = line[start: start + size].strip()

        return value if value != emptyValue else None

    present_in_exam = { ka: (int(get_string(line, 532 + i, 1)) == 1)                                   for i, ka in enumerate(KNOWLEDGE_AREAS) }
    scores          = { ka: (float(get_string(line, 536 + i * 9, 9)) if present_in_exam[ka] else None) for i, ka in enumerate(KNOWLEDGE_AREAS) }
    ranges          = { ka: (int(scores[ka] / 100.0001) + 1          if present_in_exam[ka] else None) for i, ka in enumerate(KNOWLEDGE_AREAS) }

    city_name = get_string(line, 218, 150)
    city_name = city_name and city_name.title()

    return {
        'scores': scores,
        'ranges': ranges,
        'subscription_code': get_string(line, 0, 12),
        'year': int(get_string(line, 12, 4)),
        'age': int(get_string(line, 16, 3)),
        'gender': int(get_string(line, 19, 1)),
        'school_code': get_string(line, 203, 8),
        'city': {
            'code': get_string(line, 211, 7),
            'name': city_name
        },
        'state': get_string(line, 368, 2)
    }

if __name__ == '__main__':
    @Retry(10, delay=1)
    def get_or_create_state(abbreviation):
        return State.objects.get_or_create(abbreviation=row['state'])[0]

    @Retry(10, delay=1)
    def get_or_create_city(code, defaults):
        return City.objects.get_or_create(code=code, defaults=defaults)[0]

    @Retry(10, delay=1)
    def get_or_create_school(code, defaults):
        return School.objects.get_or_create(code=code, defaults=defaults)[0]

    import sys

    if len(sys.argv) < 3: 
        raise Exception('Usage: %s <db-name> <data-file> <drop-collections>' % sys.argv[0])
        
    db_name          = sys.argv[1]
    data_file        = sys.argv[2]
    drop_collections = False if len(sys.argv) < 4 else sys.argv[3].lower() == 'true'
    
    connect(db_name)

    if drop_collections:
        print('---- Dropping collections ----')
        School.drop_collection()
        City.drop_collection()
        State.drop_collection()

    with open(data_file) as f:
        state = city = school = None

        for i, line in enumerate(f):
            if i % 200 == 0: print(i)
            # if i > 2000: break

            row = parse_line(line)
            
            # print(row)

            # If state non-existent or distinct from current state, get or create it.
            if not state or row['state'] != state.abbreviation:
                if row['state']:
                    state = get_or_create_state(row['state'])
                else:
                    state = None
            
            # If city non-existent or distinct from current city, get or create it.
            if not city or row['city']['code'] != city.code:
                if row['city']['code']:
                    city = get_or_create_city(row['city']['code'], { 'name': row['city']['name'], 'state': state })
                else:
                    city = None

            # If school non-existent or distinct from current school, get or create it.
            if not school or row['school_code'] != school.code:
                if row['school_code']:
                    school = get_or_create_school(
                        row['school_code'],
                        {
                            'name': 'Escola ID=%s da cidade de %s-%s' % (row['school_code'], row['city']['name'], row['state']), 
                            'city': city, 
                            'stats': ScoreStatistics.create_empty()
                        }
                    )
                else:
                    school = None
                    
                    # No school code provided! Get next student subscription.
                    continue

            for i, knowledge_area in enumerate(KNOWLEDGE_AREAS):
                range_value = row['ranges'][knowledge_area]
                
                if range_value: 
                    school.stats.score_counts[i][range_value - 1] += 1

            school.save()
