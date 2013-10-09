# coding=UTF-8

from mongoengine import *

KNOWLEDGE_AREAS = ['nc', 'hc', 'lc', 'mt']

class DocumentHelpers(object):
    @classmethod
    def find(cls, **kwargs):
        return cls.objects(**kwargs)

    @classmethod
    def all(cls):
        return cls.objects

class Year(Document, DocumentHelpers):
    value = IntField(min_value=2000, required=True, unique=True)
    
class ScoreStatistics(EmbeddedDocument):
    # Matrix of score counts per range and per knowledge area (ranges in columns and knowledge areas in rows).
    score_counts = ListField(ListField(IntField(min_value=0, required=True)))
    
    @classmethod
    def create_empty(cls):
        return cls(score_counts=[[0] * 10 for _ in KNOWLEDGE_AREAS])

class School(Document, DocumentHelpers):
    code  = StringField(max_length=8, required=True, unique=True)
    name  = StringField(max_length=255, required=True)
    city  = ReferenceField('City', required=True)
    stats = MapField(EmbeddedDocumentField(ScoreStatistics))        # Hash with year keys.

    meta = {
        'ordering': ['+name'],
        'indexes': ['city']
    }

class City(Document, DocumentHelpers):
    code  = StringField(max_length=7, required=True, unique=True)
    name  = StringField(max_length=255, required=True)
    state = ReferenceField('State', required=True)
    stats = MapField(EmbeddedDocumentField(ScoreStatistics))

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
        
class State(Document, DocumentHelpers):
    abbreviation = StringField(max_length=2, required=True, unique=True)
    stats        = MapField(EmbeddedDocumentField(ScoreStatistics))

    meta = {
        'ordering': ['+abbreviation']
    }

    def cities(self):
        return City.objects(state=self.id)
        
    @classmethod
    def all(cls):
        return cls.objects.order_by('abbreviation')

if __name__ == '__main__':
    LINE_SIZE        = 1180
    TOTAL_PRINTS     = 40
    SCHOOLS_CSV_FILE = '/Users/cassiano/projects/python/inep_microdata/sql/data/escolas_estado_sao_paulo.csv'

    import sys, mongoengine, os, csv
    from multiprocessing import Process
    from retry_decorator import Retry

    @Retry(100, delay=0.01, exceptions=(mongoengine.errors.NotUniqueError))
    def get_or_create_state(abbreviation):
        return State.objects.get_or_create(abbreviation=abbreviation)[0]

    @Retry(100, delay=0.01, exceptions=(mongoengine.errors.NotUniqueError))
    def get_or_create_city(code, defaults):
        return City.objects.get_or_create(code=code, defaults=defaults)[0]

    @Retry(100, delay=0.01, exceptions=(mongoengine.errors.NotUniqueError))
    def get_or_create_school(code, defaults):
        return School.objects.get_or_create(code=code, defaults=defaults)[0]

    @Retry(100, delay=0.01, exceptions=(mongoengine.errors.NotUniqueError))
    def get_or_create_year(value):
        return Year.objects.get_or_create(value=value)[0]

    def load_schools_from_csv_file(schools_csv_file):
        schools = {}

        with open(schools_csv_file, 'rb') as csvfile:
            reader = csv.reader(csvfile, delimiter=',')

            for row in reader:
                schools.update({ row[3]: row[4] })
                
        return schools

    def parse_line(line):
        def get_string(line, start, size, emptyValue = '.'):
            value = line[start: start + size].strip()

            return value if value != emptyValue else None

        present_in_exam = { ka: (int(get_string(line, 532 + i, 1)) == 1)                                   for i, ka in enumerate(KNOWLEDGE_AREAS) }
        scores          = { ka: (float(get_string(line, 536 + i * 9, 9)) if present_in_exam[ka] else None) for i, ka in enumerate(KNOWLEDGE_AREAS) }
        ranges          = { ka: (int(scores[ka] / 100.0001) + 1          if present_in_exam[ka] else None) for i, ka in enumerate(KNOWLEDGE_AREAS) }

        city_name = get_string(line, 218, 150)
        city_name = city_name and city_name

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

    def process_data_file(data_file, process_num, schools):
        print('[% 3d] Processing file %s...' % (process_num, data_file))

        file_size = os.path.getsize(data_file)
        total_lines = file_size / LINE_SIZE

        with open(data_file) as f:
            state = city = school = None

            for i, line in enumerate(f):
                if i % (total_lines / TOTAL_PRINTS) == 0: print(' ' * process_num * 4 + '[%d] %.1f%%' % (process_num, i * 100.0 / total_lines))
                # if i > 10: break

                row = parse_line(line)

                # print(row)

                # State non-existent or distinct from current state?
                if not state or row['state'] != state.abbreviation:
                    if row['state']:
                        state = get_or_create_state(row['state'])
                    else:
                        state = None

                # City non-existent or distinct from current city?
                if not city or row['city']['code'] != city.code:
                    if row['city']['code']:
                        city = get_or_create_city(row['city']['code'], { 'name': row['city']['name'], 'state': state })
                    else:
                        city = None

                # School non-existent or distinct from current school?
                if not school or row['school_code'] != school.code:
                    if row['school_code']:
                        if row['school_code'] in schools:
                            name = schools[row['school_code']]
                        else:
                            name = 'ESCOLA %s (%s-%s)' % (row['school_code'], row['city']['name'], row['state'])
                        
                        school = get_or_create_school(
                            row['school_code'],
                            {
                                'name': name,
                                'city': city, 
                                'stats': { str(row['year']): ScoreStatistics.create_empty() }
                            }
                        )
                    else:
                        school = None
        
                if school:
                    kwargs = {}
                
                    for i, knowledge_area in enumerate(KNOWLEDGE_AREAS):
                        range_value = row['ranges'][knowledge_area]
                    
                        if range_value: 
                            kwargs.update({ 'inc__stats__%d__score_counts__%d__%d' % (row['year'], i, range_value - 1): 1 })
                
                    if kwargs != {}:
                        School.find(id=school.id).update_one(**kwargs)

                        get_or_create_year(row['year'])

    if len(sys.argv) < 3: 
        raise Exception('Usage: %s <db-name> <data-files>' % sys.argv[0])
    
    last_option_is_true = sys.argv[-1].lower() == 'true'
    db_name             = sys.argv[1]
    drop_collections    = last_option_is_true
    data_files          = sys.argv[2:-1] if last_option_is_true else sys.argv[2:]
    
    schools = load_schools_from_csv_file(SCHOOLS_CSV_FILE)
    
    connect(db_name)

    if drop_collections:
        print('>>> Dropping collections...')
        
        School.drop_collection()
        City.drop_collection()
        State.drop_collection()

    children = []

    for i in range(len(data_files)):
        pid = Process(target=process_data_file, args=(data_files[i], i, schools))
        children.append(pid)
        pid.start()
        
    print('>>> Waiting for child processes to complete...')
    
    for i, child in enumerate(children):
        child.join()
        print('>>> Process %d (%s) joined.' % (i, child.pid))

    print('>>> Done!')
