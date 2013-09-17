# coding=UTF-8

from mongoengine import *

class ScoreStatistics(EmbeddedDocument):
    EMPTY_LIST = [0] * 10
    KNOWLEDGE_AREAS = ['nc', 'hc', 'lc', 'mt']
    
    values = DictField(ListField)
    
    @classmethod
    def create_empty(cls):
        return cls(values={knowledge_area: cls.EMPTY_LIST for knowledge_area in cls.KNOWLEDGE_AREAS })

    def count(self):
        # def sum(a, b): return a + b
        
        knowledge_areas_values = [self.values[knowledge_area] for knowledge_area in ScoreStatistics.KNOWLEDGE_AREAS]

        return reduce(sum, [value for values in knowledge_areas_values for value in values])
    
    def percentages(self, knowledge_area):
        data = self.values[knowledge_area]
        
        count = reduce(lambda a, b: a + b, data)
        
        return map(lambda v: v * 1.0 / count, data)

class School(Document):
    code  = StringField(max_length=8, required=True)
    name  = StringField(max_length=255, required=True)
    city  = ReferenceField('City')
    stats = EmbeddedDocumentField(ScoreStatistics)

    meta = {
        'indexes': ['code']
    }

    @classmethod
    def all(cls):
        return cls.objects.order_by('name')
    
class City(Document):
    code    = StringField(max_length=7, required=True)
    name    = StringField(max_length=255, required=True)
    state   = ReferenceField('State')
    stats   = EmbeddedDocumentField(ScoreStatistics)

    meta = {
        'indexes': ['code']
    }

    def schools(self):
        return School.objects(city=self.id).order_by('name')

    def full_name(self):
        return self.name + "-" + self.state.abbreviation

    @classmethod
    def all(cls):
        return cls.objects.order_by('name')
        
class State(Document):
    abbreviation = StringField(max_length=2, required=True)
    stats        = EmbeddedDocumentField(ScoreStatistics)

    meta = {
        'indexes': ['abbreviation']
    }

    def cities(self):
        return City.objects(state=self.id).order_by('name')
        
    @classmethod
    def all(cls):
        return cls.objects.order_by('abbreviation')

def parse_line(line):
    present_in_nc_exam = line[532:(532+1)] == '1'
    present_in_hc_exam = line[533:(533+1)] == '1'
    present_in_lc_exam = line[534:(534+1)] == '1'
    present_in_mt_exam = line[535:(535+1)] == '1'

    nc_score = float(line[536:(536+9)].strip()) if present_in_nc_exam else None
    hc_score = float(line[545:(545+9)].strip()) if present_in_hc_exam else None
    lc_score = float(line[554:(554+9)].strip()) if present_in_lc_exam else None
    mt_score = float(line[563:(563+9)].strip()) if present_in_mt_exam else None

    return {
        'scores': {
            'nc': nc_score,
            'hc': hc_score,   
            'lc': lc_score,
            'mt': mt_score
        },
        'ranges': {
            'nc': int(nc_score / 100.0001) + 1 if present_in_nc_exam else None,
            'hc': int(hc_score / 100.0001) + 1 if present_in_hc_exam else None,
            'lc': int(lc_score / 100.0001) + 1 if present_in_lc_exam else None,
            'mt': int(mt_score / 100.0001) + 1 if present_in_mt_exam else None
        },
        'subscription_code':  line[0:(0+12)],                            
        'year':               int(line[12:(12+4)]),                      
        'age':                int(line[16:(16+3)]),                      
        'gender':             int(line[19:(19+1)]),                      
        'school_code':        line[203:(203+8)],                         
        'city': {
            'code': line[211:(211+7)],                         
            'name': line[218:(218+150)].strip().title()
        },
        'state_abbreviation': line[368:(368+2)]                          
    }

if __name__ == '__main__':
    DATA_FILE = '/Users/cassiano/projects/python/inep_microdata/sql/data/enem_estado_sao_paulo.txt'

    connect('inep_scores')

    School.drop_collection()
    City.drop_collection()
    State.drop_collection()

    with open(DATA_FILE) as f:
        state = city = school = None

        for i, line in enumerate(f):
            if i % 200 == 0: print(i)
            # if i > 10: break

            row = parse_line(line)
            
            # print(row)

            # If state non-existent or distinct from current state, find or create it.
            if not state or row['state_abbreviation']  != state.abbreviation:
                state = State.objects(abbreviation=row['state_abbreviation']).first() or State(abbreviation=row['state_abbreviation']).save()
            
            # If city non-existent or distinct from current city, find or create it.
            if not city or row['city']['code'] != city.code:
                city = City.objects(code=row['city']['code']).first() or City(code=row['city']['code'], name=row['city']['name'], state=state).save()

            # If school non-existent or distinct from current school, find or create it.
            if not school or row['school_code'] != school.code:
                school = School.objects(code=row['school_code']).first() or School(
                    code=row['school_code'], 
                    name='Escola ID=%s da cidade de %s-%s' % (row['school_code'], row['city']['name'], row['state_abbreviation']), 
                    city=city, 
                    stats=ScoreStatistics.create_empty()
                )

            for knowledge_area in ScoreStatistics.KNOWLEDGE_AREAS:
                range_value = row['ranges'][knowledge_area]
                
                if range_value: 
                    school.stats.values[knowledge_area][range_value - 1] += 1

            school.save()
