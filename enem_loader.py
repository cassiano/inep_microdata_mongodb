# coding=UTF-8

DATA_FILE = '/Users/cassiano/projects/python/inep_microdata/sql/data/enem_estado_sao_paulo.txt'

from mongoengine import *

connect('enem_scores')

# def find(f, seq):
#   """Return first item in sequence where f(item) == True."""
#   for item in seq:
#     if f(item): 
#       return item

class ScoreStatistics(EmbeddedDocument):
    EMPTY_LIST = [0] * 10
    
    cn = ListField(IntField(min_value=0))
    ch = ListField(IntField(min_value=0))
    lc = ListField(IntField(min_value=0))
    mt = ListField(IntField(min_value=0))
    
    def count(self):
        def sum(a, b): return a + b
        
        return reduce(sum, self.cn) + reduce(sum, self.ch) + reduce(sum, self.lc) + reduce(sum, self.mt)
    
    def percentages(self, knowledge_area):
        data = self.__dict__['_data'][knowledge_area]
        
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
    
class City(Document):
    code    = StringField(max_length=7, required=True)
    name    = StringField(max_length=255, required=True)
    state   = ReferenceField('State')
    stats   = EmbeddedDocumentField(ScoreStatistics)

    meta = {
        'indexes': ['code']
    }

    def schools(self):
        return School.objects(city=self.id)

    def full_name(self):
        return self.name + "-" + self.state.abbreviation
        
class State(Document):
    abbreviation = StringField(max_length=2, required=True)
    stats        = EmbeddedDocumentField(ScoreStatistics)

    meta = {
        'indexes': ['abbreviation']
    }

    def cities(self):
        return City.objects(state=self.id)

if __name__ == '__main__':
    School.drop_collection()
    City.drop_collection()
    State.drop_collection()

    with open(DATA_FILE) as f:
        i = 0
        state = city = school = None

        for line in f:
            i += 1
        
            # if i > 2000: break

            present_in_nature_sciences_exam     = line[532:(532+1)] == '1'
            present_in_human_sciences_exam      = line[533:(533+1)] == '1'
            present_in_languages_and_codes_exam = line[534:(534+1)] == '1'
            present_in_math_exam                = line[535:(535+1)] == '1'

            nature_sciences_score     = float(line[536:(536+9)].strip()) if present_in_nature_sciences_exam      else None
            human_sciences_score      = float(line[545:(545+9)].strip()) if present_in_human_sciences_exam       else None
            languages_and_codes_score = float(line[554:(554+9)].strip()) if present_in_languages_and_codes_exam  else None
            math_score                = float(line[563:(563+9)].strip()) if present_in_math_exam                 else None

            nature_sciences_range     = int(nature_sciences_score     / 100.0001) + 1 if present_in_nature_sciences_exam      else None;
            human_sciences_range      = int(human_sciences_score      / 100.0001) + 1 if present_in_human_sciences_exam       else None;
            languages_and_codes_range = int(languages_and_codes_score / 100.0001) + 1 if present_in_languages_and_codes_exam  else None;
            math_range                = int(math_score                / 100.0001) + 1 if present_in_math_exam                 else None;
        
            subscription_code  = line[0:(0+12)]
            year               = int(line[12:(12+4)])
            age                = int(line[16:(16+3)])
            gender             = int(line[19:(19+1)])
            school_code        = line[203:(203+8)]
            city_code          = line[211:(211+7)]
            city_name          = line[218:(218+150)].strip().title()
            state_abbreviation = line[368:(368+2)]

            if i % 200 == 0: print(i)
            
            # print('(%d) %d anos, sexo %d, escola cód. %s, cidade de %s-%s (cód. %s): CN=%s, CH=%s, LC=%s, MT=%s' % (
            #     i,
            #     age,
            #     gender,
            #     school_code,
            #     city_name,
            #     state_abbreviation,
            #     city_code,
            #     nature_sciences_score or '',
            #     human_sciences_score or '',
            #     languages_and_codes_score or '',
            #     math_score or ''
            # ))

            # If state non-existent or distinct from current state, find or create it.
            if not state or state_abbreviation != state.abbreviation:
                state = State.objects(abbreviation=state_abbreviation).first() or State(abbreviation=state_abbreviation).save()
            
            # If city non-existent or distinct from current city, find or create it.
            if not city or city_code != city.code:
                city = City.objects(code=city_code).first() or City(code=city_code, name=city_name, state=state).save()

            # If school non-existent or distinct from current school, find or create it.
            if not school or school_code != school.code:
                school = School.objects(code=school_code).first() or School(
                    code=school_code, 
                    name='Escola ID=%s da cidade de %s-%s' % (school_code, city_name, state_abbreviation), 
                    city=city, 
                    stats=ScoreStatistics(
                        cn=ScoreStatistics.EMPTY_LIST, 
                        ch=ScoreStatistics.EMPTY_LIST, 
                        lc=ScoreStatistics.EMPTY_LIST, 
                        mt=ScoreStatistics.EMPTY_LIST
                    )
                )

            if nature_sciences_range:     school.stats.cn[nature_sciences_range     - 1] += 1
            if human_sciences_range:      school.stats.ch[human_sciences_range      - 1] += 1
            if languages_and_codes_range: school.stats.lc[languages_and_codes_range - 1] += 1
            if math_range:                school.stats.mt[math_range                - 1] += 1

            school.save()
