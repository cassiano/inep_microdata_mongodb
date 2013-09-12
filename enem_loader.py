DATA_FILE = '/Users/cassiano/projects/python/inep_microdata/sql/data/enem_estado_sao_paulo.txt'

from mongoengine import *

connect('enem_scores')

def find(f, seq):
  """Return first item in sequence where f(item) == True."""
  for item in seq:
    if f(item): 
      return item

class ScoreStatsDetail(EmbeddedDocument):
    range = IntField(min_value=1, max_value=10)
    count = IntField(min_value=0)

class ScoreStats(EmbeddedDocument):
    count = IntField(min_value=0)
    cn    = ListField(EmbeddedDocumentField(ScoreStatsDetail))
    ch    = ListField(EmbeddedDocumentField(ScoreStatsDetail))
    lc    = ListField(EmbeddedDocumentField(ScoreStatsDetail))
    mt    = ListField(EmbeddedDocumentField(ScoreStatsDetail))
    
    def summary(self):
        return [{ 
            'cn': self.cn / self.count,
            'ch': self.ch / self.count,
            'lc': self.lc / self.count,
            'mt': self.mt / self.count,
        }]

class School(EmbeddedDocument):
    code  = StringField(max_length=8, required=True)
    name  = StringField(max_length=255, required=True)
    stats = EmbeddedDocumentField(ScoreStats)

class City(EmbeddedDocument):
    code    = StringField(max_length=7, required=True)
    name    = StringField(max_length=255, required=True)
    schools = ListField(EmbeddedDocumentField(School))
    stats   = EmbeddedDocumentField(ScoreStats)

class State(Document):
    abbreviation = StringField(max_length=2, required=True)
    cities       = ListField(EmbeddedDocumentField(City))
    stats        = EmbeddedDocumentField(ScoreStats)

if __name__ == '__main__':
    State.drop_collection()

    with open(DATA_FILE) as f:
        i = 0
        state = city = school = None

        for line in f:
            i += 1
            print(i)
        
            if i > 2000: break

            present_in_nature_sciences_exam     = line[532:(532+1)] == '1'
            present_in_human_sciences_exam      = line[533:(533+1)] == '1'
            present_in_languages_and_codes_exam = line[534:(534+1)] == '1'
            present_in_math_exam                = line[535:(535+1)] == '1'

            nature_sciences_score       = float(line[536:(536+9)].strip()) if present_in_nature_sciences_exam      else None
            human_sciences_score        = float(line[545:(545+9)].strip()) if present_in_human_sciences_exam       else None
            languages_and_codes_score   = float(line[554:(554+9)].strip()) if present_in_languages_and_codes_exam  else None
            math_score                  = float(line[563:(563+9)].strip()) if present_in_math_exam                 else None

            nature_sciences_range     = int(nature_sciences_score       / 100.0001) + 1 if present_in_nature_sciences_exam      else None;
            human_sciences_range      = int(human_sciences_score        / 100.0001) + 1 if present_in_human_sciences_exam       else None;
            languages_and_codes_range = int(languages_and_codes_score   / 100.0001) + 1 if present_in_languages_and_codes_exam  else None;
            math_range                = int(math_score                  / 100.0001) + 1 if present_in_math_exam                 else None;
        
            subscription_code  = line[0:(0+12)]
            year               = int(line[12:(12+4)])
            age                = int(line[16:(16+3)])
            gender             = int(line[19:(19+1)])
            school_code        = line[203:(203+8)]
            city_code          = line[211:(211+7)]
            city_name          = line[218:(218+150)].strip().title()
            state_abbreviation = line[368:(368+2)]

            # If state non-existent or distinct from current state, find or create it.
            if not state or state_abbreviation != state.abbreviation:
                state = State.objects(abbreviation=state_abbreviation).first() or State(abbreviation=state_abbreviation)
            
            # If city non-existent or distinct from current city, find or create it.
            if not city or city_code != city.code:
                city = find(lambda c: c.code == city_code, state.cities)
                if not city: 
                    city = City(code=city_code, name=city_name)
                    state.cities.append(city)

            # If school non-existent or distinct from current school, find or create it.
            if not school or school_code != school.code:
                school = find(lambda s: s.code == school_code, city.schools)
                if not school: 
                    school = School(code=school_code, name='Escola ID=%s da cidade de %s-%s' % (school_code, city_name, state_abbreviation))
                    city.schools.append(school)

            # if not city.stats: 
            #     city.stats = ScoreStats(count=0)
            # 
            # if math_range:
            #     stats_detail = find(lambda sd: sd.range == math_range, city.stats.mt)
            # 
            #     if not stats_detail:
            #         stats_detail = ScoreStatsDetail(range=math_range, count=1)
            #         city.stats.mt.append(stats_detail)
            #     else:
            #         stats_detail.count += 1
            #     
            #     city.stats.count += 1

            if not school.stats: 
                school.stats = ScoreStats(count=0)

            if math_range:
                stats_detail = find(lambda sd: sd.range == math_range, school.stats.mt)

                if not stats_detail:
                    stats_detail = ScoreStatsDetail(range=math_range, count=1)
                    school.stats.mt.append(stats_detail)
                else:
                    stats_detail.count += 1
                
                school.stats.count += 1

            # Save the state.
            state.save()    # Has no effect if state already saved or unchanged.
