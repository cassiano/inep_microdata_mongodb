# coding=UTF-8

# To import into MongoDB: time mongoimport --host localhost:27017 --db enem_subscriptions --collection subscriptions --drop <~/tmp/inep_data.json

import sys, json, os

KNOWLEDGE_AREAS = ['NAT', 'HUM', 'LAN', 'MAT']

def parse_line(line):
    def get_string(line, start, size, empty_value = '.'):
        value = line[start: start + size].strip()

        return value if value != empty_value else None

    present_in_exam = {
        ka: (int(get_string(line, 532 + i, 1)) == 1)
        for i, ka in enumerate(KNOWLEDGE_AREAS)
    }
    scores = {
        ka: (float(get_string(line, 536 + i * 9, 9)) if present_in_exam[ka] else None)
        for i, ka in enumerate(KNOWLEDGE_AREAS)
    }
    ranges = {
        ka: (int(scores[ka] / 100.0001) + 1 if present_in_exam[ka] else None)
        for i, ka in enumerate(KNOWLEDGE_AREAS)
    }

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
            'name': get_string(line, 218, 150)
        },
        'state': get_string(line, 368, 2)
    }

def subscriptions(data_file):
    with open(data_file) as infile:
        for i, line in enumerate(infile):
            yield i, parse_line(line)

if __name__ == '__main__':
    def display_progress(current_index, total_lines):
        return current_index % (total_lines / TOTAL_PRINTS) == 0 or current_index == total_lines - 1

    LINE_SIZE        = 1180
    TOTAL_PRINTS     = 100
    OUTPUT_JSON_FILE = "/Users/cassiano/tmp/inep_data.json"

    if len(sys.argv) < 2:
        raise Exception('Usage: %s <data-file>' % sys.argv[0])

    data_file = sys.argv[1]

    file_size = os.path.getsize(data_file)
    total_lines = file_size / LINE_SIZE

    with open(OUTPUT_JSON_FILE, "w") as outfile:
        for i, subscription in subscriptions(data_file):
            if display_progress(i, total_lines):
                progress = i * 100.0 / (total_lines - 1)
                print('%.1f%%' % progress)

            # if i > 10: break

            outfile.write(json.dumps(subscription) + '\n')
