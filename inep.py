# -*- coding: UTF-8 -*-

# Imports.
from flask import Flask, request, render_template, jsonify
from ETL.enem_loader import *

# Configuration.
DB_NAME = 'enem_5'

# Application initialization.
app = Flask(__name__)
app.config.from_object(__name__)
app.config['DEBUG'] = True
connect(DB_NAME)

# Constants.
ENEM_KNOWLEDGE_AREAS = ['NAT', 'HUM', 'LAN', 'MAT']

def find_stats(object, year, enem_knowledge_area):
    def __enem_knowledge_area_index(enem_knowledge_area):
        return ENEM_KNOWLEDGE_AREAS.index(enem_knowledge_area.upper())

    return object.stats[year].score_counts[__enem_knowledge_area_index(enem_knowledge_area)]

def json_stats(stats):
    return jsonify([[i + 1, count] for i, count in enumerate(stats)])

##############################
# Schools routes
##############################

@app.route("/schools/<school_id>/aggregated_scores/<year>/<enem_knowledge_area>.json")
def aggregated_scores_by_school(school_id, year, enem_knowledge_area):
    school = School.find(id=school_id).first()
    stats  = find_stats(school, year, enem_knowledge_area)
    
    return json_stats(stats)

##############################
# Cities routes
##############################

@app.route("/cities/<city_id>/schools/search.json")
def search_schools_in_city(city_id):
    term = request.args.get('term', '')
    city = City.find(id=city_id).first()
    
    return jsonify({ 'schools': [{ 'id': str(s.id), 'value': s.name.title() } for s in School.find(city=city, name__icontains=term)] })

@app.route("/cities/<city_id>/aggregated_scores/<year>/<enem_knowledge_area>.json")
def aggregated_scores_by_city(city_id, year, enem_knowledge_area):
    city  = City.find(id=city_id).first()
    stats = find_stats(city, year, enem_knowledge_area)
    
    return json_stats(stats)

# ##############################
# # States routes
# ##############################

@app.route("/states/<state_id>/cities/search.json")
def search_cities_in_state(state_id):
    term  = request.args.get('term', '')
    state = State.find(id=state_id).first()
    
    return jsonify({ 'cities': [{ 'id': str(s.id), 'value': s.name.title() } for s in City.find(state=state, name__icontains=term)] })

@app.route("/states/<state_id>/aggregated_scores/<year>/<enem_knowledge_area>.json")
def aggregated_scores_by_state(state_id, year, enem_knowledge_area):
    state = State.find(id=state_id).first()
    stats = find_stats(state, year, enem_knowledge_area)
    
    return json_stats(stats)

##############################
# Root route
##############################

@app.route('/')
def root():
    states = State.all()
    years  = [2011]
    
    return render_template('index.html', states=states, years=years)
