// PS: locality can be either a school, city, state or even the whole country!
function reduceFunction(locality, values) {
    var counts = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

    for (var i = 0; i < values.length; i++) {
        for (var j = 0; j < values[i].counts.length; j++) {
            counts[j] += values[i].counts[j];
        }
    }

    return { counts: counts };
};

function mapFunction() {
    var counts = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

    counts[this.ranges[data.enemKnowledgeArea] - 1] = 1;

    emit('locality', { counts: counts });
};

function mapReduceFn(query, enemKnowledgeArea) {
	return db.subscriptions.mapReduce(
	    mapFunction,
	    reduceFunction,
	    {
	         query: query,
	         out: { inline: 1 },
			 scope: {
				 data: { enemKnowledgeArea: enemKnowledgeArea }
			 }
	    }
	);
};

function cityAggregates(city_code, year, enemKnowledgeArea) {
	var query = { 'city.code': city_code, year: year };
	query['ranges.' + enemKnowledgeArea] = { $ne: null };

	return mapReduceFn(query, enemKnowledgeArea);
};

function stateAggregates(stateAbbreviation, year, enemKnowledgeArea) {
	var query = { state: stateAbbreviation, year: year };
	query['ranges.' + enemKnowledgeArea] = { $ne: null };

	return mapReduceFn(query, enemKnowledgeArea);
};

function countryAggregates(year, enemKnowledgeArea) {
	var query = { year: year };
	query['ranges.' + enemKnowledgeArea] = { $ne: null };

	return mapReduceFn(query, enemKnowledgeArea);
};

var spCityAggregates = cityAggregates('3550308', 2011, 'NAT');
var spStateAggregates = stateAggregates('SP', 2011, 'NAT');
var brCountryAggregates = countryAggregates(2011, 'NAT');
