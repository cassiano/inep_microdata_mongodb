// To update all stats, run: "mongo <dbname> map_reduce.js"

var reduceFunction = function(city_or_state_object_id, values) { 
    var results = {};

    for (var i = 0; i < values.length; i++) { 
        for (var year in values[i]) {
            // Initialize the summarized statistics for the current year, if applicable.
            results[year] = results[year] || { 
                score_counts: [
                    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],     // Nature Sciences
                    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],     // Human Sciences
                    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],     // Languages and Codes
                    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]      // Math
                ]
            };

            for (var j = 0; j < values[i][year].score_counts.length; j++) { 
                for (var k = 0; k < values[i][year].score_counts[j].length; k++) { 
                    results[year].score_counts[j][k] += values[i][year].score_counts[j][k];
                }
            }
        }
    }

    return results; 
};

// Calculate stats for cities.
var city_aggregates = db.school.mapReduce(
    function() { 
        emit(this.city, this.stats); 
    }, 
    reduceFunction, 
    { out: { inline: 1 } }
);

// Update cities in db.city collection with calculated stats.
city_aggregates.results.forEach(function(city_aggregate) { 
    db.city.update(
        { _id: city_aggregate._id }, 
        { 
            $set: { 
                stats: city_aggregate.value
            } 
        }
    ); 
});

// Calculate stats for states.
var state_aggregates = db.city.mapReduce(
    function() { 
        emit(this.state, this.stats); 
    },
    reduceFunction, 
    { out: { inline: 1 } }
);

// Update states in db.state collection with calculated stats.
state_aggregates.results.forEach(function(state_aggregate) { 
    db.state.update(
        { _id: state_aggregate._id }, 
        { 
            $set: { 
                stats: state_aggregate.value
            } 
        }
    ); 
});
