// To update all stats, run: "mongo <dbname> map_reduce.js"

var reduceFunction = function(k, values) { 
    var results = {
        score_counts: [
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],     // Nature Sciences
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],     // Human Sciences
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],     // Languages and Codes
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]      // Math
        ]
    };

    for (var i = 0; i < values.length; i++) { 
        for (var j = 0; j < values[i].score_counts.length; j++) { 
            for (var k = 0; k < values[i].score_counts[j].length; k++) { 
                results.score_counts[j][k] += values[i].score_counts[j][k];
            }
        }
    }

    return results; 
};

// Calculate stats for cities.
db.school.mapReduce(
    function() { 
        emit(this.city, this.stats); 
    }, 
    reduceFunction, 
    { out: 'city_aggregates' }
);

// Update cities in db.city collection with calculated stats.
db.city.find().forEach(function(city) { 
    var city_aggregate = db.city_aggregates.find(city._id).next(); 
    
    db.city.update(
        { _id: city._id }, 
        { 
            $set: { 
                stats: city_aggregate.value 
            } 
        }
    ); 
});

// Calculate stats for states.
db.city.mapReduce(
    function() { emit(this.state, this.stats); },
    reduceFunction, 
    { out: 'state_aggregates' }
);

// Update states in db.state collection with calculated stats.
db.state.find().forEach(function(state) { 
    var state_aggregate = db.state_aggregates.find(state._id).next(); 
    
    db.state.update(
        { _id: state._id }, 
        { 
            $set: { 
                stats: state_aggregate.value 
            } 
        }
    ); 
});

// Drop the temporary collections.
db.city_aggregates.drop();
db.state_aggregates.drop();
