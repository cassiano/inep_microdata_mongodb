// PS: locality can be either a school, city, state or even the whole country!
var reduceFunction = function(locality, values) {
    var counts = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

    for (var i = 0; i < values.length; i++) { 
        for (var j = 0; j < values[i].counts.length; j++) {
            counts[j] += values[i].counts[j];
        }
    }

    return { counts: counts }; 
};

var mapFunction = function() {
    var counts = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    
    if (this.ranges['NAT'] != null) {
        counts[this.ranges['NAT'] - 1] = 1;

        emit('locality', { counts: counts }); 
    }
};

var sp_city_aggregates = db.subscriptions.mapReduce(
    mapFunction,
    reduceFunction, 
    { 
         query: { 'city.code': '3550308', year: 2011 },
         out: { inline: 1 } 
    }
);

var sp_state_aggregates = db.subscriptions.mapReduce(
    mapFunction,
    reduceFunction, 
    { 
         query: { state: 'SP', year: 2011 },
         out: { inline: 1 } 
    }
);

var br_aggregates = db.subscriptions.mapReduce(
    mapFunction,
    reduceFunction, 
    { 
         query: { year: 2011 },
         out: { inline: 1 } 
    }
);
