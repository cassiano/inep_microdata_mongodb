db.school.mapReduce(
    function() { 
        emit(this.city, this.stats.values); 
    }, 
    function(k, values) { 
        var results = {
            nc: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            hc: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            lc: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            mt: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        };

        for (var i = 0; i < values.length; i++) { 
            for (ka in values[i]) {
                for (j = 0; j < 10; j++) {
                    results[ka][j] += values[i][ka][j];
                }
            }
        }

        return results; 
    }, 
    { 
        out: 'city_aggregates' 
    }
)

db.city_aggregates.mapReduce(
    function() { 
        emit('SP', this.value);
    }, 
    function(k, values) { 
        var results = {
            nc: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            hc: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            lc: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            mt: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        };

        for (var i = 0; i < values.length; i++) { 
            for (ka in values[i]) {
                for (j = 0; j < 10; j++) {
                    results[ka][j] += values[i][ka][j];
                }
            }
        }

        return results; 
    }, 
    { 
        out: 'state_aggregates' 
    }
)
