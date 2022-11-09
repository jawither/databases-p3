// Query 7
// Find the number of users born in each month using MapReduce

let num_month_mapper = function () {
    emit(this.MOB, {count: 1, ids: [this.user_id]});
};

let num_month_reducer = function (key, values) {
    var users = [];
    var count = 0;
    values.forEach(function(value) {
        count += value.count;
        value.ids.forEach(function(user) {
            users.push(user);
        });
    });
    return {count: count, ids: users};
};

let num_month_finalizer = function (key, reduceVal) {
    // We've implemented a simple forwarding finalize function. This implementation
    // is naive: it just forwards the reduceVal to the output collection.
    // TODO: Feel free to change it if needed.
    return reduceVal;
};
