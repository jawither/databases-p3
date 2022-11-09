// Query 8
// Find the city average friend count per user using MapReduce.

let city_average_friendcount_mapper = function () {
    emit (this.hometown.city, {users : 1, friends: this.friends.length})
};

let city_average_friendcount_reducer = function (key, values) {
    var users = 0;
    var friends = 0;
    values.forEach(function(value) {
        users += value.users;
        friends += value.friends;
    });
    return {users: users, friends: friends};
};

let city_average_friendcount_finalizer = function (key, reduceVal) {
    return reduceVal.friends / reduceVal.users;
};
