// Query 6
// Find the average friend count per user.
// Return a decimal value as the average user friend count of all users in the users collection.

function find_average_friendcount(dbname) {
    db = db.getSiblingDB(dbname);

    var numerator = 0;
    var denomenator = 0;

    // TODO: calculate the average friend count
    db.users.find().forEach(function(user) {
        denomenator++;
        numerator += user.friends.length;
    });

    return numerator/denomenator;
}
