// Query 5
// Find the oldest friend for each user who has a friend. For simplicity,
// use only year of birth to determine age, if there is a tie, use the
// one with smallest user_id. You may find query 2 and query 3 helpful.
// You can create selections if you want. Do not modify users collection.
// Return a javascript object : key is the user_id and the value is the oldest_friend id.
// You should return something like this (order does not matter):
// {user1:userx1, user2:userx2, user3:userx3,...}

function get_yob(id) {
    var output;
    db.users.find(
        {user_id: id}
    ).forEach(function(user) {
        output = user.YOB;
    });
    return output;
}

function check(friend, oldest_year, has_friend, oldest_user) {
    if ((!has_friend) ||
        (get_yob(friend) < oldest_year) ||
        ((get_yob(friend) == oldest_year) && (friend < oldest_user))) {
            return true;
    }
    return false;
}

function oldest_friend(dbname) {
    db = db.getSiblingDB(dbname);

    let results = {};
    // TODO: implement oldest friends
    
    db.users.aggregate([
        {$project: {user_id: 1, friends: 1, _id: 0}},
        {$unwind: "$friends"},
        {$group : {_id: "$friends", friends: {$push: "$user_id"}}},
        {$out: "low_friends"}
    ]);

    db.users.find().forEach(function(user) {
        var has_friend = false;
        var oldest_year = -999;
        var oldest_user = -999;
        user.friends.forEach(function(friend) {
            if (check(friend, oldest_year, has_friend, oldest_user)) {
                oldest_user = friend;
                oldest_year = get_yob(friend);
            }
            has_friend = true;
        });

        db.low_friends.find(
            {_id: user.user_id}
        ).forEach(function(user) {
            user.friends.forEach(function(friend) {
                if (check(friend, oldest_year, has_friend, oldest_user)) {
                    oldest_user = friend;
                    oldest_year = get_yob(friend);
                }
                has_friend = true;
            })
        });

        if (has_friend)
            results[user.user_id] = oldest_user;
            
    });
    //print(results);
    return results;
}
