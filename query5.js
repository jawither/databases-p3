// Query 5
// Find the oldest friend for each user who has a friend. For simplicity,
// use only year of birth to determine age, if there is a tie, use the
// one with smallest user_id. You may find query 2 and query 3 helpful.
// You can create selections if you want. Do not modify users collection.
// Return a javascript object : key is the user_id and the value is the oldest_friend id.
// You should return something like this (order does not matter):
// {user1:userx1, user2:userx2, user3:userx3,...}

function oldest_friend(dbname) {
    db = db.getSiblingDB(dbname);

    let results = {};
    // TODO: implement oldest friends
    
    // db.users.aggregate([
    //     {$project: {user_id: 1, friends: 1, _id: 0}},
    //     {$unwind: "$friends"},
    //     {$out: "flat_users"}
    // ]);

    db.users.find().forEach(function(user) {
        var oldest_year = -1;
        var oldest_user;

        db.users.find().forEach(function(friend) {
            if (friend.friends.indexOf(user.user_id) != -1) {
                if (friend.YOB < oldest_year || oldest_year == -1) {
                    print ("foundA");
                    oldest_year = friend.YOB;
                    oldest_user = friend.user_id;
                }
            }
        });

        user.friends.forEach(function(friend_id) {
            db.users.find(
                {user_id: friend_id}
            ).forEach(function(friend) {
                if (friend.YOB < oldest_year || oldest_year == -1) {
                    print ("foundB");
                    oldest_year = friend.YOB;
                    oldest_user = friend.user_id;
                }
            });
        });

        if (oldest_year != -1) {
            results[user.user_id] = oldest_user;
            print(user.user_id + " " + oldest_user);
        }
            
    });
    //print(results);
    return results;
}
