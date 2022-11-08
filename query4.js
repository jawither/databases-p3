// Query 4
// Find user pairs (A,B) that meet the following constraints:
// i) user A is male and user B is female
// ii) their Year_Of_Birth difference is less than year_diff
// iii) user A and B are not friends
// iv) user A and B are from the same hometown city
// The following is the schema for output pairs:
// [
//      [user_id1, user_id2],
//      [user_id1, user_id3],
//      [user_id4, user_id2],
//      ...
//  ]
// user_id is the field from the users collection. Do not use the _id field in users.
// Return an array of arrays.

function suggest_friends(year_diff, dbname) {
    db = db.getSiblingDB(dbname);

    let pairs = [];
    // TODO: implement suggest friends
    db.users.find(
        {gender: "male"}
    ).forEach(function(m) {
        db.users.find(
            {gender: "female"}
        ).forEach(function(f) {
            var age = false;
            var friends = false;
            var town = false;

            if (Math.abs(m.YOB - f.YOB) < year_diff) {
                age = true;
            }

            if (m.friends.indexOf(f.user_id) == -1 && f.friends.indexOf(m.user_id) == -1) {
                friends = true;
            }

            if (m.hometown.city == f.hometown.city) {
                town = true;
            }

            if (age && friends && town) {
                pairs.push([m.user_id, f.user_id]);
            }

        });
    });

    return pairs;
}
