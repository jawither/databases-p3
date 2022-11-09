(function () {
    // fill in your database name
    // Your dbname is your uniqname
    let dbname = "jawither";

    load("query1.js");
    load("query2.js");
    load("query3.js");
    load("query4.js");
    load("query5.js");
    load("query6.js");
    load("query7.js");
    load("query8.js");

    print();
    print("=== Test 1 ===");
    let test1 = find_user("Bucklebury", dbname);
    // print(test1); // uncomment this line to print the query1 output
    let ans1 = test1.length;
    if (ans1 == 42) {
        print("Local test passed! Partially correct.");
    } else {
        print("Local test failed!");
        print("Expected 42 users from Bucklebury, you found", ans1, "users.");
    }

    print("=== Test 2 ===");
    unwind_friends(dbname);
    let test2 = db.flat_users.find();
    // test2.forEach(printjson); // uncomment this line to print the query2 output
    let ans2 = test2.count();
    if (ans2 == 21355) {
        print("Local test passed! Partially correct.");
    } else {
        print("Local test failed! ");
        print("Expected 21355 pairs of friends, you found", ans2, "pairs.");
    }

    print("=== Test 3 ===");
    cities_table(dbname);
    let test3 = db.cities.find({ _id: "Bucklebury" });
    let ans3 = 0;
    if (test3.hasNext()) {
        test3 = test3.next().users;
        // print(test3) // uncomment this line to print the query3 output
        ans3 = test3.length;
    }
    if (ans3 == 43) {
        print("Local test passed! Partially correct.");
    } else {
        print("Local test failed!");
        print("Expected 43 users living in Bucklebury, you found", ans3, "users.");
    }

    print("=== Test 4 ===");
    let test4 = suggest_friends(5, dbname);
    // test4.forEach(printjson); // uncomment this line to print the query4 output
    let ans4 = test4.length;
    if (ans4 == 87) {
        print("Local test passed! Partially correct.");
    } else {
        print("Local test failed!");
        print("Expected 87 pairs of suggested friends, you found", ans4, "pairs.");
    }

    print("=== Test5 === (This test is time comsuming)");
    let test5 = oldest_friend(dbname);
     printjson(test5); // uncomment this line to print the query5 output
    if (Object.keys(test5).length == 798) {
        if (test5.hasOwnProperty(799)) {
            let ans5 = test5[799];
            if (test5[799] == 51) {
                print("Local test passed! Partially correct.");
            } else {
                print("Local test failed!");
                print("Expected oldest friend for user 799 to be 51, you found", ans5, ".");
            }
        } else {
            print("Local test failed!");
            print("Expected oldest friend for user 799 to be 51. You found no oldest friend for user 799.");
        }
    } else {
        print("Local test failed!");
        print("Expected 798 users, you found", Object.keys(test5).length, "users.");
    }

    print("=== Test 6 ===");
    let ans6 = find_average_friendcount(dbname);
    // print(ans6); // uncomment this line to print the query6 output
    if ((ans6 > 26) & (ans6 < 27)) {
        print("Local test passed! Partially correct.");
    } else {
        print("Local test failed!");
        print("Expected the average number of friends to be between 26 to 27, you calculated", ans6, "friends.");
    }

    print("=== Test 7 ===");
    db.users.mapReduce(num_month_mapper, num_month_reducer, {
        out: "born_each_month",
        finalize: num_month_finalizer,
    });
    let test7 = db.born_each_month.find();
    test7.forEach(printjson); // uncomment this line to print the query7 output
    let ans7 = test7.count();
    if (ans7 == 12) {
        print("Local test passed! Partially correct.");
    } else {
        print("Local test failed!");
        print("Expected 12 months(keys), you found", ans7, "months.");
    }

    print("=== Test 8 ===");
    db.users.mapReduce(city_average_friendcount_mapper, city_average_friendcount_reducer, {
        out: "friend_city_population",
        finalize: city_average_friendcount_finalizer,
    });
    let test8 = db.friend_city_population.find();
    test8.forEach(printjson); // unconmment this line to print the query8 output
    let ans8 = test8.count();
    if (ans8 == 16) {
        print("Local test passed! Partially correct.");
    } else {
        print("Local test failed!");
        print("Expected 16 cities(keys), you found", ans8, "cities.");
    }

    print("=== Cleanup ===");
    db.getCollectionNames().forEach((col) => {
        if (col != "users") db.getCollection(col).drop();
    });
})();
