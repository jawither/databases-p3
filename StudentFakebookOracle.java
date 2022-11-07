package project2;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.ArrayList;

/*
    The StudentFakebookOracle class is derived from the FakebookOracle class and implements
    the abstract query functions that investigate the database provided via the <connection>
    parameter of the constructor to discover specific information.
*/
public final class StudentFakebookOracle extends FakebookOracle {
    // [Constructor]
    // REQUIRES: <connection> is a valid JDBC connection
    public StudentFakebookOracle(Connection connection) {
        oracle = connection;
    }

    @Override
    // Query 0
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the total number of users for which a birth month is listed
    //        (B) Find the birth month in which the most users were born
    //        (C) Find the birth month in which the fewest users (at least one) were born
    //        (D) Find the IDs, first names, and last names of users born in the month
    //            identified in (B)
    //        (E) Find the IDs, first names, and last name of users born in the month
    //            identified in (C)
    //
    // This query is provided to you completed for reference. Below you will find the appropriate
    // mechanisms for opening up a statement, executing a query, walking through results, extracting
    // data, and more things that you will need to do for the remaining nine queries
    public BirthMonthInfo findMonthOfBirthInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            // Step 1
            // ------------
            // * Find the total number of users with birth month info
            // * Find the month in which the most users were born
            // * Find the month in which the fewest (but at least 1) users were born
            ResultSet rst = stmt.executeQuery(
                    "SELECT COUNT(*) AS Birthed, Month_of_Birth " + // select birth months and number of uses with that birth month
                            "FROM " + UsersTable + " " + // from all users
                            "WHERE Month_of_Birth IS NOT NULL " + // for which a birth month is available
                            "GROUP BY Month_of_Birth " + // group into buckets by birth month
                            "ORDER BY Birthed DESC, Month_of_Birth ASC"); // sort by users born in that month, descending; break ties by birth month

            int mostMonth = 0;
            int leastMonth = 0;
            int total = 0;
            while (rst.next()) { // step through result rows/records one by one
                if (rst.isFirst()) { // if first record
                    mostMonth = rst.getInt(2); //   it is the month with the most
                }
                if (rst.isLast()) { // if last record
                    leastMonth = rst.getInt(2); //   it is the month with the least
                }
                total += rst.getInt(1); // get the first field's value as an integer
            }
            BirthMonthInfo info = new BirthMonthInfo(total, mostMonth, leastMonth);

            // Step 2
            // ------------
            // * Get the names of users born in the most popular birth month
            rst = stmt.executeQuery(
                    "SELECT User_ID, First_Name, Last_Name " + // select ID, first name, and last name
                            "FROM " + UsersTable + " " + // from all users
                            "WHERE Month_of_Birth = " + mostMonth + " " + // born in the most popular birth month
                            "ORDER BY User_ID"); // sort smaller IDs first

            while (rst.next()) {
                info.addMostPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 3
            // ------------
            // * Get the names of users born in the least popular birth month
            rst = stmt.executeQuery(
                    "SELECT User_ID, First_Name, Last_Name " + // select ID, first name, and last name
                            "FROM " + UsersTable + " " + // from all users
                            "WHERE Month_of_Birth = " + leastMonth + " " + // born in the least popular birth month
                            "ORDER BY User_ID"); // sort smaller IDs first

            while (rst.next()) {
                info.addLeastPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 4
            // ------------
            // * Close resources being used
            rst.close();
            stmt.close(); // if you close the statement first, the result set gets closed automatically

            return info;

        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return new BirthMonthInfo(-1, -1, -1);
        }
    }

    @Override
    // Query 1
    // -----------------------------------------------------------------------------------
    // GOALS: (A) The first name(s) with the most letters
    //        (B) The first name(s) with the fewest letters
    //        (C) The first name held by the most users
    //        (D) The number of users whose first name is that identified in (C)
    public FirstNameInfo findNameInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                FirstNameInfo info = new FirstNameInfo();
                info.addLongName("Aristophanes");
                info.addLongName("Michelangelo");
                info.addLongName("Peisistratos");
                info.addShortName("Bob");
                info.addShortName("Sue");
                info.addCommonName("Harold");
                info.addCommonName("Jessica");
                info.setCommonNameCount(42);
                return info;
            */

            FirstNameInfo info = new FirstNameInfo();

            // step 1: find shortest and longest name lengths
            ResultSet rst = stmt.executeQuery(
                "SELECT MAX(LENGTH(u2.first_name)) AS len " + // max name length
                "FROM " + UsersTable + " u2 " +
                "UNION " +
                "SELECT MIN(LENGTH(u2.first_name)) AS len " + // min name length
                "FROM " + UsersTable + " u2 " +
                "ORDER BY len"
            );

            long shortestNameLength = -1;
            long longestNameLength = -1;
            
            while(rst.next()) {
                if (rst.isFirst()) {
                    shortestNameLength = rst.getLong(1);
                }
                if (rst.isLast()) {
                    longestNameLength = rst.getLong(1);
                }
            }
            
            // step 2: find names of these lengths
            rst = stmt.executeQuery(
                "SELECT u1.first_name " + // select first names
                "FROM " + UsersTable + " u1 " + // from all users
                "WHERE (LENGTH(u1.first_name) = " + shortestNameLength + " " + // where name is of shortest length
                "OR LENGTH(u1.first_name) = " + longestNameLength + ") " + // or name is of longest length
                "GROUP BY (u1.first_name) " + // group into buckets by first name
                "ORDER BY LENGTH(u1.first_name), u1.first_name" // and sort by length
            );

            while(rst.next()) {
                if (rst.getString(1).length() == shortestNameLength) {
                    info.addShortName(rst.getString(1));
                }
                else {
                    info.addLongName(rst.getString(1));
                }
            }

            // step 3: find most common name(s)
            rst = stmt.executeQuery(
                "SELECT u1.first_name, COUNT(u1.first_name) " + // select name and count
                "FROM " + UsersTable + " u1 " + // from users' names
                "GROUP BY (u1.first_name) " + // grouped by name
                "HAVING COUNT(u1.first_name) = (" + // where group count is equal to
                    "SELECT MAX(counts) " + // the maximum of all counts
                    "FROM (SELECT COUNT(u3.first_name) AS counts " + // from counts
                        "FROM " + UsersTable + " u3 " + 
                        "GROUP BY (u3.first_name) " +
                        "ORDER BY COUNT(u3.first_name)" + // of groups grouped by name
                    ")" +
                ") " +
                "ORDER BY u1.first_name" // ordered by name
            );

            while (rst.next()) {
                info.setCommonNameCount(rst.getLong(2));
                info.addCommonName(rst.getString(1));
            }
            
            stmt.close();
            return info;
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return new FirstNameInfo();
        }
    }

    @Override
    // Query 2
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users without any friends
    //
    // Be careful! Remember that if two users are friends, the Friends table only contains
    // the one entry (U1, U2) where U1 < U2.
    public FakebookArrayList<UserInfo> lonelyUsers() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(15, "Abraham", "Lincoln");
                UserInfo u2 = new UserInfo(39, "Margaret", "Thatcher");
                results.add(u1);
                results.add(u2);
            */

            ResultSet rst = stmt.executeQuery(
                "SELECT u1.user_id, u1.first_name, u1.last_name " +
                "FROM " + UsersTable + " u1 " +
                "WHERE u1.user_id NOT IN (" +
                    "SELECT f1.user1_id " +
                    "FROM " + FriendsTable + " f1 " +
                    "UNION " +
                    "SELECT f1.user2_id " +
                    "FROM " + FriendsTable + " f1" +
                ") ORDER BY u1.user_id ASC"
            );

            while (rst.next()) {
                UserInfo userToAdd = new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3));
                results.add(userToAdd);
            }
            stmt.close();
            
            
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        return results;
        
        
    }

    @Override
    // Query 3
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users who no longer live
    //            in their hometown (i.e. their current city and their hometown are different)
    public FakebookArrayList<UserInfo> liveAwayFromHome() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(9, "Meryl", "Streep");
                UserInfo u2 = new UserInfo(104, "Tom", "Hanks");
                results.add(u1);
                results.add(u2);
            */

            ResultSet rst = stmt.executeQuery(
                "SELECT u1.user_id, u1.first_name, u1.last_name " +
                "FROM " + UsersTable + " u1, " + CurrentCitiesTable + " cur, " + HometownCitiesTable + " home " +
                "WHERE u1.user_id = cur.user_id " +
                "AND u1.user_id = home.user_id " +
                "AND home.hometown_city_id != cur.current_city_id " +
                "ORDER BY u1.user_id ASC"
            );

            while (rst.next()) {
                UserInfo userToAdd = new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3));
                results.add(userToAdd);
            }

            stmt.close();
            

        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    @Override
    // Query 4
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, links, and IDs and names of the containing album of the top
    //            <num> photos with the most tagged users
    //        (B) For each photo identified in (A), find the IDs, first names, and last names
    //            of the users therein tagged
    public FakebookArrayList<TaggedPhotoInfo> findPhotosWithMostTags(int num) throws SQLException {
        FakebookArrayList<TaggedPhotoInfo> results = new FakebookArrayList<TaggedPhotoInfo>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                PhotoInfo p = new PhotoInfo(80, 5, "www.photolink.net", "Winterfell S1");
                UserInfo u1 = new UserInfo(3901, "Jon", "Snow");
                UserInfo u2 = new UserInfo(3902, "Arya", "Stark");
                UserInfo u3 = new UserInfo(3903, "Sansa", "Stark");
                TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
                tp.addTaggedUser(u1);
                tp.addTaggedUser(u2);
                tp.addTaggedUser(u3);
                results.add(tp);
            */

                ResultSet rst = stmt.executeQuery(
                    "SELECT p1.photo_id AS PID, p1.album_id AS AID, p1.photo_link AS Link, a1.album_name AS AName " +
                    "FROM " + PhotosTable + " p1, " + AlbumsTable + " a1, " +
                    "(SELECT t1.tag_photo_id AS id, COUNT(t1.tag_photo_id) AS ct " +
                    "FROM " + TagsTable + " t1 " +
                    "GROUP BY (t1.tag_photo_id) " +
                    "ORDER BY ct DESC, id) " +
                    "tags " +
                    "WHERE tags.id = p1.photo_id " +
                    "AND p1.album_id = a1.album_id " +
                    "AND ROWNUM <= " + num
                );

                while (rst.next()) {
                    long newPhotoId = rst.getLong(1);
                    PhotoInfo p = new PhotoInfo(newPhotoId, rst.getLong(2), rst.getString(3), rst.getString(4));
                    TaggedPhotoInfo taggedPhotoToAdd = new TaggedPhotoInfo(p);

                    try (Statement subjects_stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                    FakebookOracleConstants.ReadOnly)) {

                        ResultSet subjects_rst = subjects_stmt.executeQuery(
                            "SELECT u1.user_id, u1.first_name, u1.last_name " +
                            "FROM " + UsersTable + " u1, " + TagsTable + " t1 " +
                            "WHERE u1.user_id = t1.tag_subject_id " +
                            "AND t1.tag_photo_id = " + newPhotoId + " " +
                            "ORDER BY u1.user_id ASC"
                        );

                        while(subjects_rst.next()) {
                            taggedPhotoToAdd.addTaggedUser(new UserInfo(subjects_rst.getLong(1), subjects_rst.getString(2), subjects_rst.getString(3)));
                        }

                        subjects_stmt.close();
                    }
                    catch (SQLException e) {
                        System.err.println(e.getMessage());
                        break;
                    }

                    results.add(taggedPhotoToAdd);
                }

        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    @Override
    // Query 5
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, last names, and birth years of each of the two
    //            users in the top <num> pairs of users that meet each of the following
    //            criteria:
    //              (i) same gender
    //              (ii) tagged in at least one common photo
    //              (iii) difference in birth years is no more than <yearDiff>
    //              (iv) not friends
    //        (B) For each pair identified in (A), find the IDs, links, and IDs and names of
    //            the containing album of each photo in which they are tagged together
    public FakebookArrayList<MatchPair> matchMaker(int num, int yearDiff) throws SQLException {
        FakebookArrayList<MatchPair> results = new FakebookArrayList<MatchPair>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(93103, "Romeo", "Montague");
                UserInfo u2 = new UserInfo(93113, "Juliet", "Capulet");
                MatchPair mp = new MatchPair(u1, 1597, u2, 1597);
                PhotoInfo p = new PhotoInfo(167, 309, "www.photolink.net", "Tragedy");
                mp.addSharedPhoto(p);
                results.add(mp);
            */

            ResultSet rst = stmt.executeQuery(
                "SELECT common_tags.id1, common_tags.fn1, common_tags.ln1, common_tags.y1, common_tags.id2, common_tags.fn2, common_tags.ln2, common_tags.y2, COUNT(common_tags.pid) AS ct FROM  " +
                "(SELECT u1.user_id AS id1, u1.first_name AS fn1, u1.last_name AS ln1, u1.year_of_birth AS y1, u2.user_id AS id2, u2.first_name AS fn2, u2.last_name AS ln2, u2.year_of_birth AS y2, t1.tag_photo_id AS pid  " +
                "FROM  " +
                ""+UsersTable+" u1, "+UsersTable+" u2, "+TagsTable+" t1, "+TagsTable+" t2  " +
                "WHERE u1.user_id = t1.tag_subject_id  " +
                "AND t1.tag_photo_id = t2.tag_photo_id  " +
                "AND u2.user_id = t2.tag_subject_id  " +
                "AND u1.user_id < u2.user_id  " +
                "AND u1.gender = u2.gender  " +
                "AND (ABS(u1.year_of_birth - u2.year_of_birth) <= 2))  " +
                "common_tags  " +
                "LEFT JOIN "+FriendsTable+" f1  " +
                "ON common_tags.id1 = f1.user1_id AND common_tags.id2 = f1.user2_id  " +
                "WHERE f1.user1_id IS NULL  " +
                "GROUP BY (common_tags.id1, common_tags.fn1, common_tags.ln1, common_tags.y1, common_tags.id2, common_tags.fn2, common_tags.ln2, common_tags.y2)  " +
                "ORDER BY ct, common_tags.id1, common_tags.id2"
            );

            while(rst.next()) {
                long id1 = rst.getLong(1);
                long id2 = rst.getLong(5);
                UserInfo u1 = new UserInfo(id1, rst.getString(2), rst.getString(3));
                UserInfo u2 = new UserInfo(id2, rst.getString(6), rst.getString(7));
                MatchPair mp = new MatchPair(u1, rst.getLong(4), u2, rst.getLong(8));

                try (Statement photos_stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                    FakebookOracleConstants.ReadOnly)) {

                        ResultSet photos_rst = photos_stmt.executeQuery(
                            "SELECT p1.photo_id, p1.photo_link, p1.album_id, a1.album_name " + 
                            "FROM "+PhotosTable+" p1, "+AlbumsTable+" a1, "+TagsTable+" t1, "+TagsTable+" t2 " + 
                            "WHERE t1.tag_subject_id = "+id1+" " + 
                            "AND t2.tag_subject_id = "+id2+" " + 
                            "AND t1.tag_photo_id = t2.tag_photo_id " + 
                            "AND p1.photo_id = t1.tag_photo_id " + 
                            "AND p1.album_id = a1.album_id " + 
                            "ORDER BY p1.photo_id ASC"
                        );

                        while(photos_rst.next()) {
                            PhotoInfo p = new PhotoInfo(photos_rst.getLong(1), photos_rst.getLong(3), photos_rst.getString(2), photos_rst.getString(4));
                            mp.addSharedPhoto(p);
                        }

                        results.add(mp);

                        photos_stmt.close();
                    }
                    catch (SQLException e) {
                        System.err.println(e.getMessage());
                        break;
                    }
            }

        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    @Override
    // Query 6
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of each of the two users in
    //            the top <num> pairs of users who are not friends but have a lot of
    //            common friends
    //        (B) For each pair identified in (A), find the IDs, first names, and last names
    //            of all the two users' common friends
    public FakebookArrayList<UsersPair> suggestFriends(int num) throws SQLException {
        FakebookArrayList<UsersPair> results = new FakebookArrayList<UsersPair>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(16, "The", "Hacker");
                UserInfo u2 = new UserInfo(80, "Dr.", "Marbles");
                UserInfo u3 = new UserInfo(192, "Digit", "Le Boid");
                UsersPair up = new UsersPair(u1, u2);
                up.addSharedFriend(u3);
                results.add(up);
            */

            ResultSet rst = stmt.executeQuery(
                "SELECT ordered.id1, u4.first_name, u4.last_name, ordered.id2, u5.first_name, u5.last_name, ordered.ct FROM " +
                "(SELECT * FROM "+
                "(SELECT * FROM " +
                "(SELECT u1.user_id AS id1, u3.user_id AS id2, COUNT(*) AS ct " +
                "FROM "+UsersTable+" u1, "+UsersTable+" u2, "+UsersTable+" u3, "+FriendsTable+" f1, "+FriendsTable+" f2 " +
                "WHERE ((u1.user_id = f1.user1_id AND u2.user_id = f1.user2_id) OR (u1.user_id = f1.user2_id AND u2.user_id = f1.user1_id)) " +
                "AND ((u2.user_id = f2.user1_id AND u3.user_id = f2.user2_id) OR (u2.user_id = f2.user2_id AND u3.user_id = f2.user1_id)) " +
                "AND (u1.user_id < u3.user_id) " +
                "GROUP BY (u1.user_id, u3.user_id)) " +
                "mutuals " +
                "LEFT JOIN "+FriendsTable+" f3 " +
                "ON mutuals.id1 = f3.user1_id AND mutuals.id2 = f3.user2_id " +
                "WHERE f3.user1_id IS NULL " +
                "ORDER BY mutuals.ct DESC, mutuals.id1, mutuals.id2) " +
                "WHERE ROWNUM <= 5) " +
                "ordered, " +
                ""+UsersTable+" u4, "+UsersTable+" u5 " +
                "WHERE u4.user_id = ordered.id1 " +
                "AND u5.user_id = ordered.id2 " +
                "ORDER BY ordered.ct DESC, ordered.id1, ordered.id2"
            );

            while(rst.next()) {
                long id1 = rst.getLong(1);
                long id2 = rst.getLong(4);
                UserInfo u1 = new UserInfo(id1, rst.getString(2), rst.getString(3));
                UserInfo u2 = new UserInfo(id2, rst.getString(5), rst.getString(6));
                UsersPair pair = new UsersPair(u1, u2);

                try (Statement mutuals_stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                    FakebookOracleConstants.ReadOnly)) {
                    ResultSet mutuals_rst = mutuals_stmt.executeQuery(
                        "SELECT u2.user_id, u2.first_name, u2.last_name " +
                        "FROM "+UsersTable+" u1, "+UsersTable+" u2, "+UsersTable+" u3, "+FriendsTable+" f1, "+FriendsTable+" f2 " +
                        "WHERE ((u1.user_id = f1.user1_id AND u2.user_id = f1.user2_id) OR (u1.user_id = f1.user2_id AND u2.user_id = f1.user1_id)) " +
                        "AND ((u2.user_id = f2.user1_id AND u3.user_id = f2.user2_id) OR (u2.user_id = f2.user2_id AND u3.user_id = f2.user1_id)) " +
                        "AND u1.user_id = " + id1 + " " +
                        "AND u3.user_id = " + id2 + " " +
                        "ORDER BY u2.user_id ASC"
                    );
                    while(mutuals_rst.next()) {
                        UserInfo mutual = new UserInfo(mutuals_rst.getLong(1), mutuals_rst.getString(2), mutuals_rst.getString(3));
                        pair.addSharedFriend(mutual);
                    }
                    mutuals_stmt.close();
                }
                catch (SQLException e) {
                    System.err.println(e.getMessage());
                    break;
                }
                results.add(pair);
            }

        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    @Override
    // Query 7
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the name of the state or states in which the most events are held
    //        (B) Find the number of events held in the states identified in (A)
    public EventStateInfo findEventStates() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                EventStateInfo info = new EventStateInfo(50);
                info.addState("Kentucky");
                info.addState("Hawaii");
                info.addState("New Hampshire");
                return info;
            */
            
            ResultSet rst = stmt.executeQuery(
                "(SELECT MAX(counts) FROM " +
                "(SELECT c1.state_name, COUNT(e1.event_id) AS counts " +
                "FROM "+EventsTable+" e1, "+CitiesTable+" c1 " +
                "WHERE e1.event_city_id = c1.city_id " +
                "GROUP BY (c1.state_name)))"
            );
            
            long max_events = -1;
            while (rst.next()) {
                max_events = rst.getLong(1);
                break;
            }

            rst = stmt.executeQuery(
                "SELECT c2.state_name, COUNT(e2.event_id) " +
                "FROM "+EventsTable+" e2, "+CitiesTable+" c2 " +
                "WHERE e2.event_city_id = c2.city_id " +
                "GROUP BY (c2.state_name) " +
                "HAVING COUNT (e2.event_id) = " +
                max_events +
                "ORDER BY c2.state_name"
            );

            EventStateInfo info = new EventStateInfo(max_events);

            while (rst.next()) {
                info.addState(rst.getString(1));
            }
            stmt.close();
            return info;
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return new EventStateInfo(-1);
        }
    }

    @Override
    // Query 8
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the ID, first name, and last name of the oldest friend of the user
    //            with User ID <userID>
    //        (B) Find the ID, first name, and last name of the youngest friend of the user
    //            with User ID <userID>
    public AgeInfo findAgeInfo(long userID) throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo old = new UserInfo(12000000, "Galileo", "Galilei");
                UserInfo young = new UserInfo(80000000, "Neil", "deGrasse Tyson");
                return new AgeInfo(old, young);
            */

            ResultSet rst = stmt.executeQuery(
                "SELECT * FROM " +
                "(SELECT * FROM " + 
                "(SELECT u2.user_id, u2.first_name, u2.last_name, oldest_day.yob as y, oldest_day.mob as m, oldest_day.dob as d " + 
                "FROM "+UsersTable+" u2, " + 
                "(SELECT * FROM (SELECT u1.year_of_birth as yob, u1.month_of_birth as mob, u1.day_of_birth as dob " + 
                "FROM "+UsersTable+" u1, "+FriendsTable+" f1 " + 
                "WHERE (("+userID+" = f1.user1_id AND u1.user_id = f1.user2_id) OR (215 = f1.user2_id AND u1.user_id = f1.user1_id)) " + 
                "ORDER BY u1.year_of_birth, u1.month_of_birth, u1.day_of_birth) " + 
                "WHERE ROWNUM = 1) " + 
                "oldest_day " + 
                "WHERE u2.year_of_birth = oldest_day.yob " + 
                "AND u2.month_of_birth = oldest_day.mob " + 
                "AND u2.day_of_birth = oldest_day.dob " + 
                "ORDER BY u2.user_id DESC) " + 
                "WHERE ROWNUM = 1 " + 
                "UNION " + 
                "SELECT * FROM " + 
                "(SELECT u2.user_id, u2.first_name, u2.last_name, youngest_day.yob as y, youngest_day.mob as m, youngest_day.dob as d " + 
                "FROM "+UsersTable+" u2, " + 
                "(SELECT * FROM (SELECT u1.year_of_birth as yob, u1.month_of_birth as mob, u1.day_of_birth as dob " + 
                "FROM "+UsersTable+" u1, "+FriendsTable+" f1 " + 
                "WHERE (("+userID+" = f1.user1_id AND u1.user_id = f1.user2_id) OR (215 = f1.user2_id AND u1.user_id = f1.user1_id)) " + 
                "ORDER BY u1.year_of_birth DESC, u1.month_of_birth DESC, u1.day_of_birth DESC) " + 
                "WHERE ROWNUM = 1) " + 
                "youngest_day " + 
                "WHERE u2.year_of_birth = youngest_day.yob " + 
                "AND u2.month_of_birth = youngest_day.mob " + 
                "AND u2.day_of_birth = youngest_day.dob " + 
                "ORDER BY u2.user_id DESC) " + 
                "WHERE ROWNUM = 1) " + 
                "ORDER BY y, m, d"
            );

            rst.next();
            UserInfo old = new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3));
            rst.next();
            UserInfo young = new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3));
            return new AgeInfo(old, young);
            
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return new AgeInfo(new UserInfo(-1, "ERROR", "ERROR"), new UserInfo(-1, "ERROR", "ERROR"));
        }
    }

    @Override
    // Query 9
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find all pairs of users that meet each of the following criteria
    //              (i) same last name
    //              (ii) same hometown
    //              (iii) are friends
    //              (iv) less than 10 birth years apart
    public FakebookArrayList<SiblingInfo> findPotentialSiblings() throws SQLException {
        FakebookArrayList<SiblingInfo> results = new FakebookArrayList<SiblingInfo>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(81023, "Kim", "Kardashian");
                UserInfo u2 = new UserInfo(17231, "Kourtney", "Kardashian");
                SiblingInfo si = new SiblingInfo(u1, u2);
                results.add(si);
            */

            ResultSet rst = stmt.executeQuery(
                "SELECT u1.user_id, u1.first_name, u1.last_name, u2.user_id, u2.first_name, u2.last_name FROM " +
                ""+UsersTable+" u1, "+UsersTable+" u2, "+FriendsTable+" f1, " +
                ""+HometownCitiesTable+" c1, "+HometownCitiesTable+" c2 " +
                "WHERE u1.user_id = f1.user1_id " +
                "AND u2.user_id = f1.user2_id " +
                "AND u1.last_name = u2.last_name " +
                "AND c1.user_id = u1.user_id " +
                "AND c2.user_id = u2.user_id " +
                "AND c1.hometown_city_id = c2.hometown_city_id " +
                "AND (ABS(u1.year_of_birth - u2.year_of_birth) < 10) " +
                "ORDER BY u1.user_id, u2.user_id"
            );

            while (rst.next()) {
                UserInfo u1 = new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3));
                UserInfo u2 = new UserInfo(rst.getLong(4), rst.getString(5), rst.getString(6));
                SiblingInfo si = new SiblingInfo(u1, u2);
                results.add(si);
            }

        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    // Member Variables
    private Connection oracle;
    private final String UsersTable = FakebookOracleConstants.UsersTable;
    private final String CitiesTable = FakebookOracleConstants.CitiesTable;
    private final String FriendsTable = FakebookOracleConstants.FriendsTable;
    private final String CurrentCitiesTable = FakebookOracleConstants.CurrentCitiesTable;
    private final String HometownCitiesTable = FakebookOracleConstants.HometownCitiesTable;
    private final String ProgramsTable = FakebookOracleConstants.ProgramsTable;
    private final String EducationTable = FakebookOracleConstants.EducationTable;
    private final String EventsTable = FakebookOracleConstants.EventsTable;
    private final String AlbumsTable = FakebookOracleConstants.AlbumsTable;
    private final String PhotosTable = FakebookOracleConstants.PhotosTable;
    private final String TagsTable = FakebookOracleConstants.TagsTable;
}
