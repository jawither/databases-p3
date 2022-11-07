import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.TreeSet;
import java.util.Vector;

import org.json.JSONObject;
import org.json.JSONArray;

public class GetData {

    static String prefix = "project3.";

    // You must use the following variable as the JDBC connection
    Connection oracleConnection = null;

    // You must refer to the following variables for the corresponding 
    // tables in your database
    String userTableName = null;
    String friendsTableName = null;
    String cityTableName = null;
    String currentCityTableName = null;
    String hometownCityTableName = null;

    // DO NOT modify this constructor
    public GetData(String u, Connection c) {
        super();
        String dataType = u;
        oracleConnection = c;
        userTableName = prefix + dataType + "_USERS";
        friendsTableName = prefix + dataType + "_FRIENDS";
        cityTableName = prefix + dataType + "_CITIES";
        currentCityTableName = prefix + dataType + "_USER_CURRENT_CITIES";
        hometownCityTableName = prefix + dataType + "_USER_HOMETOWN_CITIES";
    }

    // TODO: Implement this function
    @SuppressWarnings("unchecked")
    public JSONArray toJSON() throws SQLException {

        // This is the data structure to store all users' information
        JSONArray users_info = new JSONArray();
        
        try (Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
            // Your implementation goes here....
            
            ResultSet rst = stmt.executeQuery(
                    "SELECT * FROM " + userTableName);

            while (rst.next()) {

                // get basic user info ---------------------------------------------------------------------------------------
                JSONObject newUser = new JSONObject();
                long id = rst.getLong(1);
                newUser.put("user_id", id);
                newUser.put("first_name", rst.getString(2));
                newUser.put("last_name", rst.getString(3));
                newUser.put("YOB", rst.getLong(4));
                newUser.put("MOB", rst.getLong(5));
                newUser.put("DOB", rst.getLong(6));
                newUser.put("gender", rst.getString(7));

                // get user friends -------------------------------------------------------------------------------------------
                try (Statement friends_stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {

                    ResultSet friends_rst = friends_stmt.executeQuery(
                            "SELECT USER2_ID FROM " + friendsTableName + " WHERE USER1_ID = " + id);

                    JSONArray friendsArray = new JSONArray();
                    while (friends_rst.next()) {
                        friendsArray.put(friends_rst.getLong(1));
                    }

                    newUser.put("friends", friendsArray);

                    friends_stmt.close();
                } catch (SQLException e) {
                    System.err.println(e.getMessage());
                }

                // get user hometown -------------------------------------------------------------------------------------------
                try (Statement home_stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {

                    ResultSet home_rst = home_stmt.executeQuery(
                            "select city_name, state_name, country_name " + 
                            "from " + hometownCityTableName + " u, " + cityTableName + " c " + 
                            "where u.hometown_city_id = c.city_id " + 
                            "and u.user_id = " + id);

                    JSONObject hometown = new JSONObject();
                    while (home_rst.next()) {
                        hometown.put("city", home_rst.getString(1));
                        hometown.put("state", home_rst.getString(2));
                        hometown.put("country", home_rst.getString(3));
                    }

                    newUser.put("hometown", hometown);
                    
                    home_stmt.close();
                } catch (SQLException e) {
                    System.err.println(e.getMessage());
                }

                // get user current city -------------------------------------------------------------------------------------------
                try (Statement cur_stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {

                    ResultSet cur_rst = cur_stmt.executeQuery(
                            "select city_name, state_name, country_name " + 
                            "from " + currentCityTableName + " u, " + cityTableName + " c " + 
                            "where u.current_city_id = c.city_id " + 
                            "and u.user_id = " + id);

                    JSONObject current = new JSONObject();
                    while (cur_rst.next()) {
                        current.put("city", cur_rst.getString(1));
                        current.put("state", cur_rst.getString(2));
                        current.put("country", cur_rst.getString(3));
                    }

                    newUser.put("current", current);
                    
                    cur_stmt.close();
                } catch (SQLException e) {
                    System.err.println(e.getMessage());
                }

                users_info.put(newUser);
            }
            
            stmt.close();
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return users_info;
    }

    // This outputs to a file "output.json"
    // DO NOT MODIFY this function
    public void writeJSON(JSONArray users_info) {
        try {
            FileWriter file = new FileWriter(System.getProperty("user.dir") + "/output.json");
            file.write(users_info.toString());
            file.flush();
            file.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
