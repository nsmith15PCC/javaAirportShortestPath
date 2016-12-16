package db;

import java.sql.*;

public class DatabaseView {
    public static void main(String[] args) throws SQLException {
        String driver = "org.apache.derby.jdbc.EmbeddedDriver";
        String URL = "jdbc:derby:db/db";
        try {
            Class.forName(driver);
        }
        catch(ClassNotFoundException e) {
        }

        Connection c = DriverManager.getConnection(URL);
        Statement s = c.createStatement();

        ResultSet rs = s.executeQuery("SELECT * FROM routes_airlines");
        for (int j = 0; j < 100; ++j) {
            rs.next();
            for (int i = 1; i <= 2; ++i) {
                System.out.print("\"" + rs.getString(i) + "\"    ");
            }
            System.out.println();
        }
        s.close();
        c.close();
    }

}
