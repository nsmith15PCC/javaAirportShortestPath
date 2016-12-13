package db;

import db.CSVParser.CSVParser;
import java.util.List;
import java.sql.PreparedStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.SQLException;
import java.io.File;
import java.io.IOException;

class BuildDatabaseUtility {
    private Connection c;

    BuildDatabaseUtility() throws SQLException {
        String driver = "org.apache.derby.jdbc.EmbeddedDriver";
        String URL = "jdbc:derby:db/db;create=true";

        try {
            Class.forName(driver);
        }
        catch(ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        c = DriverManager.getConnection(URL);
    }


    void close() throws SQLException {
        c.close();
    }

    void execute() throws SQLException, IOException {
        makeAirports();
        makeAirlines();
    }

    private void makeAirports() throws SQLException, IOException {
        Statement s = c.createStatement();
        PreparedStatement ps;
        List<String> l;
        CSVParser par = new CSVParser("db"+File.separator+"airports.dat");
        
        try {
            s.execute("DROP TABLE airports");
        }
        catch (SQLException e) {
            // e.printStackTrace();
        }
        s.execute("CREATE TABLE airports(" +
                  "Airport_ID INT PRIMARY KEY," +
                  "Name VARCHAR(128)," +
                  "City VARCHAR(128)," +
                  "Country VARCHAR(32)," +
                  "IATA CHAR(3)," +
                  "Latitude DOUBLE," +
                  "Longitude DOUBLE)");
        s.close();

        ps = c.prepareStatement("INSERT INTO airports VALUES (?, ?, ?, ?, ?, ?, ?)");
        while((l = par.getNext()) != null) {
            ps.setInt(1, Integer.parseInt(l.get(0)));
            ps.setString(2, l.get(1));
            ps.setString(3, l.get(2));
            ps.setString(4, l.get(3));
            ps.setString(5, l.get(4));
            ps.setDouble(6, Double.parseDouble(l.get(6)));
            ps.setDouble(7, Double.parseDouble(l.get(7)));
            try {
                ps.executeUpdate();
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
            ps.clearParameters();
        }
        ps.close();
        System.out.println("Airports Successful!");
    }

    private void makeAirlines() throws SQLException, IOException {
        Statement s = c.createStatement();
        PreparedStatement ps;
        CSVParser par = new CSVParser("db"+File.separator+"airlines.dat");
        List<String> l;

        try {
            s.execute("DROP TABLE airlines");
        }
        catch (SQLException e) {
            // e.printStackTrace();
        }
        s.execute("CREATE TABLE airlines(" +
                  "Airline_ID INT PRIMARY KEY," +
                  "Name VARCHAR(128)," +
                  "IATA CHAR(2))");
        s.close();

        ps = c.prepareStatement("INSERT INTO airlines VALUES (?, ?, ?)");
        while((l = par.getNext()) != null) {
            ps.setInt(1, Integer.parseInt(l.get(0)));
            ps.setString(2, l.get(1));
            ps.setString(3, l.get(3));
            try {
                ps.executeUpdate();
            }
            catch(SQLException e) {
                e.printStackTrace();
            }
            ps.clearParameters();
        }
        ps.close();
        System.out.println("Airlines Successful!");
    }


}

