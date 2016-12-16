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
    private static final double radius = 3958.8;

    BuildDatabaseUtility() throws SQLException {
        c = null;
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
        try {
            if (c != null) c.close();
        }
        catch(SQLException e) {
            e.printStackTrace();
        }
    }

    void execute() throws SQLException, IOException {
        makeAirports();
        makeAirlines();
        makeRoutes();
    }

    private void makeAirports() throws SQLException, IOException {
        Statement s = null;
        PreparedStatement ps = null;

        try {
            s = c.createStatement();
            List<String> l;
            CSVParser par = new CSVParser("db"+File.separator+"airports.dat");
        
            try {
                s.execute("DROP TABLE airports");
            }
            catch (SQLException e) {
                e.printStackTrace();
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
        }
        finally {
            try {
                if (s != null)  s.close();
            }
            catch(SQLException e) {
                e.printStackTrace();
            }
            try {
                if (ps != null)  ps.close();
            }
            catch(SQLException e) {
                e.printStackTrace();
            }
        }

        System.out.println("Airports Successful!");
    }

    private void makeAirlines() throws SQLException, IOException {
        Statement s = null;
        PreparedStatement ps = null;
        try {
            s = c.createStatement();
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
        }
        finally {
            try {
                if (s != null)  s.close();
            }
            catch(SQLException e) {
                e.printStackTrace();
            }
            try {
                if (ps != null)  ps.close();
            }
            catch(SQLException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Airlines Successful!");
    }

    private void makeRoutes() throws SQLException, IOException {
        Statement s = null;
        PreparedStatement routeInsert = null, getRouteID = null, routeAirlineInsert = null;

        try {
            s = c.createStatement();
            CSVParser par = new CSVParser("db"+File.separator+"routes.dat");
            List<String> l;

            try {
                s.execute("DROP TABLE routes");
                s.execute("DROP TABLE routes_airlines");
            }
            catch (SQLException e) {
                // e.printStackTrace();
            }
            s.execute("CREATE TABLE routes(" +
                "Route_ID INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY," +
                "Source_ID INT REFERENCES airports (Airport_ID)," +
                "Dest_ID INT REFERENCES airports (Airport_ID)," +
                "Distance DOUBLE," +
                "UNIQUE (Source_ID, Dest_ID))"
            );
            s.execute("CREATE TABlE routes_airlines(" +
                "Route_ID INT REFERENCES routes(Route_ID)," +
                "Airline_ID INT REFERENCES airlines(Airline_ID)," +
                "PRIMARY KEY (Route_ID, Airline_ID))"
            );

            routeInsert = c.prepareStatement("INSERT INTO routes " +
                "(Source_ID, Dest_ID, Distance) " +
                "VALUES (?, ?, ?)"
            );
            routeAirlineInsert = c.prepareStatement("INSERT INTO routes_airlines " +
                "(Route_ID, Airline_ID) " +
                "VALUES (?, ?)"
            );
            getRouteID = c.prepareStatement("SELECT Route_ID " +
                "FROM routes " +
                "WHERE Source_ID = ? AND Dest_ID = ?"
            );

            while((l = par.getNext()) != null) {
                int Airline_ID, Source_ID, Dest_ID;
                try {
                    Airline_ID = Integer.parseInt(l.get(1));
                    Source_ID = Integer.parseInt(l.get(3));
                    Dest_ID = Integer.parseInt(l.get(5));
                }
                catch(NumberFormatException e) {
                    continue;
                }
                double distance = greatCircle(Source_ID, Dest_ID);
                routeInsert.setInt(1, Source_ID);
                routeInsert.setInt(2, Dest_ID);
                routeInsert.setDouble(3, distance);
                try {
                    routeInsert.executeUpdate();
                }
                catch(SQLException e) {
                }
                getRouteID.setInt(1, Source_ID);
                getRouteID.setInt(2, Dest_ID);
                ResultSet rs = getRouteID.executeQuery();
                rs.next();
                int Route_ID = rs.getInt(1);
                routeAirlineInsert.setInt(1, Route_ID);
                routeAirlineInsert.setInt(2, Airline_ID);
                routeAirlineInsert.executeUpdate();
            }
        }
        finally {
            try {
                if (s != null)  s.close();
            }
            catch(SQLException e) {
                e.printStackTrace();
            }
            try {
                if (routeInsert != null)  routeInsert.close();
            }
            catch(SQLException e) {
                e.printStackTrace();
            }
            try {
                if (routeAirlineInsert != null)  routeAirlineInsert.close();
            }
            catch(SQLException e) {
                e.printStackTrace();
            }
            try {
                if (getRouteID != null)  getRouteID.close();
            }
            catch(SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private double greatCircle(int sourceID, int destID) throws SQLException{
        double sourceLat, sourceLong, destLat, destLong;
        Statement s = c.createStatement();
        ResultSet rs;

        rs = s.executeQuery("SELECT Latitude, Longitude FROM airports WHERE Airport_ID = " + Integer.toString(sourceID));
        rs.next();
        sourceLat = Math.toRadians(rs.getDouble(1));
        sourceLong = Math.toRadians(rs.getDouble(2));

        rs = s.executeQuery("SELECT Latitude, Longitude FROM airports WHERE Airport_ID = " + Integer.toString(destID));
        rs.next();
        destLat = Math.toRadians(rs.getDouble(1));
        destLong = Math.toRadians(rs.getDouble(2));

        return radius*(Math.acos(Math.sin(sourceLat) * Math.sin(destLat)
            + Math.cos(sourceLat) * Math.cos(destLat) * Math.cos(sourceLong - destLong)));
    }


}

