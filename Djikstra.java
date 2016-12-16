import java.sql.*;
import java.io.IOException;
import java.io.File;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

class Djikstra {
    private Connection c;
    private final Integer source, dest;

    public Djikstra(Integer s, Integer d) throws SQLException, ClassNotFoundException {
        try {
            Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        }
        catch(ClassNotFoundException e) {
            System.out.println("Error Loading Driver!");
            throw(e);
        }

        c = DriverManager.getConnection("jdbc:derby:db/db");
        source = s;
        dest = d;
    }

    public String execute() throws SQLException {
        Airport finish = findPath();
        String pathString = pathString(finish.getPath());
        return(pathString);
    }

    public void close() {
        try {
            c.close();
        }
        catch (SQLException e) {}
    }

    private String pathString(List<Route> path) throws SQLException {
        PreparedStatement getAirportData = null, getAirlineData = null;
        try {
            getAirportData = c.prepareStatement("SELECT " +
                "routes.Distance, " + 
                "source.IATA, source.Name, source.City, source.Country, " +
                "dest.IATA, dest.Name, dest.City, source.Country " +
                "FROM routes JOIN airports source " +
                "ON source.Airport_ID = routes.Source_ID " +
                "JOIN airports dest " + 
                "ON dest.Airport_ID = routes.Dest_ID " +
                "WHERE routes.Route_ID = ?"
            );
            getAirlineData = c.prepareStatement("SELECT " +
                "airlines.Name, airlines.IATA " +
                "FROM airlines JOIN routes_airlines " +
                "ON airlines.Airline_ID = routes_airlines.Airline_ID " +
                "WHERE routes_airlines.Route_ID = ?"
            );
            String ret = "";
            for (Route r : path) {
                int Route_ID = r.Route_ID;
                getAirportData.setInt(1, Route_ID);
                ResultSet airportData = getAirportData.executeQuery();
                airportData.next();
                ret = ret + "Fly " + airportData.getString(1) + " miles\n" +
                    "    From (" +  airportData.getString(2) + ") " + airportData.getString(3) + ", " +
                    airportData.getString(4) + ", " + airportData.getString(5) + "\n" +
                    "    To (" +  airportData.getString(6) + ") " + airportData.getString(7) + ", " +
                    airportData.getString(8) + ", " + airportData.getString(9) + "\n";
                getAirlineData.setInt(1, Route_ID);
                ResultSet airlineData = getAirlineData.executeQuery();
                ret += "    On ";
                while(airlineData.next()) {
                    ret = ret + airlineData.getString(1) + " (" + airlineData.getString(2) + "), ";
                }
                ret += "\n";
            }
            return ret;
        }
        finally {
            try {
                if(getAirportData != null) getAirportData.close();
            }
            catch(SQLException e) {}
            try {
                if(getAirlineData != null) getAirlineData.close();
            }
            catch(SQLException e) {}
        }
    }

    private Airport findPath() throws SQLException {
        PreparedStatement ps = null;
        try {
            Map<Integer, Airport> allAirports = allAirports();
            int nearest = findNearest(allAirports);
            while (nearest != dest) {
                Airport node = allAirports.remove(nearest);
                List<Route> adjacent = allOutgoingRoutes(nearest);
                for (Route r : adjacent) {
                    Airport next = allAirports.get(r.Dest_ID);
                    if (next!=null) {
                        next.update(node.getPath(), node.getDistance(), r);
                    }
                }
                nearest = findNearest(allAirports);
            }
            return allAirports.get(nearest);        
        }
        finally {
            try {
                if (ps!=null) ps.close();
            }
            catch(SQLException e) {}
        }
    }

    private int findNearest(Map<Integer, Airport> allAirports) {
        Integer nearest = null;
        double distance = Double.MAX_VALUE, curDist;

        for (Map.Entry<Integer, Airport> entry : allAirports.entrySet()) {
            if((curDist = entry.getValue().getDistance()) < distance) {
                nearest = entry.getKey();
                distance = curDist;
            }
        }

        return nearest;
    }

    private Map<Integer, Airport> allAirports() throws SQLException {
        Statement s = null;
        try {
            Map<Integer, Airport> ret = new HashMap<>();
            s = c.createStatement();
            ResultSet rs = s.executeQuery("SELECT Airport_ID FROM airports");
            while (rs.next()) {
                int Airport_ID = rs.getInt(1);
                ret.put(Airport_ID, new Airport(Airport_ID));
            }
            ret.get(source).setDistance(0);
            ret.get(source).setPath(new ArrayList<Route>());
            return ret;
        }
        finally {
            try {
                if(s!=null) s.close();
            }
            catch(SQLException e){}
        }
    }

    private List<Route> allOutgoingRoutes(Integer Airport_ID) throws SQLException {
        Statement s = null;
        try {
            s = c.createStatement();
            List<Route> ret = new ArrayList<>();
            ResultSet rs = s.executeQuery("SELECT Route_ID, Distance, Dest_ID "+
               "FROM routes " +
               "WHERE Source_ID = " + Airport_ID.toString()
            );
            while(rs.next()) {
                int Route_ID, Dest_ID;
                double distance;
                try {
                    Route_ID = rs.getInt(1);
                    distance = rs.getDouble(2);
                    Dest_ID = rs.getInt(3);
                }
                catch(NumberFormatException e) {
                    continue;
                }
                ret.add(new Route(Route_ID, distance, Dest_ID));
            }
            return ret;
        }
        finally {
            try {
                if (s!=null) s.close();
            }
            catch(SQLException e) {}
        }
    }

    private class Airport {
        private int Airport_ID;
        private double distance;
        private List<Route> path;

        public Airport(int aid) {
            Airport_ID = aid;
            distance = Double.MAX_VALUE;
        }

        public boolean update(List<Route> newPath, double newDistance, Route newLeg) {
            if (distance < (newDistance + newLeg.distance)) return false;
            path = new ArrayList<Route>();
            distance = 0;
            for (Route r : newPath) {
                path.add(new Route(r));
                distance += r.distance;
            }
            path.add(newLeg);
            distance += newLeg.distance;
            return true;
        }

        public double getDistance() {
            return distance;
        }

        public void setDistance(int newDistance) {
            distance = newDistance;
        }

        public List<Route> getPath() {
            return path;
        }

        public void setPath(List<Route> p) {
            path = p;
        }
    }

    private class Route {
        public int Route_ID;
        public double distance;
        public int Dest_ID;

        public Route(int rid, double dist, int did) {
            Route_ID = rid;
            distance = dist;
            Dest_ID = did;
        }
        public Route(Route other) {
            Route_ID = other.Route_ID;
            distance = other.distance;
            Dest_ID = other.Dest_ID;
        }
    }
    
}