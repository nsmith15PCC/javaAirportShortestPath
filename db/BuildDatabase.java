package db;

import java.sql.SQLException;
import java.io.IOException;

public class BuildDatabase {
    public static void main(String[] args) throws SQLException, IOException {
        BuildDatabaseUtility bdu = new BuildDatabaseUtility();
        try {
            bdu.execute();
        }
        finally {
            bdu.close();
        }
    }
}
