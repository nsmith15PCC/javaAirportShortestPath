import java.io.IOException;
import java.sql.SQLException;

public class DjikstraDemo {
    public static void main(String[] args) throws NumberFormatException, IOException, SQLException, ClassNotFoundException {
        Djikstra d = null;
        try {
            int source = Integer.parseInt(args[0]), dest = Integer.parseInt(args[1]);
            System.out.println(String.valueOf(source) + "    " + String.valueOf(dest));
            d = new Djikstra(3484, 3797);
            String ret = d.execute();
            System.out.println(ret);
        }
        finally {
            if (d != null) d.close();
        }
    }
}
