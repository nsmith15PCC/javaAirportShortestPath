package db.CSVParser;

import java.util.List;
import java.io.IOException;

class CSVDemo {
    private static final String filename = "airlines.dat";

    public static void main(String[] args) throws IOException {
        CSVParser par;
        try {
            par = new CSVParser(filename);
        }
        catch(IOException e) {
            System.out.println("Error opening file.");
            throw(e);
        }
        for(int i = 0; i < 10; ++i) {
            List<String> l;
            l = par.getNext();
            for (String s : l) {
                System.out.print(s + "    ");
            }
            System.out.println();
        }
    }

}
