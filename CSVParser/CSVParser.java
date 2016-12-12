package CSVParser;

import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

public class CSVParser {
    private BufferedReader br;

    CSVParser(String filename) throws IOException {
        br = new BufferedReader(new FileReader(filename));
    }

    private List<String> parseLine(String line) {
        StringBuilder sb = new StringBuilder();
        boolean inQuotes = false;
        char[] ca = line.toCharArray();
        List<String> ret = new ArrayList<>();

        for(char c : ca) {
            if(c == '\"') {
                inQuotes = !inQuotes;
            }
            else if(c == ',' && !inQuotes) {
                ret.add(sb.toString());
                sb = new StringBuilder();
            }
            else {
                sb.append(c);
            }
        }
        return ret;
    }

    public String[] getNext() {
        String nextLine;
        try {
            nextLine = br.readLine();
        }
        catch (IOException e) {
            System.out.println("Error reading file");
            return null;
        }
        if(nextLine == null) {
            return null;
        }
        else {
            return (String[]) parseLine(nextLine).toArray();
        }
    }
}

